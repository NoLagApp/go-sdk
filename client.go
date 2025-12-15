package nolag

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vmihailenco/msgpack/v5"
)

// Common errors
var (
	ErrNotConnected = errors.New("not connected")
	ErrAuthFailed   = errors.New("authentication failed")
	ErrTimeout      = errors.New("operation timed out")
)

// Client is the NoLag real-time messaging client.
type Client struct {
	token   string
	options Options

	conn           *websocket.Conn
	status         ConnectionStatus
	authenticated  bool
	subscriptions  map[string]MessageHandler
	eventHandlers  map[string][]EventHandler
	pendingAcks    map[string]chan *protocolMessage
	messageID      int
	reconnectCount int

	mu            sync.RWMutex
	writeMu       sync.Mutex
	done          chan struct{}
	heartbeatDone chan struct{}
}

// New creates a new NoLag client with the given actor token.
func New(token string, opts ...Options) *Client {
	options := DefaultOptions()
	if len(opts) > 0 {
		options = mergeOptions(options, opts[0])
	}

	return &Client{
		token:         token,
		options:       options,
		status:        StatusDisconnected,
		subscriptions: make(map[string]MessageHandler),
		eventHandlers: make(map[string][]EventHandler),
		pendingAcks:   make(map[string]chan *protocolMessage),
	}
}

func mergeOptions(defaults, custom Options) Options {
	if custom.URL != "" {
		defaults.URL = custom.URL
	}
	if custom.ReconnectInterval > 0 {
		defaults.ReconnectInterval = custom.ReconnectInterval
	}
	if custom.MaxReconnectAttempts > 0 {
		defaults.MaxReconnectAttempts = custom.MaxReconnectAttempts
	}
	if custom.HeartbeatInterval >= 0 {
		defaults.HeartbeatInterval = custom.HeartbeatInterval
	}
	defaults.Reconnect = custom.Reconnect
	defaults.QoS = custom.QoS
	defaults.LoadBalance = custom.LoadBalance
	defaults.LoadBalanceGroup = custom.LoadBalanceGroup
	defaults.ActorTokenID = custom.ActorTokenID
	defaults.Debug = custom.Debug
	return defaults
}

// Connect establishes a connection to the NoLag broker.
func (c *Client) Connect() error {
	c.mu.Lock()
	if c.status == StatusConnected || c.status == StatusConnecting {
		c.mu.Unlock()
		return nil
	}
	c.status = StatusConnecting
	c.done = make(chan struct{})
	c.mu.Unlock()

	c.emit("connecting")

	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	conn, _, err := dialer.Dial(c.options.URL, http.Header{})
	if err != nil {
		c.mu.Lock()
		c.status = StatusDisconnected
		c.mu.Unlock()
		c.emit("error", err)
		return fmt.Errorf("dial failed: %w", err)
	}

	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()

	// Start message reader
	go c.readLoop()

	// Authenticate
	if err := c.authenticate(); err != nil {
		c.Close()
		return err
	}

	c.mu.Lock()
	c.status = StatusConnected
	c.authenticated = true
	c.reconnectCount = 0
	c.mu.Unlock()

	// Start heartbeat
	c.startHeartbeat()

	c.emit("connected")
	c.log("Connected to NoLag")

	return nil
}

func (c *Client) authenticate() error {
	msg := &protocolMessage{
		Type:  msgTypeAuth,
		Token: c.token,
	}

	// Include reconnect flag if this is a reconnection attempt
	// This tells the server to restore subscriptions
	if c.reconnectCount > 0 {
		msg.Reconnect = true
	}

	resp, err := c.sendAndWait(msg, msgTypeAuthAck, 10*time.Second)
	if err != nil {
		return fmt.Errorf("auth failed: %w", err)
	}

	if resp.Error != "" {
		return fmt.Errorf("%w: %s", ErrAuthFailed, resp.Error)
	}

	return nil
}

// Close disconnects from the broker.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.status == StatusDisconnected {
		return nil
	}

	c.status = StatusDisconnected
	c.authenticated = false

	// Stop heartbeat
	if c.heartbeatDone != nil {
		close(c.heartbeatDone)
		c.heartbeatDone = nil
	}

	// Close done channel
	if c.done != nil {
		close(c.done)
		c.done = nil
	}

	// Close WebSocket
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}

	go c.emit("disconnected")
	c.log("Disconnected")

	return nil
}

// Status returns the current connection status.
func (c *Client) Status() ConnectionStatus {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.status
}

// Subscribe registers a handler for messages on the given topic.
func (c *Client) Subscribe(topic string, handler MessageHandler, opts ...SubscribeOptions) error {
	c.mu.RLock()
	if c.status != StatusConnected {
		c.mu.RUnlock()
		return ErrNotConnected
	}
	c.mu.RUnlock()

	msg := &protocolMessage{
		Type:  msgTypeSubscribe,
		Topic: topic,
		QoS:   c.options.QoS,
	}

	// Use connection-level defaults, allow per-topic override
	loadBalance := c.options.LoadBalance
	loadBalanceGroup := c.options.LoadBalanceGroup

	if len(opts) > 0 {
		opt := opts[0]
		if opt.QoS != nil {
			msg.QoS = *opt.QoS
		}
		if opt.LoadBalance != nil {
			loadBalance = *opt.LoadBalance
		}
		if opt.LoadBalanceGroup != "" {
			loadBalanceGroup = opt.LoadBalanceGroup
		}
	}

	// Only include loadBalance fields when actually using load balancing
	if loadBalance {
		msg.LoadBalance = true
		if loadBalanceGroup != "" {
			msg.LoadBalanceGroup = loadBalanceGroup
		}
	}

	resp, err := c.sendAndWait(msg, msgTypeSubAck, 10*time.Second)
	if err != nil {
		return fmt.Errorf("subscribe failed: %w", err)
	}

	if resp.Error != "" {
		return fmt.Errorf("subscribe failed: %s", resp.Error)
	}

	c.mu.Lock()
	c.subscriptions[topic] = handler
	c.mu.Unlock()

	c.log("Subscribed to:", topic)
	return nil
}

// Unsubscribe removes a subscription from a topic.
func (c *Client) Unsubscribe(topic string) error {
	c.mu.RLock()
	if c.status != StatusConnected {
		c.mu.RUnlock()
		return ErrNotConnected
	}
	c.mu.RUnlock()

	msg := &protocolMessage{
		Type:  msgTypeUnsub,
		Topic: topic,
	}

	resp, err := c.sendAndWait(msg, msgTypeUnsubAck, 10*time.Second)
	if err != nil {
		return fmt.Errorf("unsubscribe failed: %w", err)
	}

	if resp.Error != "" {
		return fmt.Errorf("unsubscribe failed: %s", resp.Error)
	}

	c.mu.Lock()
	delete(c.subscriptions, topic)
	c.mu.Unlock()

	c.log("Unsubscribed from:", topic)
	return nil
}

// Emit publishes a message to a topic.
func (c *Client) Emit(topic string, data any, opts ...EmitOptions) error {
	c.mu.RLock()
	if c.status != StatusConnected {
		c.mu.RUnlock()
		return ErrNotConnected
	}
	c.mu.RUnlock()

	msg := &protocolMessage{
		Type:  msgTypePublish,
		Topic: topic,
		Data:  data,
		QoS:   c.options.QoS,
	}

	if len(opts) > 0 {
		opt := opts[0]
		if opt.QoS != nil {
			msg.QoS = *opt.QoS
		}
		msg.Retain = opt.Retain
		msg.Echo = opt.Echo
	}

	// For QoS 0, fire and forget
	if msg.QoS == QoSAtMostOnce {
		return c.send(msg)
	}

	// For QoS 1+, wait for ack
	resp, err := c.sendAndWait(msg, msgTypePubAck, 10*time.Second)
	if err != nil {
		return fmt.Errorf("emit failed: %w", err)
	}

	if resp.Error != "" {
		return fmt.Errorf("emit failed: %s", resp.Error)
	}

	return nil
}

// SetPresence sets the presence data for this client.
func (c *Client) SetPresence(data map[string]any) error {
	c.mu.RLock()
	if c.status != StatusConnected {
		c.mu.RUnlock()
		return ErrNotConnected
	}
	c.mu.RUnlock()

	msg := &protocolMessage{
		Type: msgTypePresSet,
		Data: data,
	}

	return c.send(msg)
}

// GetPresence retrieves presence information for all actors in a topic.
func (c *Client) GetPresence(topic string) ([]ActorPresence, error) {
	c.mu.RLock()
	if c.status != StatusConnected {
		c.mu.RUnlock()
		return nil, ErrNotConnected
	}
	c.mu.RUnlock()

	msg := &protocolMessage{
		Type:  msgTypePresGet,
		Topic: topic,
	}

	resp, err := c.sendAndWait(msg, msgTypePresence, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("get presence failed: %w", err)
	}

	// Parse presence data from response
	var result []ActorPresence
	if data, ok := resp.Data.([]any); ok {
		for _, item := range data {
			if m, ok := item.(map[string]any); ok {
				presence := ActorPresence{
					Presence: make(map[string]any),
				}
				if id, ok := m["actor_token_id"].(string); ok {
					presence.ActorTokenID = id
				}
				if at, ok := m["actor_type"].(string); ok {
					presence.ActorType = ActorType(at)
				}
				if p, ok := m["presence"].(map[string]any); ok {
					presence.Presence = p
				}
				if ts, ok := m["joined_at"].(float64); ok {
					presence.JoinedAt = time.Unix(int64(ts), 0)
				}
				result = append(result, presence)
			}
		}
	}

	return result, nil
}

// On registers an event handler.
// Events: "connected", "disconnected", "reconnecting", "error", "presence"
func (c *Client) On(event string, handler EventHandler) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.eventHandlers[event] = append(c.eventHandlers[event], handler)
}

// Off removes all handlers for an event.
func (c *Client) Off(event string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.eventHandlers, event)
}

func (c *Client) emit(event string, args ...any) {
	c.mu.RLock()
	handlers := c.eventHandlers[event]
	c.mu.RUnlock()

	for _, h := range handlers {
		h(args...)
	}
}

func (c *Client) send(msg *protocolMessage) error {
	data, err := msgpack.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal failed: %w", err)
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()

	if conn == nil {
		return ErrNotConnected
	}

	return conn.WriteMessage(websocket.BinaryMessage, data)
}

func (c *Client) sendAndWait(msg *protocolMessage, ackType messageType, timeout time.Duration) (*protocolMessage, error) {
	c.mu.Lock()
	c.messageID++
	msg.ID = fmt.Sprintf("%d", c.messageID)
	ackChan := make(chan *protocolMessage, 1)
	c.pendingAcks[msg.ID] = ackChan
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.pendingAcks, msg.ID)
		c.mu.Unlock()
	}()

	if err := c.send(msg); err != nil {
		return nil, err
	}

	select {
	case resp := <-ackChan:
		return resp, nil
	case <-time.After(timeout):
		return nil, ErrTimeout
	case <-c.done:
		return nil, ErrNotConnected
	}
}

func (c *Client) readLoop() {
	defer func() {
		c.handleDisconnect()
	}()

	for {
		select {
		case <-c.done:
			return
		default:
		}

		c.mu.RLock()
		conn := c.conn
		c.mu.RUnlock()

		if conn == nil {
			return
		}

		messageType, data, err := conn.ReadMessage()
		if err != nil {
			c.log("Read error:", err)
			return
		}

		// Empty message is heartbeat pong
		if len(data) == 0 {
			c.log("Heartbeat pong received")
			continue
		}

		if messageType != websocket.BinaryMessage {
			continue
		}

		var msg protocolMessage
		if err := msgpack.Unmarshal(data, &msg); err != nil {
			c.log("Unmarshal error:", err)
			continue
		}

		c.handleMessage(&msg)
	}
}

func (c *Client) handleMessage(msg *protocolMessage) {
	// Check for pending ack
	if msg.ID != "" {
		c.mu.RLock()
		ackChan, ok := c.pendingAcks[msg.ID]
		c.mu.RUnlock()
		if ok {
			select {
			case ackChan <- msg:
			default:
			}
			return
		}
	}

	switch msg.Type {
	case msgTypeMessage:
		c.mu.RLock()
		handler, ok := c.subscriptions[msg.Topic]
		c.mu.RUnlock()

		if ok && handler != nil {
			meta := MessageMeta{
				IsReplay: msg.IsReplay,
				MsgID:    msg.MsgID,
			}
			if msg.Meta != nil {
				meta.Sender = msg.Meta.Sender
				if msg.Meta.Timestamp > 0 {
					meta.Timestamp = time.Unix(int64(msg.Meta.Timestamp), 0)
				}
			}
			handler(msg.Data, meta)
		}

	case msgTypePresence:
		c.emit("presence", msg.Topic, msg.Data)

	case msgTypeError:
		c.emit("error", errors.New(msg.Error))
	}
}

func (c *Client) handleDisconnect() {
	c.mu.Lock()
	wasConnected := c.status == StatusConnected
	c.status = StatusDisconnected
	c.authenticated = false
	c.mu.Unlock()

	if !wasConnected {
		return
	}

	c.emit("disconnected")

	if c.options.Reconnect {
		go c.reconnect()
	}
}

func (c *Client) reconnect() {
	c.mu.Lock()
	c.status = StatusReconnecting
	c.reconnectCount++
	attempt := c.reconnectCount
	c.mu.Unlock()

	if c.options.MaxReconnectAttempts > 0 && attempt > c.options.MaxReconnectAttempts {
		c.log("Max reconnect attempts reached")
		c.mu.Lock()
		c.status = StatusDisconnected
		c.mu.Unlock()
		return
	}

	c.emit("reconnecting", attempt)
	c.log("Reconnecting... attempt", attempt)

	time.Sleep(c.options.ReconnectInterval)

	c.mu.Lock()
	c.status = StatusDisconnected
	c.mu.Unlock()

	if err := c.Connect(); err != nil {
		c.log("Reconnect failed:", err)
		go c.reconnect()
		return
	}

	// Resubscribe to all topics
	c.mu.RLock()
	topics := make([]string, 0, len(c.subscriptions))
	handlers := make(map[string]MessageHandler)
	for topic, handler := range c.subscriptions {
		topics = append(topics, topic)
		handlers[topic] = handler
	}
	c.mu.RUnlock()

	for _, topic := range topics {
		if err := c.Subscribe(topic, handlers[topic]); err != nil {
			c.log("Resubscribe failed for", topic, ":", err)
		}
	}
}

func (c *Client) startHeartbeat() {
	if c.options.HeartbeatInterval <= 0 {
		return
	}

	c.mu.Lock()
	c.heartbeatDone = make(chan struct{})
	done := c.heartbeatDone
	c.mu.Unlock()

	go func() {
		ticker := time.NewTicker(c.options.HeartbeatInterval)
		defer ticker.Stop()

		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				c.mu.RLock()
				conn := c.conn
				status := c.status
				c.mu.RUnlock()

				if conn == nil || status != StatusConnected {
					return
				}

				c.writeMu.Lock()
				err := conn.WriteMessage(websocket.BinaryMessage, []byte{})
				c.writeMu.Unlock()

				if err != nil {
					c.log("Heartbeat failed:", err)
					return
				}
				c.log("Heartbeat ping sent")
			}
		}
	}()
}

func (c *Client) log(args ...any) {
	if c.options.Debug {
		log.Println(append([]any{"[NoLag]"}, args...)...)
	}
}

// SetApp creates an App context for scoped pub/sub.
//
// Example:
//
//	room := client.SetApp("chat").SetRoom("general")
//	room.Subscribe("messages", handler)
//	room.Emit("messages", data)
func (c *Client) SetApp(app string) *App {
	return &App{client: c, app: app}
}

// App is an intermediate context for setting the room.
type App struct {
	client *Client
	app    string
}

// SetRoom creates a Room context for scoped pub/sub within app/room.
func (a *App) SetRoom(room string) *Room {
	return &Room{client: a.client, app: a.app, room: room}
}

// Room provides a scoped context for pub/sub within an app/room.
// Topics are automatically prefixed with "app/room/".
type Room struct {
	client *Client
	app    string
	room   string
}

// Prefix returns the full topic prefix (app/room).
func (r *Room) Prefix() string {
	return fmt.Sprintf("%s/%s", r.app, r.room)
}

func (r *Room) fullTopic(topic string) string {
	return fmt.Sprintf("%s/%s/%s", r.app, r.room, topic)
}

// Subscribe registers a handler for messages on the given topic (auto-prefixed with app/room).
func (r *Room) Subscribe(topic string, handler MessageHandler, opts ...SubscribeOptions) error {
	return r.client.Subscribe(r.fullTopic(topic), handler, opts...)
}

// Unsubscribe removes a subscription from a topic (auto-prefixed with app/room).
func (r *Room) Unsubscribe(topic string) error {
	return r.client.Unsubscribe(r.fullTopic(topic))
}

// Emit publishes a message to a topic (auto-prefixed with app/room).
func (r *Room) Emit(topic string, data any, opts ...EmitOptions) error {
	return r.client.Emit(r.fullTopic(topic), data, opts...)
}

// On registers an event handler for the given topic (auto-prefixed with app/room).
func (r *Room) On(topic string, handler EventHandler) {
	r.client.On(r.fullTopic(topic), handler)
}

// Off removes all handlers for an event (auto-prefixed with app/room).
func (r *Room) Off(topic string) {
	r.client.Off(r.fullTopic(topic))
}
