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
	actorID        string
	projectID      string
	actorType      ActorType
	subscriptions  map[string]MessageHandler
	topicFilters   map[string][]any
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
		topicFilters:  make(map[string][]any),
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
	// Only override heartbeat if explicitly set (> 0 means custom value, we keep default otherwise)
	// A value of -1 can be used to explicitly disable heartbeats
	if custom.HeartbeatInterval > 0 {
		defaults.HeartbeatInterval = custom.HeartbeatInterval
	} else if custom.HeartbeatInterval < 0 {
		// Negative value explicitly disables heartbeat
		defaults.HeartbeatInterval = 0
	}
	// Note: custom.HeartbeatInterval == 0 means "not set", keep default
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

	// Auth uses a special handler (no message ID matching)
	// The server responds with type: "auth" and success: true/false
	authChan := make(chan *protocolMessage, 1)
	c.mu.Lock()
	c.pendingAcks["__auth__"] = authChan
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.pendingAcks, "__auth__")
		c.mu.Unlock()
	}()

	if err := c.send(msg); err != nil {
		return fmt.Errorf("auth failed: %w", err)
	}

	select {
	case resp := <-authChan:
		if resp.Error != "" {
			return fmt.Errorf("%w: %s", ErrAuthFailed, resp.Error)
		}
		if !resp.Success {
			return fmt.Errorf("%w: server returned success=false", ErrAuthFailed)
		}
		// Extract actor info from response
		c.actorID = resp.ActorTokenId
		c.projectID = resp.ProjectID
		if resp.ActorTypeStr != "" {
			c.actorType = ActorType(resp.ActorTypeStr)
		}
		return nil
	case <-time.After(10 * time.Second):
		return fmt.Errorf("auth failed: %w", ErrTimeout)
	case <-c.done:
		return fmt.Errorf("auth failed: %w", ErrNotConnected)
	}
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

// ActorID returns the actor ID assigned by the server after authentication.
func (c *Client) ActorID() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.actorID
}

// ProjectID returns the project ID from the auth response.
func (c *Client) ProjectID() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.projectID
}

// ActorTypeValue returns the actor type from the auth response.
func (c *Client) ActorTypeValue() ActorType {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.actorType
}

// Subscribe registers a handler for messages on the given topic.
// Note: Subscribe is fire-and-forget like the JS SDK - it doesn't wait for server acknowledgment.
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
	}

	// Use connection-level defaults, allow per-topic override
	loadBalance := c.options.LoadBalance
	loadBalanceGroup := c.options.LoadBalanceGroup

	if len(opts) > 0 {
		opt := opts[0]
		if opt.LoadBalance != nil {
			loadBalance = *opt.LoadBalance
		}
		if opt.LoadBalanceGroup != "" {
			loadBalanceGroup = opt.LoadBalanceGroup
		}
		if len(opt.Filters) > 0 {
			msg.Filters = opt.Filters
			c.mu.Lock()
			c.topicFilters[topic] = opt.Filters
			c.mu.Unlock()
		}
	}

	// Only include loadBalance fields when actually using load balancing
	if loadBalance {
		msg.LoadBalance = true
		if loadBalanceGroup != "" {
			msg.LoadBalanceGroup = loadBalanceGroup
		}
	}

	// Fire-and-forget like JS SDK - don't wait for ack
	if err := c.send(msg); err != nil {
		return fmt.Errorf("subscribe failed: %w", err)
	}

	c.mu.Lock()
	c.subscriptions[topic] = handler
	c.mu.Unlock()

	c.log("Subscribed to:", topic)
	return nil
}

// Unsubscribe removes a subscription from a topic.
// Note: Unsubscribe is fire-and-forget like the JS SDK.
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

	// Fire-and-forget like JS SDK
	if err := c.send(msg); err != nil {
		return fmt.Errorf("unsubscribe failed: %w", err)
	}

	c.mu.Lock()
	delete(c.subscriptions, topic)
	delete(c.topicFilters, topic)
	c.mu.Unlock()

	c.log("Unsubscribed from:", topic)
	return nil
}

// SetFilters replaces all filters for a topic.
// Empty slice switches back to wildcard (receive all messages).
// Accepts mixed items: string for OR, []string for AND groups.
func (c *Client) SetFilters(topic string, filters []any) error {
	c.mu.RLock()
	if c.status != StatusConnected {
		c.mu.RUnlock()
		return ErrNotConnected
	}
	c.mu.RUnlock()

	msg := &protocolMessage{
		Type:    "setFilters",
		Topic:   topic,
		Filters: filters,
	}

	if err := c.send(msg); err != nil {
		return fmt.Errorf("setFilters failed: %w", err)
	}

	c.mu.Lock()
	if len(filters) > 0 {
		c.topicFilters[topic] = filters
	} else {
		delete(c.topicFilters, topic)
	}
	c.mu.Unlock()

	return nil
}

// AddFilters adds simple string filters to the existing set for a topic.
// AND groups in the existing set are preserved.
func (c *Client) AddFilters(topic string, filters []string) error {
	c.mu.RLock()
	existing := c.topicFilters[topic]
	c.mu.RUnlock()

	simpleSet := make(map[string]bool)
	var andGroups []any
	for _, item := range existing {
		switch v := item.(type) {
		case string:
			simpleSet[v] = true
		default:
			andGroups = append(andGroups, v)
		}
	}
	for _, f := range filters {
		simpleSet[f] = true
	}

	result := make([]any, 0, len(simpleSet)+len(andGroups))
	for f := range simpleSet {
		result = append(result, f)
	}
	result = append(result, andGroups...)

	return c.SetFilters(topic, result)
}

// RemoveFilters removes specific simple string filters from a topic.
// AND groups in the existing set are preserved.
func (c *Client) RemoveFilters(topic string, filters []string) error {
	c.mu.RLock()
	existing := c.topicFilters[topic]
	c.mu.RUnlock()

	remove := make(map[string]bool)
	for _, f := range filters {
		remove[f] = true
	}

	result := make([]any, 0)
	for _, item := range existing {
		switch v := item.(type) {
		case string:
			if !remove[v] {
				result = append(result, v)
			}
		default:
			// Preserve AND groups
			result = append(result, item)
		}
	}

	return c.SetFilters(topic, result)
}

// Emit publishes a message to a topic.
// Note: Emit is fire-and-forget like the JS SDK.
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
	}

	if len(opts) > 0 {
		opt := opts[0]
		msg.Retain = opt.Retain
		msg.Echo = opt.Echo
		if opt.Filter != "" {
			msg.Filter = opt.Filter
		} else if len(opt.Filters) > 0 {
			msg.Filters = opt.Filters
		}
	}

	// Fire-and-forget like JS SDK
	return c.send(msg)
}

// SetPresence sets the presence data for this client, optionally scoped to a room.
func (c *Client) SetPresence(data map[string]any, roomID ...string) error {
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
	if len(roomID) > 0 && roomID[0] != "" {
		msg.RoomID = roomID[0]
	}

	return c.send(msg)
}

// GetPresence retrieves presence information for all actors, optionally for a specific room.
func (c *Client) GetPresence(roomID ...string) ([]ActorPresence, error) {
	c.mu.RLock()
	if c.status != StatusConnected {
		c.mu.RUnlock()
		return nil, ErrNotConnected
	}
	c.mu.RUnlock()

	msg := &protocolMessage{
		Type: msgTypePresGet,
	}
	if len(roomID) > 0 && roomID[0] != "" {
		msg.RoomID = roomID[0]
	}

	ackChan := make(chan *protocolMessage, 1)
	c.mu.Lock()
	c.pendingAcks["__presenceList__"] = ackChan
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.pendingAcks, "__presenceList__")
		c.mu.Unlock()
	}()

	if err := c.send(msg); err != nil {
		return nil, fmt.Errorf("get presence failed: %w", err)
	}

	select {
	case resp := <-ackChan:
		var result []ActorPresence
		if data, ok := resp.Data.([]any); ok {
			for _, item := range data {
				if m, ok := item.(map[string]any); ok {
					presence := ActorPresence{
						Presence: make(map[string]any),
					}
					if id, ok := m["actorTokenId"].(string); ok {
						presence.ActorTokenID = id
					}
					if p, ok := m["presence"].(map[string]any); ok {
						presence.Presence = p
					}
					result = append(result, presence)
				}
			}
		}
		return result, nil
	case <-time.After(10 * time.Second):
		return nil, fmt.Errorf("get presence: %w", ErrTimeout)
	case <-c.done:
		return nil, ErrNotConnected
	}
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
	c.log("readLoop started")
	defer func() {
		c.log("readLoop exiting")
		c.handleDisconnect()
	}()

	for {
		select {
		case <-c.done:
			c.log("readLoop: done channel closed")
			return
		default:
		}

		c.mu.RLock()
		conn := c.conn
		c.mu.RUnlock()

		if conn == nil {
			c.log("readLoop: conn is nil")
			return
		}

		c.log("readLoop: waiting for message...")
		messageType, data, err := conn.ReadMessage()
		c.log("readLoop: got message, type:", messageType, "len:", len(data), "err:", err)
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
			c.log("readLoop: skipping non-binary message type:", messageType)
			continue
		}

		c.log("Received binary message, length:", len(data))

		var msg protocolMessage
		if err := msgpack.Unmarshal(data, &msg); err != nil {
			c.log("Unmarshal error:", err, "data (first 100 bytes):", string(data[:min(len(data), 100)]))
			continue
		}

		c.handleMessage(&msg)
	}
}

func (c *Client) handleMessage(msg *protocolMessage) {
	c.log("Received message type:", msg.Type, "ID:", msg.ID, "Topic:", msg.Topic)

	// Handle auth response specially (no message ID)
	if msg.Type == msgTypeAuth {
		c.log("Auth response received, success:", msg.Success, "error:", msg.Error)
		c.mu.RLock()
		authChan, ok := c.pendingAcks["__auth__"]
		c.mu.RUnlock()
		c.log("Auth channel exists:", ok)
		if ok {
			// Check success field - if false, set error
			if !msg.Success && msg.Error == "" {
				msg.Error = "authentication failed"
			}
			select {
			case authChan <- msg:
				c.log("Auth response sent to channel")
			default:
				c.log("Auth channel full, dropping response")
			}
			return
		}
	}

	// Check for pending ack by message ID
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
				Filter:   msg.Filter,
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
		// Broker sends: {type: "presence", event: "join"/"leave"/"update", data: {...}}
		if msg.Event != "" {
			eventData, _ := msg.Data.(map[string]any)
			if eventData == nil {
				eventData = make(map[string]any)
			}
			actorTokenID, _ := eventData["actor_token_id"].(string)
			presenceData, _ := eventData["presence"].(map[string]any)
			actor := ActorPresence{
				ActorTokenID: actorTokenID,
				Presence:     presenceData,
			}
			c.emit(fmt.Sprintf("presence:%s", msg.Event), actor)
		} else {
			c.emit("presence", msg.Topic, msg.Data)
		}

	case "presenceList":
		// Handle presenceList response
		ackKey := "__presenceList__"
		c.mu.RLock()
		ackChan, ok := c.pendingAcks[ackKey]
		c.mu.RUnlock()
		if ok {
			select {
			case ackChan <- msg:
			default:
			}
		}

	case msgTypeLobbySubscribed:
		ackKey := fmt.Sprintf("lobbySubscribed:%s", msg.LobbyID)
		c.mu.RLock()
		ackChan, ok := c.pendingAcks[ackKey]
		c.mu.RUnlock()
		if ok {
			select {
			case ackChan <- msg:
			default:
			}
		}

	case msgTypeLobbyPresenceList:
		ackKey := fmt.Sprintf("lobbyPresenceList:%s", msg.LobbyID)
		c.mu.RLock()
		ackChan, ok := c.pendingAcks[ackKey]
		c.mu.RUnlock()
		if ok {
			select {
			case ackChan <- msg:
			default:
			}
		}

	case msgTypeLobbyPresence:
		presenceData := make(map[string]any)
		if m, ok := msg.Data.(map[string]any); ok {
			presenceData = m
		}
		event := LobbyPresenceEvent{
			LobbyID: msg.LobbyID,
			RoomID:  msg.RoomID,
			ActorID: msg.ActorID,
			Data:    presenceData,
		}
		c.emit(fmt.Sprintf("lobby:%s:presence:%s", msg.LobbyID, msg.Event), event)
		c.emit(fmt.Sprintf("lobbyPresence:%s", msg.Event), event)

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

// SetLobby creates a Lobby context for observing presence across rooms.
func (a *App) SetLobby(lobbyID string) *Lobby {
	return &Lobby{client: a.client, lobbyID: lobbyID}
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

// SetFilters replaces all filters for a topic (auto-prefixed with app/room).
func (r *Room) SetFilters(topic string, filters []any) error {
	return r.client.SetFilters(r.fullTopic(topic), filters)
}

// AddFilters adds filters to existing set for a topic (auto-prefixed with app/room).
func (r *Room) AddFilters(topic string, filters []string) error {
	return r.client.AddFilters(r.fullTopic(topic), filters)
}

// RemoveFilters removes specific filters from a topic (auto-prefixed with app/room).
func (r *Room) RemoveFilters(topic string, filters []string) error {
	return r.client.RemoveFilters(r.fullTopic(topic), filters)
}

// SetPresence sets the presence data scoped to this room.
func (r *Room) SetPresence(data map[string]any) error {
	return r.client.SetPresence(data, r.room)
}

// On registers an event handler for the given topic (auto-prefixed with app/room).
func (r *Room) On(topic string, handler EventHandler) {
	r.client.On(r.fullTopic(topic), handler)
}

// Off removes all handlers for an event (auto-prefixed with app/room).
func (r *Room) Off(topic string) {
	r.client.Off(r.fullTopic(topic))
}

// Lobby provides a scoped context for observing presence across rooms.
// Lobbies are read-only - you can only observe presence, not publish to them.
type Lobby struct {
	client  *Client
	lobbyID string
}

// LobbyID returns the lobby identifier.
func (l *Lobby) LobbyID() string {
	return l.lobbyID
}

// Subscribe subscribes to this lobby's presence events.
// Returns a snapshot of current presence when subscription completes.
func (l *Lobby) Subscribe() (LobbyPresenceState, error) {
	l.client.mu.RLock()
	if l.client.status != StatusConnected {
		l.client.mu.RUnlock()
		return nil, ErrNotConnected
	}
	l.client.mu.RUnlock()

	ackKey := fmt.Sprintf("lobbySubscribed:%s", l.lobbyID)
	ackChan := make(chan *protocolMessage, 1)

	l.client.mu.Lock()
	l.client.pendingAcks[ackKey] = ackChan
	l.client.mu.Unlock()

	defer func() {
		l.client.mu.Lock()
		delete(l.client.pendingAcks, ackKey)
		l.client.mu.Unlock()
	}()

	msg := &protocolMessage{
		Type:    msgTypeLobbySubscribe,
		LobbyID: l.lobbyID,
	}
	if err := l.client.send(msg); err != nil {
		return nil, fmt.Errorf("lobby subscribe failed: %w", err)
	}

	select {
	case resp := <-ackChan:
		return parseLobbyPresence(resp.Presence), nil
	case <-time.After(10 * time.Second):
		return nil, fmt.Errorf("lobby subscribe: %w", ErrTimeout)
	case <-l.client.done:
		return nil, ErrNotConnected
	}
}

// Unsubscribe unsubscribes from this lobby's presence events (fire-and-forget).
func (l *Lobby) Unsubscribe() error {
	l.client.mu.RLock()
	if l.client.status != StatusConnected {
		l.client.mu.RUnlock()
		return ErrNotConnected
	}
	l.client.mu.RUnlock()

	return l.client.send(&protocolMessage{
		Type:    msgTypeLobbyUnsubscribe,
		LobbyID: l.lobbyID,
	})
}

// FetchPresence fetches the current presence state for the lobby.
func (l *Lobby) FetchPresence() (LobbyPresenceState, error) {
	l.client.mu.RLock()
	if l.client.status != StatusConnected {
		l.client.mu.RUnlock()
		return nil, ErrNotConnected
	}
	l.client.mu.RUnlock()

	ackKey := fmt.Sprintf("lobbyPresenceList:%s", l.lobbyID)
	ackChan := make(chan *protocolMessage, 1)

	l.client.mu.Lock()
	l.client.pendingAcks[ackKey] = ackChan
	l.client.mu.Unlock()

	defer func() {
		l.client.mu.Lock()
		delete(l.client.pendingAcks, ackKey)
		l.client.mu.Unlock()
	}()

	msg := &protocolMessage{
		Type:    msgTypeGetLobbyPresence,
		LobbyID: l.lobbyID,
	}
	if err := l.client.send(msg); err != nil {
		return nil, fmt.Errorf("fetch lobby presence failed: %w", err)
	}

	select {
	case resp := <-ackChan:
		return parseLobbyPresence(resp.Presence), nil
	case <-time.After(10 * time.Second):
		return nil, fmt.Errorf("fetch lobby presence: %w", ErrTimeout)
	case <-l.client.done:
		return nil, ErrNotConnected
	}
}

// On registers a handler for presence events in this lobby (e.g., "presence:join").
func (l *Lobby) On(event string, handler LobbyPresenceHandler) {
	eventType := event
	if len(event) > 9 && event[:9] == "presence:" {
		eventType = event[9:]
	}
	eventKey := fmt.Sprintf("lobby:%s:presence:%s", l.lobbyID, eventType)
	l.client.On(eventKey, func(args ...any) {
		if len(args) > 0 {
			if e, ok := args[0].(LobbyPresenceEvent); ok {
				handler(e)
			}
		}
	})
}

// Off removes all handlers for a presence event in this lobby.
func (l *Lobby) Off(event string) {
	eventType := event
	if len(event) > 9 && event[:9] == "presence:" {
		eventType = event[9:]
	}
	eventKey := fmt.Sprintf("lobby:%s:presence:%s", l.lobbyID, eventType)
	l.client.Off(eventKey)
}

func parseLobbyPresence(data any) LobbyPresenceState {
	result := make(LobbyPresenceState)
	if m, ok := data.(map[string]any); ok {
		for roomID, roomData := range m {
			if roomMap, ok := roomData.(map[string]any); ok {
				result[roomID] = make(map[string]map[string]any)
				for actorID, actorData := range roomMap {
					if actorMap, ok := actorData.(map[string]any); ok {
						result[roomID][actorID] = actorMap
					}
				}
			}
		}
	}
	return result
}
