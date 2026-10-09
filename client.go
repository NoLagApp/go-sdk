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

	conn            *websocket.Conn
	status          ConnectionStatus
	authenticated   bool
	actorID         string
	projectID       string
	actorType       ActorType
	eventHandlers   map[string][]EventHandler
	pendingAcks     map[string]chan *protocolMessage
	messageID       int
	reconnectCount  int
	protocolVersion int

	// The session (see session.go): what the caller asked for, put back after
	// a reconnect and cleared by Close.
	subscriptions   map[string]subscription
	roomPresence    *roomPresence
	projectPresence map[string]any
	lobbies         map[string]struct{}
	// session is incremented by Close. A reconnect loop or an in-flight call
	// that started under an earlier value belongs to an ended session.
	session uint64

	mu      sync.RWMutex
	writeMu sync.Mutex
	// sessionMu serialises each change to the session with the frame that
	// carries it, and with restoreSession, so the broker sees them in the
	// order the session records them.
	sessionMu sync.Mutex
	// done and heartbeatDone belong to the current connection and are
	// closed when it is dropped.
	done          chan struct{}
	heartbeatDone chan struct{}
}

// New creates a new NoLag client with the given actor token.
func New(token string, opts ...Options) *Client {
	options := DefaultOptions()
	if len(opts) > 0 {
		options = mergeOptions(options, opts[0])
	}

	c := &Client{
		token:         token,
		options:       options,
		status:        StatusDisconnected,
		eventHandlers: make(map[string][]EventHandler),
		pendingAcks:   make(map[string]chan *protocolMessage),
	}
	c.clearSessionLocked()
	return c
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
	} else if custom.MaxReconnectAttempts < 0 {
		// Negative value means unlimited attempts; 0 keeps the default.
		defaults.MaxReconnectAttempts = 0
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
	if custom.Reconnect != nil {
		defaults.Reconnect = custom.Reconnect
	}
	if custom.QoS != nil {
		defaults.QoS = custom.QoS
	}
	defaults.LoadBalance = custom.LoadBalance
	defaults.LoadBalanceGroup = custom.LoadBalanceGroup
	defaults.ActorTokenID = custom.ActorTokenID
	defaults.Debug = custom.Debug
	return defaults
}

// Connect establishes a connection to the NoLag broker.
//
// When the client already has a session (it reconnected automatically, or
// Connect is called again after the connection dropped without Close), the
// session is put back before the "connected" event: every subscription the
// broker did not restore by itself is sent again with its options and
// current filters, and presence and lobby subscriptions are sent again. A
// "connected" handler that subscribes again is harmless: a subscribe replaces
// the broker's subscription for that topic and the client keeps one handler
// per topic, so nothing is delivered twice.
func (c *Client) Connect() error {
	c.mu.RLock()
	session := c.session
	c.mu.RUnlock()
	return c.connect(session)
}

// connect dials and authenticates for the given session, and fails if Close
// ends that session first.
func (c *Client) connect(session uint64) error {
	c.mu.Lock()
	if c.session != session {
		c.mu.Unlock()
		return ErrNotConnected
	}
	if c.status == StatusConnected || c.status == StatusConnecting {
		c.mu.Unlock()
		return nil
	}
	c.status = StatusConnecting
	// Tells the server to restore subscriptions it still holds.
	isReconnect := c.reconnectCount > 0
	c.mu.Unlock()

	c.emit("connecting")

	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	conn, _, err := dialer.Dial(c.options.URL, http.Header{})
	if err != nil {
		c.mu.Lock()
		if c.session == session && c.status == StatusConnecting {
			c.status = StatusDisconnected
		}
		c.mu.Unlock()
		c.emit("error", err)
		return fmt.Errorf("dial failed: %w", err)
	}

	done := make(chan struct{})
	c.mu.Lock()
	if c.session != session || c.status != StatusConnecting {
		// Close ran while dialling.
		c.mu.Unlock()
		conn.Close()
		return ErrNotConnected
	}
	c.conn = conn
	c.done = done
	c.mu.Unlock()

	// Start message reader
	go c.readLoop(conn, done)

	// Authenticate, then put the session back before reporting connected
	restored, err := c.authenticate(isReconnect, done)
	if err == nil {
		err = c.restoreSession(restored)
	}
	if err == nil {
		c.mu.Lock()
		if c.conn == conn && c.session == session {
			c.status = StatusConnected
			c.authenticated = true
			c.reconnectCount = 0
		} else {
			err = ErrNotConnected
		}
		c.mu.Unlock()
	}
	if err != nil {
		c.mu.Lock()
		if c.dropConnLocked(conn) {
			c.status = StatusDisconnected
		}
		c.mu.Unlock()
		return err
	}

	// Start heartbeat
	c.startHeartbeat(conn)

	c.emit("connected")
	c.log("Connected to NoLag")

	return nil
}

// authenticate sends the auth frame and waits for the reply. It returns the
// reply's restoredSubscriptions.
func (c *Client) authenticate(isReconnect bool, done chan struct{}) ([]any, error) {
	msg := &protocolMessage{
		Type:            msgTypeAuth,
		Token:           c.token,
		ProtocolVersion: ProtocolVersion,
		Reconnect:       isReconnect,
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
		return nil, fmt.Errorf("auth failed: %w", err)
	}

	select {
	case resp := <-authChan:
		if resp.Error != "" {
			return nil, fmt.Errorf("%w: %s", ErrAuthFailed, resp.Error)
		}
		if !resp.Success {
			return nil, fmt.Errorf("%w: server returned success=false", ErrAuthFailed)
		}
		// Extract actor info from response. Guarded because readLoop is already
		// running and the accessors read these under RLock.
		c.mu.Lock()
		c.actorID = resp.ActorTokenId
		c.projectID = resp.ProjectID
		if resp.ActorTypeStr != "" {
			c.actorType = ActorType(resp.ActorTypeStr)
		}
		// The broker echoes the negotiated version, min(client, server). It is
		// absent on brokers older than v2, which means version 1.
		if resp.ProtocolVersion > 0 {
			c.protocolVersion = resp.ProtocolVersion
		} else {
			c.protocolVersion = 1
		}
		negotiated := c.protocolVersion
		c.mu.Unlock()

		c.log("Protocol version negotiated:", negotiated)
		if len(resp.RestoredSubscriptions) > 0 {
			c.log("Server restored subscriptions:", resp.RestoredSubscriptions)
		}
		return resp.RestoredSubscriptions, nil
	case <-time.After(10 * time.Second):
		return nil, fmt.Errorf("auth failed: %w", ErrTimeout)
	case <-done:
		return nil, fmt.Errorf("auth failed: %w", ErrNotConnected)
	}
}

// Close disconnects from the broker and ends the session: the client forgets
// its subscriptions, presence and lobby subscriptions, and a reconnect in
// progress stops. A later Connect starts a fresh session.
func (c *Client) Close() error {
	c.mu.Lock()
	c.session++
	c.clearSessionLocked()
	c.reconnectCount = 0
	wasActive := c.status != StatusDisconnected
	c.status = StatusDisconnected
	c.authenticated = false
	c.dropConnLocked(c.conn)
	c.mu.Unlock()

	if wasActive {
		go c.emit("disconnected")
		c.log("Disconnected")
	}

	return nil
}

// dropConnLocked closes conn and releases what belongs to it, if conn is
// still the current connection. c.mu must be held for writing. It returns
// false when conn was already dropped or replaced.
func (c *Client) dropConnLocked(conn *websocket.Conn) bool {
	if conn == nil || c.conn != conn {
		return false
	}
	c.conn = nil
	c.authenticated = false
	if c.heartbeatDone != nil {
		close(c.heartbeatDone)
		c.heartbeatDone = nil
	}
	if c.done != nil {
		close(c.done)
		c.done = nil
	}
	conn.Close()
	return true
}

// connectedSession returns the current session and whether the client is
// connected.
func (c *Client) connectedSession() (uint64, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.session, c.status == StatusConnected
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

// ProtocolVersion returns the wire protocol version negotiated with the broker,
// min(ProtocolVersion, broker version). It is 0 before Connect succeeds.
//
// A negotiated version below 2 means the broker is older than this SDK and will
// send only legacy error names, so ServerError.Code and ServerError.Hint will be
// empty.
func (c *Client) ProtocolVersion() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.protocolVersion
}

// Subscribe registers a handler for messages on the given topic.
// Note: Subscribe is fire-and-forget like the JS SDK - it doesn't wait for server acknowledgment.
//
// The subscription, with its options, is part of the session: after a
// reconnect it is restored with its current filters. Subscribing to a topic
// again replaces its handler and options.
func (c *Client) Subscribe(topic string, handler MessageHandler, opts ...SubscribeOptions) error {
	c.sessionMu.Lock()
	defer c.sessionMu.Unlock()

	session, connected := c.connectedSession()
	if !connected {
		return ErrNotConnected
	}

	sub := c.newSubscription(handler, opts)

	// Fire-and-forget like JS SDK - don't wait for ack
	if err := c.send(sub.frame(topic)); err != nil {
		return fmt.Errorf("subscribe failed: %w", err)
	}

	c.mu.Lock()
	if c.session == session {
		c.subscriptions[topic] = sub
	}
	c.mu.Unlock()

	c.log("Subscribed to:", topic)
	return nil
}

// Unsubscribe removes a subscription from a topic.
// Note: Unsubscribe is fire-and-forget like the JS SDK.
//
// The topic leaves the session even when Unsubscribe returns an error (for
// example ErrNotConnected during a reconnect), so it is not restored later.
func (c *Client) Unsubscribe(topic string) error {
	c.sessionMu.Lock()
	defer c.sessionMu.Unlock()

	c.mu.Lock()
	delete(c.subscriptions, topic)
	connected := c.status == StatusConnected
	c.mu.Unlock()

	if !connected {
		return ErrNotConnected
	}

	msg := &protocolMessage{
		Type:  msgTypeUnsub,
		Topic: topic,
	}

	// Fire-and-forget like JS SDK
	if err := c.send(msg); err != nil {
		return fmt.Errorf("unsubscribe failed: %w", err)
	}

	c.log("Unsubscribed from:", topic)
	return nil
}

// SetFilters replaces all filters for a topic.
// Empty slice switches back to wildcard (receive all messages).
// Accepts mixed items: string for OR, []string for AND groups.
//
// The new filters are recorded on the subscription, so a reconnect restores
// them rather than the ones passed to Subscribe.
func (c *Client) SetFilters(topic string, filters []any) error {
	c.sessionMu.Lock()
	defer c.sessionMu.Unlock()
	return c.setFiltersLocked(topic, filters)
}

// setFiltersLocked is SetFilters with c.sessionMu held.
func (c *Client) setFiltersLocked(topic string, filters []any) error {
	session, connected := c.connectedSession()
	if !connected {
		return ErrNotConnected
	}

	filters = cloneFilters(filters)
	msg := &setFiltersFrame{
		Type:    "setFilters",
		Topic:   topic,
		Filters: filters,
	}
	if msg.Filters == nil {
		msg.Filters = []any{}
	}

	if err := c.send(msg); err != nil {
		return fmt.Errorf("setFilters failed: %w", err)
	}

	c.mu.Lock()
	if sub, ok := c.subscriptions[topic]; ok && c.session == session {
		sub.filters = filters
		c.subscriptions[topic] = sub
	}
	c.mu.Unlock()

	return nil
}

// topicFilters returns the current filters of a subscribed topic.
func (c *Client) topicFilters(topic string) []any {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.subscriptions[topic].filters
}

// AddFilters adds simple string filters to the existing set for a topic.
// AND groups in the existing set are preserved.
func (c *Client) AddFilters(topic string, filters []string) error {
	c.sessionMu.Lock()
	defer c.sessionMu.Unlock()

	existing := c.topicFilters(topic)

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

	return c.setFiltersLocked(topic, result)
}

// RemoveFilters removes specific simple string filters from a topic.
// AND groups in the existing set are preserved.
func (c *Client) RemoveFilters(topic string, filters []string) error {
	c.sessionMu.Lock()
	defer c.sessionMu.Unlock()

	existing := c.topicFilters(topic)

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

	return c.setFiltersLocked(topic, result)
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
	qos := *c.options.QoS
	msg.QoS = &qos

	if len(opts) > 0 {
		opt := opts[0]
		if opt.QoS != nil {
			msg.QoS = opt.QoS
		}
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
//
// Presence is part of the session: the broker drops it with the connection,
// so the client sends the last room presence (and the last presence without
// a room) again after a reconnect.
func (c *Client) SetPresence(data map[string]any, roomID ...string) error {
	c.sessionMu.Lock()
	defer c.sessionMu.Unlock()

	session, connected := c.connectedSession()
	if !connected {
		return ErrNotConnected
	}

	room := ""
	if len(roomID) > 0 {
		room = roomID[0]
	}
	data = cloneMap(data)

	if err := c.send(presenceFrame(data, room)); err != nil {
		return err
	}

	c.mu.Lock()
	if c.session == session {
		if room != "" {
			c.roomPresence = &roomPresence{roomID: room, data: data}
		} else {
			c.projectPresence = data
		}
	}
	c.mu.Unlock()
	return nil
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
	done := c.done
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
					// Persistent Presence: status (online|offline|waking)
					if s, ok := m["status"].(string); ok {
						presence.Status = s
					}
					result = append(result, presence)
				}
			}
		}
		return result, nil
	case <-time.After(10 * time.Second):
		return nil, fmt.Errorf("get presence: %w", ErrTimeout)
	case <-done:
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

// OnError registers a handler for errors reported by the broker.
//
// Subscribe and Emit are fire-and-forget and do not return broker-side failures,
// so this is the only way to observe problems such as subscribing to a room that
// does not exist or publishing to a topic the actor cannot write. Register it
// before calling Connect:
//
//	client := nolag.New(token)
//	client.OnError(func(err *nolag.ServerError) {
//		log.Printf("nolag: %v", err)
//	})
//	if err := client.Connect(); err != nil { ... }
//
// This is a typed wrapper over On("error", ...). Handlers run on the read loop,
// so do not block in them.
func (c *Client) OnError(handler ErrorHandler) {
	if handler == nil {
		return
	}
	c.On("error", func(args ...any) {
		if len(args) == 0 {
			return
		}
		if serverErr, ok := args[0].(*ServerError); ok {
			handler(serverErr)
			return
		}
		// Defensive: surface any other error shape without dropping it.
		if err, ok := args[0].(error); ok {
			handler(&ServerError{Name: err.Error()})
		}
	})
}

func (c *Client) emit(event string, args ...any) {
	c.mu.RLock()
	handlers := c.eventHandlers[event]
	c.mu.RUnlock()

	for _, h := range handlers {
		h(args...)
	}
}

// send writes one frame: a *protocolMessage, or a frame type of its own such
// as setFiltersFrame.
func (c *Client) send(msg any) error {
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
	done := c.done
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
	case <-done:
		return nil, ErrNotConnected
	}
}

// readLoop reads one connection until it fails or is dropped.
func (c *Client) readLoop(conn *websocket.Conn, done chan struct{}) {
	c.log("readLoop started")
	defer func() {
		c.log("readLoop exiting")
		c.handleDisconnect(conn)
	}()

	for {
		select {
		case <-done:
			c.log("readLoop: done channel closed")
			return
		default:
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
		sub, ok := c.subscriptions[msg.Topic]
		c.mu.RUnlock()
		handler := sub.handler

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
			// Persistent Presence: status (online|offline|waking). The event
			// name itself carries "waking" (emitted as presence:waking).
			if s, ok := eventData["status"].(string); ok {
				actor.Status = s
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

	case msgTypeHydration:
		// The hydration webhook's response body, forwarded once per subscribe.
		// msg.Topic is the bare topic name, not the room-qualified pattern.
		c.emit("hydration", msg.Topic, msg.Data)

	case msgTypeError:
		c.emit("error", &ServerError{
			Code:   msg.Code,
			Name:   msg.Error,
			Topic:  msg.Topic,
			Hint:   msg.Hint,
			MsgRef: msg.MsgRef,
		})
	}
}

// handleDisconnect runs when conn's read loop ends. If conn is still the
// current connection it is dropped, and an established connection starts
// the reconnect loop. A connection still being set up is left to connect,
// which sees the drop and fails.
func (c *Client) handleDisconnect(conn *websocket.Conn) {
	c.mu.Lock()
	if !c.dropConnLocked(conn) {
		c.mu.Unlock()
		return
	}
	wasConnected := c.status == StatusConnected
	c.status = StatusDisconnected
	session := c.session
	c.mu.Unlock()

	if !wasConnected {
		return
	}

	c.emit("disconnected")

	if c.options.Reconnect != nil && *c.options.Reconnect {
		go c.reconnect(session)
	}
}

// reconnect retries connect until it succeeds, the attempts run out, another
// caller connects, or Close ends the session.
func (c *Client) reconnect(session uint64) {
	for {
		c.mu.Lock()
		if c.session != session || c.status == StatusConnected || c.status == StatusConnecting {
			c.mu.Unlock()
			return
		}
		c.reconnectCount++
		attempt := c.reconnectCount
		if c.options.MaxReconnectAttempts > 0 && attempt > c.options.MaxReconnectAttempts {
			c.status = StatusDisconnected
			c.mu.Unlock()
			c.log("Max reconnect attempts reached")
			return
		}
		c.status = StatusReconnecting
		c.mu.Unlock()

		c.emit("reconnecting", attempt)
		c.log("Reconnecting... attempt", attempt)

		time.Sleep(c.options.ReconnectInterval)

		// connect restores the session before it reports connected.
		if err := c.connect(session); err != nil {
			c.log("Reconnect failed:", err)
			continue
		}
		return
	}
}

func (c *Client) startHeartbeat(conn *websocket.Conn) {
	if c.options.HeartbeatInterval <= 0 {
		return
	}

	c.mu.Lock()
	if c.conn != conn {
		c.mu.Unlock()
		return
	}
	done := make(chan struct{})
	c.heartbeatDone = done
	c.mu.Unlock()

	go func() {
		ticker := time.NewTicker(c.options.HeartbeatInterval)
		defer ticker.Stop()

		for {
			select {
			case <-done:
				return
			case <-ticker.C:
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
//
// Once subscribed, the lobby is part of the session and is subscribed again
// after a reconnect (the snapshot of that resubscribe is not delivered).
func (l *Lobby) Subscribe() (LobbyPresenceState, error) {
	ackKey := fmt.Sprintf("lobbySubscribed:%s", l.lobbyID)
	ackChan := make(chan *protocolMessage, 1)

	l.client.mu.Lock()
	if l.client.status != StatusConnected {
		l.client.mu.Unlock()
		return nil, ErrNotConnected
	}
	session := l.client.session
	done := l.client.done
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
		l.client.mu.Lock()
		if l.client.session == session {
			l.client.lobbies[l.lobbyID] = struct{}{}
		}
		l.client.mu.Unlock()
		return parseLobbyPresence(resp.Presence), nil
	case <-time.After(10 * time.Second):
		return nil, fmt.Errorf("lobby subscribe: %w", ErrTimeout)
	case <-done:
		return nil, ErrNotConnected
	}
}

// Unsubscribe unsubscribes from this lobby's presence events (fire-and-forget).
//
// The lobby leaves the session even when Unsubscribe returns an error, so it
// is not subscribed again after a reconnect.
func (l *Lobby) Unsubscribe() error {
	l.client.sessionMu.Lock()
	defer l.client.sessionMu.Unlock()

	l.client.mu.Lock()
	delete(l.client.lobbies, l.lobbyID)
	connected := l.client.status == StatusConnected
	l.client.mu.Unlock()

	if !connected {
		return ErrNotConnected
	}

	return l.client.send(&protocolMessage{
		Type:    msgTypeLobbyUnsubscribe,
		LobbyID: l.lobbyID,
	})
}

// FetchPresence fetches the current presence state for the lobby.
func (l *Lobby) FetchPresence() (LobbyPresenceState, error) {
	ackKey := fmt.Sprintf("lobbyPresenceList:%s", l.lobbyID)
	ackChan := make(chan *protocolMessage, 1)

	l.client.mu.Lock()
	if l.client.status != StatusConnected {
		l.client.mu.Unlock()
		return nil, ErrNotConnected
	}
	done := l.client.done
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
	case <-done:
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
