// Package nolag provides a real-time messaging SDK for Go applications.
package nolag

import "time"

// ConnectionStatus represents the current connection state.
type ConnectionStatus string

const (
	StatusDisconnected ConnectionStatus = "disconnected"
	StatusConnecting   ConnectionStatus = "connecting"
	StatusConnected    ConnectionStatus = "connected"
	StatusReconnecting ConnectionStatus = "reconnecting"
)

// ActorType represents the type of actor.
type ActorType string

const (
	ActorDevice  ActorType = "device"
	ActorUser    ActorType = "user"
	ActorServer  ActorType = "server"
	ActorSession ActorType = "session"
)

// QoS represents the quality of service level.
type QoS int

const (
	QoSAtMostOnce  QoS = 0 // Fire and forget
	QoSAtLeastOnce QoS = 1 // Guaranteed delivery, may duplicate
	QoSExactlyOnce QoS = 2 // Guaranteed exactly once
)

// Options configures the NoLag client.
type Options struct {
	// URL is the WebSocket endpoint. Default: "wss://broker.nolag.app/ws"
	URL string

	// Reconnect enables automatic reconnection. Default: true
	Reconnect bool

	// ReconnectInterval is the time between reconnect attempts. Default: 5s
	ReconnectInterval time.Duration

	// MaxReconnectAttempts is the max reconnection attempts (0 = infinite). Default: 10
	MaxReconnectAttempts int

	// HeartbeatInterval is the interval for heartbeat pings. Default: 30s, 0 to disable
	HeartbeatInterval time.Duration

	// QoS is the default quality of service level. Default: QoSAtLeastOnce
	QoS QoS

	// LoadBalance enables load balancing across subscribers. Default: false
	LoadBalance bool

	// LoadBalanceGroup is the group name for load balancing.
	LoadBalanceGroup string

	// ActorTokenID is an optional actor token identifier.
	ActorTokenID string

	// Debug enables debug logging. Default: false
	Debug bool
}

// DefaultOptions returns options with sensible defaults.
func DefaultOptions() Options {
	return Options{
		URL:                  "wss://broker.nolag.app/ws",
		Reconnect:            true,
		ReconnectInterval:    5 * time.Second,
		MaxReconnectAttempts: 10,
		HeartbeatInterval:    30 * time.Second,
		QoS:                  QoSAtLeastOnce,
		LoadBalance:          false,
		Debug:                false,
	}
}

// SubscribeOptions configures a subscription.
type SubscribeOptions struct {
	QoS              *QoS
	LoadBalance      *bool
	LoadBalanceGroup string
}

// EmitOptions configures a message publish.
type EmitOptions struct {
	QoS    *QoS
	Retain bool
	Echo   *bool // Whether to receive this message back if subscribed (default: true)
}

// MessageMeta contains metadata about a received message.
type MessageMeta struct {
	Sender    string
	Timestamp time.Time
	IsReplay  bool   // Whether this message is being replayed from history
	MsgID     string // Unique message ID for ACK
}

// ActorPresence represents presence information for an actor.
type ActorPresence struct {
	ActorTokenID string
	ActorType    ActorType
	Presence     map[string]any
	JoinedAt     time.Time
}

// MessageHandler is called when a message is received on a subscribed topic.
type MessageHandler func(data any, meta MessageMeta)

// EventHandler is a generic event callback.
type EventHandler func(args ...any)

// Message types for the protocol
type messageType string

const (
	msgTypeAuth      messageType = "auth"
	msgTypeAuthAck   messageType = "auth_ack"
	msgTypeSubscribe messageType = "subscribe"
	msgTypeSubAck    messageType = "sub_ack"
	msgTypeUnsub     messageType = "unsubscribe"
	msgTypeUnsubAck  messageType = "unsub_ack"
	msgTypePublish   messageType = "publish"
	msgTypePubAck    messageType = "pub_ack"
	msgTypeMessage   messageType = "message"
	msgTypePresence  messageType = "presence"
	msgTypePresGet   messageType = "presence_get"
	msgTypePresSet   messageType = "presence_set"
	msgTypeError     messageType = "error"
)

// Internal message structure
type protocolMessage struct {
	Type             messageType    `msgpack:"type"`
	Topic            string         `msgpack:"topic,omitempty"`
	Data             any            `msgpack:"data,omitempty"`
	Token            string         `msgpack:"token,omitempty"`
	ID               string         `msgpack:"id,omitempty"`
	QoS              QoS            `msgpack:"qos,omitempty"`
	Retain           bool           `msgpack:"retain,omitempty"`
	Echo             *bool          `msgpack:"echo,omitempty"`
	LoadBalance      bool           `msgpack:"loadBalance,omitempty"`
	LoadBalanceGroup string         `msgpack:"loadBalanceGroup,omitempty"`
	Reconnect        bool           `msgpack:"reconnect,omitempty"`
	Options          map[string]any `msgpack:"options,omitempty"`
	Error            string         `msgpack:"error,omitempty"`
	Meta             *messageMeta   `msgpack:"meta,omitempty"`
	IsReplay         bool           `msgpack:"isReplay,omitempty"`
	MsgID            string         `msgpack:"msgId,omitempty"`
	// Auth response fields
	Success      bool   `msgpack:"success,omitempty"`
	ActorTokenId string `msgpack:"actorTokenId,omitempty"`
}

type messageMeta struct {
	Sender    string  `msgpack:"sender,omitempty"`
	Timestamp float64 `msgpack:"timestamp,omitempty"`
}
