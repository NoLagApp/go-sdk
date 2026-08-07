package nolag

import "strconv"

// ServerError is a structured error frame reported by the NoLag broker over the
// WebSocket connection.
//
// Subscribe and Emit are fire-and-forget, so the broker reports problems such as
// an unknown topic or a permission refusal asynchronously rather than as a return
// value. Register a handler with Client.OnError to receive them; without one these
// errors are silent and a misconfigured app simply never delivers messages.
type ServerError struct {
	// Code is the numeric NoLag error code, for example 42940 for unknown_topic.
	// Zero when the broker did not send one.
	Code int

	// Name is the short machine-readable error name, for example "unknown_topic"
	// or "not_authorized". Always set.
	Name string

	// Topic is the topic pattern the error refers to, in
	// app-slug/room-slug/topic-name form. Empty for errors that are not
	// topic-scoped.
	Topic string

	// Hint is a human-readable suggestion for resolving the error, when the broker
	// provides one.
	Hint string

	// MsgRef references the client message that triggered the error, when the
	// error was raised in response to a specific publish.
	MsgRef string
}

// Error implements the error interface.
func (e *ServerError) Error() string {
	msg := e.Name
	if e.Code != 0 {
		msg += " (" + strconv.Itoa(e.Code) + ")"
	}
	if e.Topic != "" {
		msg += " on '" + e.Topic + "'"
	}
	if e.Hint != "" {
		msg += ": " + e.Hint
	}
	return msg
}

// ErrorHandler is called when the broker reports an error.
type ErrorHandler func(err *ServerError)
