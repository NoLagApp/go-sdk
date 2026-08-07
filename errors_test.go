package nolag

import (
	"errors"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// ServerError.Error() must degrade gracefully: the broker only sends code, topic
// and hint on protocol v2, so every combination has to render sensibly.
func TestServerErrorMessage(t *testing.T) {
	tests := []struct {
		name string
		err  ServerError
		want string
	}{
		{
			name: "full v2 frame",
			err: ServerError{
				Code:  42940,
				Name:  "unknown_topic",
				Topic: "my-app-a3f9/general/messages",
				Hint:  "room is not configured",
			},
			want: "unknown_topic (42940) on 'my-app-a3f9/general/messages': room is not configured",
		},
		{
			name: "legacy v1 frame carries only a name",
			err:  ServerError{Name: "not_authorized"},
			want: "not_authorized",
		},
		{
			name: "topic without code or hint",
			err:  ServerError{Name: "not_authorized", Topic: "app/room/topic"},
			want: "not_authorized on 'app/room/topic'",
		},
		{
			name: "code without topic",
			err:  ServerError{Code: 500, Name: "internal"},
			want: "internal (500)",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.err.Error(); got != tc.want {
				t.Fatalf("Error()\n got: %q\nwant: %q", got, tc.want)
			}
		})
	}
}

// ServerError must satisfy the error interface and stay retrievable through
// errors.As, so callers can use it in ordinary Go error handling.
func TestServerErrorImplementsError(t *testing.T) {
	var err error = &ServerError{Code: 42940, Name: "unknown_topic"}

	var target *ServerError
	if !errors.As(err, &target) {
		t.Fatal("errors.As failed to unwrap *ServerError")
	}
	if target.Code != 42940 {
		t.Fatalf("Code: want 42940, got %d", target.Code)
	}
}

// The v2 error frame must actually decode off the wire. This is the regression
// that mattered: the fields existed on the broker but had nowhere to land, so
// code and hint were silently dropped.
func TestErrorFrameDecodesFromMsgpack(t *testing.T) {
	frame := map[string]any{
		"type":   "error",
		"code":   42940,
		"error":  "unknown_topic",
		"topic":  "my-app-a3f9/general/messages",
		"hint":   "room is not configured, provision it via the rooms API",
		"msgRef": "17",
	}

	packed, err := msgpack.Marshal(frame)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var msg protocolMessage
	if err := msgpack.Unmarshal(packed, &msg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if msg.Type != msgTypeError {
		t.Fatalf("Type: want error, got %q", msg.Type)
	}
	if msg.Code != 42940 {
		t.Fatalf("Code: want 42940, got %d", msg.Code)
	}
	if msg.Error != "unknown_topic" {
		t.Fatalf("Error: want unknown_topic, got %q", msg.Error)
	}
	if msg.Topic != "my-app-a3f9/general/messages" {
		t.Fatalf("Topic: got %q", msg.Topic)
	}
	if msg.Hint == "" {
		t.Fatal("Hint was dropped during decode")
	}
	if msg.MsgRef != "17" {
		t.Fatalf("MsgRef: want 17, got %q", msg.MsgRef)
	}
}

// The auth frame must carry protocolVersion outbound, otherwise the broker
// treats the client as v1 and downgrades every error to a bare name.
func TestAuthFrameCarriesProtocolVersion(t *testing.T) {
	packed, err := msgpack.Marshal(&protocolMessage{
		Type:            msgTypeAuth,
		Token:           "tok",
		ProtocolVersion: ProtocolVersion,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded map[string]any
	if err := msgpack.Unmarshal(packed, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	got, ok := decoded["protocolVersion"]
	if !ok {
		t.Fatal("auth frame is missing protocolVersion; broker would treat this client as v1")
	}
	if v, _ := msgpack.Marshal(got); len(v) == 0 {
		t.Fatal("protocolVersion did not encode")
	}
	if ProtocolVersion < 2 {
		t.Fatalf("ProtocolVersion must be at least 2 for structured errors, got %d", ProtocolVersion)
	}
}

// handleMessage must deliver a *ServerError (not a bare errors.errorString) to
// both the generic On("error") handler and the typed OnError handler.
func TestHandleErrorMessageDeliversServerError(t *testing.T) {
	c := New("token")

	var generic any
	c.On("error", func(args ...any) {
		if len(args) > 0 {
			generic = args[0]
		}
	})

	var typed *ServerError
	c.OnError(func(err *ServerError) { typed = err })

	c.handleMessage(&protocolMessage{
		Type:   msgTypeError,
		Code:   42940,
		Error:  "unknown_topic",
		Topic:  "my-app-a3f9/general/messages",
		Hint:   "room is not configured",
		MsgRef: "9",
	})

	serverErr, ok := generic.(*ServerError)
	if !ok {
		t.Fatalf("On(\"error\") received %T, want *ServerError", generic)
	}
	if serverErr.Code != 42940 || serverErr.Name != "unknown_topic" {
		t.Fatalf("generic handler got wrong values: %+v", serverErr)
	}

	if typed == nil {
		t.Fatal("OnError handler was not called")
	}
	if typed.Topic != "my-app-a3f9/general/messages" {
		t.Fatalf("OnError Topic: got %q", typed.Topic)
	}
	if typed.Hint != "room is not configured" {
		t.Fatalf("OnError Hint: got %q", typed.Hint)
	}
	if typed.MsgRef != "9" {
		t.Fatalf("OnError MsgRef: got %q", typed.MsgRef)
	}
}

// A nil handler must not panic, and non-ServerError values sent to the generic
// event bus must still reach an OnError handler rather than vanishing.
func TestOnErrorEdgeCases(t *testing.T) {
	c := New("token")
	c.OnError(nil) // must not panic or register anything

	var got *ServerError
	c.OnError(func(err *ServerError) { got = err })

	// Connect() emits a plain error on dial failure, not a ServerError.
	c.emit("error", errors.New("dial failed"))

	if got == nil {
		t.Fatal("OnError did not receive a plain error")
	}
	if got.Name != "dial failed" {
		t.Fatalf("Name: want %q, got %q", "dial failed", got.Name)
	}
}
