package nolag

import (
	"testing"
	"time"
)

// Before v0.5.0 an Options literal that did not mention Reconnect silently
// disabled reconnection, because the bool was copied unconditionally.
func TestMergeOptionsKeepsReconnectDefaultWhenOmitted(t *testing.T) {
	got := mergeOptions(DefaultOptions(), Options{URL: "wss://example.test/ws"})
	if got.Reconnect == nil || !*got.Reconnect {
		t.Fatalf("Reconnect should default to true when omitted, got %v", got.Reconnect)
	}
	if got.URL != "wss://example.test/ws" {
		t.Fatalf("URL not applied: %q", got.URL)
	}
}

func TestMergeOptionsHonoursExplicitReconnectFalse(t *testing.T) {
	got := mergeOptions(DefaultOptions(), Options{Reconnect: Bool(false)})
	if got.Reconnect == nil || *got.Reconnect {
		t.Fatalf("Reconnect should be false when set explicitly, got %v", got.Reconnect)
	}
}

func TestMergeOptionsReconnectAttempts(t *testing.T) {
	cases := []struct {
		in   int
		want int
	}{
		{0, 10}, // not set: default
		{3, 3},  // explicit
		{-1, 0}, // unlimited (the reconnect loop treats 0 as no cap)
	}
	for _, c := range cases {
		got := mergeOptions(DefaultOptions(), Options{MaxReconnectAttempts: c.in})
		if got.MaxReconnectAttempts != c.want {
			t.Errorf("MaxReconnectAttempts %d: want %d, got %d", c.in, c.want, got.MaxReconnectAttempts)
		}
	}
}

func TestMergeOptionsHeartbeat(t *testing.T) {
	if got := mergeOptions(DefaultOptions(), Options{}); got.HeartbeatInterval != 30*time.Second {
		t.Fatalf("omitted heartbeat should keep 30s, got %v", got.HeartbeatInterval)
	}
	if got := mergeOptions(DefaultOptions(), Options{HeartbeatInterval: -1}); got.HeartbeatInterval != 0 {
		t.Fatalf("negative heartbeat should disable, got %v", got.HeartbeatInterval)
	}
}

// The broker's hydration frame must reach a handler with the bare topic
// name and the webhook body.
func TestHydrationFrameIsEmitted(t *testing.T) {
	c := New("test-token", Options{Reconnect: Bool(false)})
	var gotTopic string
	var gotData any
	c.On("hydration", func(args ...any) {
		gotTopic, _ = args[0].(string)
		gotData = args[1]
	})

	c.handleMessage(&protocolMessage{
		Type:  msgTypeHydration,
		Topic: "messages",
		Data:  map[string]any{"items": []any{1, 2}},
	})

	if gotTopic != "messages" {
		t.Fatalf("topic: want messages, got %q", gotTopic)
	}
	if m, ok := gotData.(map[string]any); !ok || len(m["items"].([]any)) != 2 {
		t.Fatalf("data not forwarded: %#v", gotData)
	}
}
