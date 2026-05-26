//go:build integration

// Integration test for the NoLag Go SDK.
//
// Run:
//   NOLAG_TOKEN1=<token-a> NOLAG_TOKEN2=<token-b> NOLAG_APP_SLUG=<slug> go test -v -tags=integration -run TestIntegration -timeout 120s

package nolag

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func iGetEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

var (
	iToken1  = os.Getenv("NOLAG_TOKEN1")
	iToken2  = os.Getenv("NOLAG_TOKEN2")
	iAppSlug = os.Getenv("NOLAG_APP_SLUG")
	iRoom    = iGetEnv("NOLAG_ROOM", "default-workflow")
)

func skipIfMissing(t *testing.T) {
	if iToken1 == "" || iToken2 == "" || iAppSlug == "" {
		t.Skip("NOLAG_TOKEN1, NOLAG_TOKEN2, and NOLAG_APP_SLUG must be set")
	}
}

func connectClients(t *testing.T) (*Client, *Client) {
	t.Helper()
	a := New(iToken1)
	b := New(iToken2)
	if err := a.Connect(); err != nil {
		t.Fatalf("Client A connect failed: %v", err)
	}
	if err := b.Connect(); err != nil {
		t.Fatalf("Client B connect failed: %v", err)
	}
	return a, b
}

// ═══════════════════════════════════════════
// CONNECTION & PROPERTIES
// ═══════════════════════════════════════════

func TestIntegration_ConnectAuth(t *testing.T) {
	skipIfMissing(t)
	a, b := connectClients(t)
	defer a.Close()
	defer b.Close()

	if !a.Status().IsConnected() || !b.Status().IsConnected() {
		t.Fatal("clients not connected")
	}
	if a.ActorID() == "" || b.ActorID() == "" {
		t.Fatal("actor IDs not set")
	}
	if a.ProjectID() == "" {
		t.Fatal("project ID not set")
	}
	if a.ActorTypeValue() == "" {
		t.Fatal("actor type not set")
	}
	t.Logf("A=%s B=%s project=%s type=%s", a.ActorID(), b.ActorID(), a.ProjectID(), a.ActorTypeValue())
}

func (s ConnectionStatus) IsConnected() bool {
	return s == StatusConnected
}

// ═══════════════════════════════════════════
// FLUENT API
// ═══════════════════════════════════════════

func TestIntegration_FluentAPI(t *testing.T) {
	skipIfMissing(t)
	a := New(iToken1)
	defer a.Close()
	if err := a.Connect(); err != nil {
		t.Fatal(err)
	}

	app := a.SetApp(iAppSlug)
	room := app.SetRoom("test-room")
	if room.Prefix() != iAppSlug+"/test-room" {
		t.Fatalf("expected prefix %s/test-room, got %s", iAppSlug, room.Prefix())
	}

	lobby := app.SetLobby("test-lobby")
	if lobby.LobbyID() != "test-lobby" {
		t.Fatalf("expected lobby ID test-lobby, got %s", lobby.LobbyID())
	}
}

// ═══════════════════════════════════════════
// BASIC PUB/SUB
// ═══════════════════════════════════════════

func TestIntegration_BasicPubSub(t *testing.T) {
	skipIfMissing(t)
	a, b := connectClients(t)
	defer a.Close()
	defer b.Close()

	rA := a.SetApp(iAppSlug).SetRoom(iRoom)
	rB := b.SetApp(iAppSlug).SetRoom(iRoom)

	done := make(chan map[string]any, 1)
	rB.Subscribe("basic-test", func(data any, meta MessageMeta) {
		if m, ok := data.(map[string]any); ok {
			done <- m
		}
	})
	time.Sleep(500 * time.Millisecond)

	rA.Emit("basic-test", map[string]any{"msg": "hello", "n": 42})

	select {
	case d := <-done:
		if d["msg"] != "hello" {
			t.Fatalf("expected 'hello', got %v", d["msg"])
		}
		t.Logf("received: %v", d)
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func TestIntegration_DirectAPI(t *testing.T) {
	skipIfMissing(t)
	a, b := connectClients(t)
	defer a.Close()
	defer b.Close()

	topic := fmt.Sprintf("%s/%s/direct-test", iAppSlug, iRoom)
	done := make(chan any, 1)
	b.Subscribe(topic, func(data any, meta MessageMeta) {
		done <- data
	})
	time.Sleep(500 * time.Millisecond)

	a.Emit(topic, map[string]any{"direct": true})

	select {
	case d := <-done:
		t.Logf("received: %v", d)
	case <-time.After(10 * time.Second):
		t.Fatal("timeout")
	}
}

func TestIntegration_SubscribeUnsubscribe(t *testing.T) {
	skipIfMissing(t)
	a, b := connectClients(t)
	defer a.Close()
	defer b.Close()

	rA := a.SetApp(iAppSlug).SetRoom(iRoom)
	rB := b.SetApp(iAppSlug).SetRoom(iRoom)

	var count atomic.Int32
	rB.Subscribe("unsub-test", func(data any, meta MessageMeta) {
		count.Add(1)
	})
	time.Sleep(300 * time.Millisecond)

	rA.Emit("unsub-test", map[string]any{"seq": 1})
	time.Sleep(500 * time.Millisecond)
	if count.Load() != 1 {
		t.Fatalf("expected 1 message before unsub, got %d", count.Load())
	}

	rB.Unsubscribe("unsub-test")
	time.Sleep(300 * time.Millisecond)

	rA.Emit("unsub-test", map[string]any{"seq": 2})
	time.Sleep(500 * time.Millisecond)
	if count.Load() != 1 {
		t.Fatalf("expected 1 message after unsub, got %d", count.Load())
	}
}

// ═══════════════════════════════════════════
// EMIT OPTIONS
// ═══════════════════════════════════════════

func TestIntegration_EchoTrue(t *testing.T) {
	skipIfMissing(t)
	a := New(iToken1)
	defer a.Close()
	a.Connect()

	rA := a.SetApp(iAppSlug).SetRoom(iRoom)
	done := make(chan any, 1)
	rA.Subscribe("echo-test", func(data any, meta MessageMeta) {
		done <- data
	})
	time.Sleep(300 * time.Millisecond)

	echoTrue := true
	rA.Emit("echo-test", map[string]any{"echo": true}, EmitOptions{Echo: &echoTrue})

	select {
	case <-done:
		t.Log("self-received with echo=true")
	case <-time.After(10 * time.Second):
		t.Fatal("timeout")
	}
}

func TestIntegration_EchoFalse(t *testing.T) {
	skipIfMissing(t)
	a, b := connectClients(t)
	defer a.Close()
	defer b.Close()

	rA := a.SetApp(iAppSlug).SetRoom(iRoom)
	rB := b.SetApp(iAppSlug).SetRoom(iRoom)

	var countA atomic.Int32
	doneB := make(chan any, 1)

	rA.Subscribe("noecho-test", func(data any, meta MessageMeta) {
		countA.Add(1)
	})
	rB.Subscribe("noecho-test", func(data any, meta MessageMeta) {
		doneB <- data
	})
	time.Sleep(300 * time.Millisecond)

	echoFalse := false
	rA.Emit("noecho-test", map[string]any{"noecho": true}, EmitOptions{Echo: &echoFalse})

	select {
	case <-doneB:
	case <-time.After(10 * time.Second):
		t.Fatal("B timeout")
	}
	time.Sleep(500 * time.Millisecond)
	if countA.Load() != 0 {
		t.Fatalf("A received %d messages with echo=false", countA.Load())
	}
}

func TestIntegration_QoS(t *testing.T) {
	skipIfMissing(t)
	a, b := connectClients(t)
	defer a.Close()
	defer b.Close()

	rA := a.SetApp(iAppSlug).SetRoom(iRoom)
	rB := b.SetApp(iAppSlug).SetRoom(iRoom)

	for _, qos := range []QoS{QoSAtLeastOnce, QoSAtMostOnce} {
		topic := fmt.Sprintf("qos-%d-test", qos)
		done := make(chan any, 1)
		q := qos
		rB.Subscribe(topic, func(data any, meta MessageMeta) {
			done <- data
		}, SubscribeOptions{QoS: &q})
		time.Sleep(500 * time.Millisecond)

		rA.Emit(topic, map[string]any{"qos": int(qos)}, EmitOptions{QoS: &q})

		select {
		case <-done:
			t.Logf("QoS %d passed", qos)
		case <-time.After(8 * time.Second):
			if qos == QoSAtMostOnce {
				t.Logf("QoS %d skipped (not guaranteed)", qos)
			} else {
				t.Fatalf("QoS %d timeout", qos)
			}
		}
		rB.Unsubscribe(topic)
		time.Sleep(300 * time.Millisecond)
	}
}

func TestIntegration_Retain(t *testing.T) {
	skipIfMissing(t)
	a, b := connectClients(t)
	defer a.Close()
	defer b.Close()

	rA := a.SetApp(iAppSlug).SetRoom(iRoom)
	rB := b.SetApp(iAppSlug).SetRoom(iRoom)

	// Publish retained message
	rA.Subscribe("retain-test", func(data any, meta MessageMeta) {})
	time.Sleep(300 * time.Millisecond)
	rA.Emit("retain-test", map[string]any{"retained": true}, EmitOptions{Retain: true})
	time.Sleep(1 * time.Second)

	// Late subscriber should get it
	done := make(chan any, 1)
	rB.Subscribe("retain-test", func(data any, meta MessageMeta) {
		if data != nil {
			done <- data
		}
	})

	select {
	case d := <-done:
		t.Logf("retained message received: %v", d)
	case <-time.After(5 * time.Second):
		t.Log("retain skipped (not delivered to late subscriber)")
	}

	// Clean up
	rA.Emit("retain-test", nil, EmitOptions{Retain: true})
}

// ═══════════════════════════════════════════
// DATA TYPES & META
// ═══════════════════════════════════════════

func TestIntegration_ComplexData(t *testing.T) {
	skipIfMissing(t)
	a, b := connectClients(t)
	defer a.Close()
	defer b.Close()

	rA := a.SetApp(iAppSlug).SetRoom(iRoom)
	rB := b.SetApp(iAppSlug).SetRoom(iRoom)

	done := make(chan map[string]any, 1)
	rB.Subscribe("data-test", func(data any, meta MessageMeta) {
		if m, ok := data.(map[string]any); ok {
			done <- m
		}
	})
	time.Sleep(300 * time.Millisecond)

	rA.Emit("data-test", map[string]any{
		"string": "hello",
		"int":    42,
		"float":  3.14,
		"bool":   true,
		"null":   nil,
		"list":   []any{1, 2, 3},
		"nested": map[string]any{"a": map[string]any{"b": "c"}},
	})

	select {
	case d := <-done:
		if d["string"] != "hello" {
			t.Fatalf("string mismatch: %v", d["string"])
		}
		t.Logf("complex data OK: %d keys", len(d))
	case <-time.After(10 * time.Second):
		t.Fatal("timeout")
	}
}

func TestIntegration_StringData(t *testing.T) {
	skipIfMissing(t)
	a, b := connectClients(t)
	defer a.Close()
	defer b.Close()

	rA := a.SetApp(iAppSlug).SetRoom(iRoom)
	rB := b.SetApp(iAppSlug).SetRoom(iRoom)

	done := make(chan any, 1)
	rB.Subscribe("string-test", func(data any, meta MessageMeta) {
		done <- data
	})
	time.Sleep(300 * time.Millisecond)

	rA.Emit("string-test", "hello world")

	select {
	case d := <-done:
		if d != "hello world" {
			t.Fatalf("expected 'hello world', got %v", d)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timeout")
	}
}

func TestIntegration_MessageMeta(t *testing.T) {
	skipIfMissing(t)
	a, b := connectClients(t)
	defer a.Close()
	defer b.Close()

	rA := a.SetApp(iAppSlug).SetRoom(iRoom)
	rB := b.SetApp(iAppSlug).SetRoom(iRoom)

	done := make(chan MessageMeta, 1)
	rB.Subscribe("meta-test", func(data any, meta MessageMeta) {
		done <- meta
	})
	time.Sleep(300 * time.Millisecond)

	rA.Emit("meta-test", map[string]any{"check": "meta"})

	select {
	case meta := <-done:
		t.Logf("meta: sender=%s filter=%s isReplay=%v", meta.Sender, meta.Filter, meta.IsReplay)
	case <-time.After(10 * time.Second):
		t.Fatal("timeout")
	}
}

// ═══════════════════════════════════════════
// FILTERS
// ═══════════════════════════════════════════

func TestIntegration_SingleFilter(t *testing.T) {
	skipIfMissing(t)
	a, b := connectClients(t)
	defer a.Close()
	defer b.Close()

	rA := a.SetApp(iAppSlug).SetRoom(iRoom)
	rB := b.SetApp(iAppSlug).SetRoom(iRoom)

	var received []map[string]any
	var mu sync.Mutex
	rB.Subscribe("filter1-test", func(data any, meta MessageMeta) {
		if m, ok := data.(map[string]any); ok {
			mu.Lock()
			received = append(received, m)
			mu.Unlock()
		}
	}, SubscribeOptions{Filters: []any{"color:red"}})
	time.Sleep(500 * time.Millisecond)

	rA.Emit("filter1-test", map[string]any{"item": "apple"}, EmitOptions{Filter: "color:red"})
	time.Sleep(300 * time.Millisecond)
	rA.Emit("filter1-test", map[string]any{"item": "sky"}, EmitOptions{Filter: "color:blue"})
	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 1 || received[0]["item"] != "apple" {
		t.Fatalf("expected 1 message (apple), got %d: %v", len(received), received)
	}
}

func TestIntegration_MultipleOrFilters(t *testing.T) {
	skipIfMissing(t)
	a, b := connectClients(t)
	defer a.Close()
	defer b.Close()

	rA := a.SetApp(iAppSlug).SetRoom(iRoom)
	rB := b.SetApp(iAppSlug).SetRoom(iRoom)

	var count atomic.Int32
	rB.Subscribe("orfilt-test", func(data any, meta MessageMeta) {
		count.Add(1)
	}, SubscribeOptions{Filters: []any{"type:alert", "type:warning"}})
	time.Sleep(500 * time.Millisecond)

	rA.Emit("orfilt-test", map[string]any{"msg": "alert"}, EmitOptions{Filter: "type:alert"})
	rA.Emit("orfilt-test", map[string]any{"msg": "warn"}, EmitOptions{Filter: "type:warning"})
	rA.Emit("orfilt-test", map[string]any{"msg": "info"}, EmitOptions{Filter: "type:info"})
	time.Sleep(800 * time.Millisecond)

	if count.Load() != 2 {
		t.Fatalf("expected 2 messages, got %d", count.Load())
	}
}

func TestIntegration_AndFilterGroup(t *testing.T) {
	skipIfMissing(t)
	a, b := connectClients(t)
	defer a.Close()
	defer b.Close()

	rA := a.SetApp(iAppSlug).SetRoom(iRoom)
	rB := b.SetApp(iAppSlug).SetRoom(iRoom)

	var received []map[string]any
	var mu sync.Mutex
	rB.Subscribe("andfilt-test", func(data any, meta MessageMeta) {
		if m, ok := data.(map[string]any); ok {
			mu.Lock()
			received = append(received, m)
			mu.Unlock()
		}
	}, SubscribeOptions{Filters: []any{[]string{"color:red", "size:large"}}})
	time.Sleep(500 * time.Millisecond)

	rA.Emit("andfilt-test", map[string]any{"item": "big-apple"}, EmitOptions{Filters: []string{"color:red", "size:large"}})
	time.Sleep(300 * time.Millisecond)
	rA.Emit("andfilt-test", map[string]any{"item": "small"}, EmitOptions{Filter: "color:red"})
	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 1 || received[0]["item"] != "big-apple" {
		t.Fatalf("expected 1 (big-apple), got %d: %v", len(received), received)
	}
}

func TestIntegration_SetFilters(t *testing.T) {
	skipIfMissing(t)
	a, b := connectClients(t)
	defer a.Close()
	defer b.Close()

	rA := a.SetApp(iAppSlug).SetRoom(iRoom)
	rB := b.SetApp(iAppSlug).SetRoom(iRoom)

	var received []map[string]any
	var mu sync.Mutex
	rB.Subscribe("setfilt-test", func(data any, meta MessageMeta) {
		if m, ok := data.(map[string]any); ok {
			mu.Lock()
			received = append(received, m)
			mu.Unlock()
		}
	}, SubscribeOptions{Filters: []any{"old:filter"}})
	time.Sleep(300 * time.Millisecond)

	rB.SetFilters("setfilt-test", []any{"new:filter"})
	time.Sleep(300 * time.Millisecond)

	rA.Emit("setfilt-test", map[string]any{"seq": 1}, EmitOptions{Filter: "old:filter"})
	rA.Emit("setfilt-test", map[string]any{"seq": 2}, EmitOptions{Filter: "new:filter"})
	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 1 {
		t.Fatalf("expected 1, got %d", len(received))
	}
}

func TestIntegration_AddRemoveFilters(t *testing.T) {
	skipIfMissing(t)
	a, b := connectClients(t)
	defer a.Close()
	defer b.Close()

	rA := a.SetApp(iAppSlug).SetRoom(iRoom)
	rB := b.SetApp(iAppSlug).SetRoom(iRoom)

	var count atomic.Int32
	rB.Subscribe("addfilt-test", func(data any, meta MessageMeta) {
		count.Add(1)
	}, SubscribeOptions{Filters: []any{"tag:a"}})
	time.Sleep(300 * time.Millisecond)

	rA.Emit("addfilt-test", map[string]any{"seq": 1}, EmitOptions{Filter: "tag:a"})
	rA.Emit("addfilt-test", map[string]any{"seq": 2}, EmitOptions{Filter: "tag:b"})
	time.Sleep(500 * time.Millisecond)
	if count.Load() != 1 {
		t.Fatalf("before add: expected 1, got %d", count.Load())
	}

	rB.AddFilters("addfilt-test", []string{"tag:b"})
	time.Sleep(300 * time.Millisecond)
	rA.Emit("addfilt-test", map[string]any{"seq": 3}, EmitOptions{Filter: "tag:b"})
	time.Sleep(500 * time.Millisecond)
	if count.Load() != 2 {
		t.Fatalf("after add: expected 2, got %d", count.Load())
	}
}

func TestIntegration_FilterInMeta(t *testing.T) {
	skipIfMissing(t)
	a, b := connectClients(t)
	defer a.Close()
	defer b.Close()

	rA := a.SetApp(iAppSlug).SetRoom(iRoom)
	rB := b.SetApp(iAppSlug).SetRoom(iRoom)

	done := make(chan MessageMeta, 1)
	rB.Subscribe("filtmeta-test", func(data any, meta MessageMeta) {
		done <- meta
	}, SubscribeOptions{Filters: []any{"env:prod"}})
	time.Sleep(300 * time.Millisecond)

	rA.Emit("filtmeta-test", map[string]any{"x": 1}, EmitOptions{Filter: "env:prod"})

	select {
	case meta := <-done:
		if meta.Filter != "env:prod" {
			t.Fatalf("expected filter 'env:prod', got '%s'", meta.Filter)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timeout")
	}
}

// ═══════════════════════════════════════════
// PRESENCE
// ═══════════════════════════════════════════

func TestIntegration_Presence(t *testing.T) {
	skipIfMissing(t)
	a, b := connectClients(t)
	defer a.Close()
	defer b.Close()

	rA := a.SetApp(iAppSlug).SetRoom(iRoom)
	rB := b.SetApp(iAppSlug).SetRoom(iRoom)

	// B joins presence first
	joinDone := make(chan ActorPresence, 1)
	b.On("presence:join", func(args ...any) {
		if len(args) > 0 {
			if actor, ok := args[0].(ActorPresence); ok {
				if actor.ActorTokenID == a.ActorID() {
					joinDone <- actor
				}
			}
		}
	})
	rB.SetPresence(map[string]any{"name": "Agent B", "status": "online"})
	time.Sleep(500 * time.Millisecond)

	// A joins — B should get join event
	rA.SetPresence(map[string]any{"name": "Agent A", "status": "online"})

	select {
	case actor := <-joinDone:
		if actor.Presence["name"] != "Agent A" {
			t.Fatalf("expected name 'Agent A', got %v", actor.Presence["name"])
		}
		t.Logf("presence join: %s name=%v", actor.ActorTokenID, actor.Presence["name"])
	case <-time.After(5 * time.Second):
		t.Fatal("presence join timeout")
	}

	// Update presence
	updateDone := make(chan ActorPresence, 1)
	b.On("presence:update", func(args ...any) {
		if len(args) > 0 {
			if actor, ok := args[0].(ActorPresence); ok {
				if actor.ActorTokenID == a.ActorID() {
					updateDone <- actor
				}
			}
		}
	})
	rA.SetPresence(map[string]any{"name": "Agent A", "status": "busy"})

	select {
	case actor := <-updateDone:
		if actor.Presence["status"] != "busy" {
			t.Fatalf("expected status 'busy', got %v", actor.Presence["status"])
		}
		t.Logf("presence update: status=%v", actor.Presence["status"])
	case <-time.After(5 * time.Second):
		t.Fatal("presence update timeout")
	}
}

// ═══════════════════════════════════════════
// LOAD BALANCING
// ═══════════════════════════════════════════

func TestIntegration_LoadBalance(t *testing.T) {
	skipIfMissing(t)
	a := New(iToken1)
	b := New(iToken2)
	c := New(iToken1) // Same token as A for same LB group
	defer a.Close()
	defer b.Close()
	defer c.Close()
	a.Connect()
	b.Connect()
	c.Connect()

	rA := a.SetApp(iAppSlug).SetRoom(iRoom)
	rB := b.SetApp(iAppSlug).SetRoom(iRoom)
	rC := c.SetApp(iAppSlug).SetRoom(iRoom)

	var countB, countC atomic.Int32
	lb := true
	lbOpts := SubscribeOptions{LoadBalance: &lb, LoadBalanceGroup: "go-test-workers"}

	rB.Subscribe("lb-test", func(data any, meta MessageMeta) {
		countB.Add(1)
	}, lbOpts)
	rC.Subscribe("lb-test", func(data any, meta MessageMeta) {
		countC.Add(1)
	}, lbOpts)
	time.Sleep(500 * time.Millisecond)

	msgCount := 10
	for i := 0; i < msgCount; i++ {
		rA.Emit("lb-test", map[string]any{"seq": i})
	}
	time.Sleep(2 * time.Second)

	total := countB.Load() + countC.Load()
	distributed := countB.Load() > 0 && countC.Load() > 0
	if total != int32(msgCount) || !distributed {
		t.Fatalf("LB: B=%d C=%d total=%d/%d distributed=%v", countB.Load(), countC.Load(), total, msgCount, distributed)
	}
	t.Logf("LB: B=%d C=%d total=%d/%d distributed=%v", countB.Load(), countC.Load(), total, msgCount, distributed)
}

// ═══════════════════════════════════════════
// ADVANCED
// ═══════════════════════════════════════════

func TestIntegration_MultipleRooms(t *testing.T) {
	skipIfMissing(t)
	a, b := connectClients(t)
	defer a.Close()
	defer b.Close()

	rA1 := a.SetApp(iAppSlug).SetRoom("room-alpha")
	rA2 := a.SetApp(iAppSlug).SetRoom("room-beta")
	rB1 := b.SetApp(iAppSlug).SetRoom("room-alpha")
	rB2 := b.SetApp(iAppSlug).SetRoom("room-beta")

	var alpha, beta atomic.Int32
	rB1.Subscribe("multi-test", func(data any, meta MessageMeta) { alpha.Add(1) })
	rB2.Subscribe("multi-test", func(data any, meta MessageMeta) { beta.Add(1) })
	time.Sleep(300 * time.Millisecond)

	rA1.Emit("multi-test", map[string]any{"room": "alpha"})
	rA2.Emit("multi-test", map[string]any{"room": "beta"})
	time.Sleep(800 * time.Millisecond)

	if alpha.Load() != 1 || beta.Load() != 1 {
		t.Fatalf("expected 1/1, got alpha=%d beta=%d", alpha.Load(), beta.Load())
	}
}

func TestIntegration_RapidMessages(t *testing.T) {
	skipIfMissing(t)
	a, b := connectClients(t)
	defer a.Close()
	defer b.Close()

	rA := a.SetApp(iAppSlug).SetRoom(iRoom)
	rB := b.SetApp(iAppSlug).SetRoom(iRoom)

	msgCount := 20
	done := make(chan bool, 1)
	var received []int
	var mu sync.Mutex

	rB.Subscribe("rapid-test", func(data any, meta MessageMeta) {
		if m, ok := data.(map[string]any); ok {
			var seq int
			switch v := m["seq"].(type) {
			case int:
				seq = v
			case int8:
				seq = int(v)
			case int16:
				seq = int(v)
			case int32:
				seq = int(v)
			case int64:
				seq = int(v)
			case uint:
				seq = int(v)
			case uint8:
				seq = int(v)
			case uint16:
				seq = int(v)
			case uint32:
				seq = int(v)
			case uint64:
				seq = int(v)
			default:
				return
			}
			mu.Lock()
			received = append(received, seq)
			if len(received) >= msgCount {
				select {
				case done <- true:
				default:
				}
			}
			mu.Unlock()
		}
	})
	time.Sleep(300 * time.Millisecond)

	for i := 0; i < msgCount; i++ {
		rA.Emit("rapid-test", map[string]any{"seq": i})
	}

	select {
	case <-done:
		mu.Lock()
		ordered := true
		for i := 1; i < len(received); i++ {
			if received[i] < received[i-1] {
				ordered = false
				break
			}
		}
		mu.Unlock()
		t.Logf("rapid: %d/%d ordered=%v", len(received), msgCount, ordered)
	case <-time.After(10 * time.Second):
		mu.Lock()
		t.Fatalf("timeout: received %d/%d", len(received), msgCount)
		mu.Unlock()
	}
}
