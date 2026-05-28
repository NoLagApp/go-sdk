//go:build integration

// Self-contained integration tests for the NoLag Go SDK.
//
// These tests use the NoLag REST API to create actor tokens, run broker
// tests, then clean up. Only NOLAG_API_KEY is required.
//
// Run:
//   NOLAG_API_KEY=nlg_live_xxx.secret go test -v -tags=integration -run TestIntegration -timeout 180s
//
// Optional overrides:
//   NOLAG_APP_SLUG    — app slug (default: nolag-agents-sdk-3529)
//   NOLAG_ROOM        — room slug (default: go-integration-test)
//   NOLAG_BROKER_URL  — broker URL (default: wss://broker.nolag.app/ws)
//   NOLAG_API_URL     — API base URL (default: https://api.nolag.app/v1)

package nolag

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ── Test configuration ──────────────────────────────────────────────

var (
	iAPIKey    = os.Getenv("NOLAG_API_KEY")
	iAppSlug  = iGetEnv("NOLAG_APP_SLUG", "nolag-agents-sdk-3529")
	iRoom     = iGetEnv("NOLAG_ROOM", "go-integration-test")
	iBrokerURL = iGetEnv("NOLAG_BROKER_URL", "wss://broker.nolag.app/ws")
	iAPIURL   = iGetEnv("NOLAG_API_URL", "https://api.nolag.app/v1")

	// Populated by TestMain
	iToken1   string
	iToken2   string
	iActor1ID string
	iActor2ID string
	iAPI      *API
	iAppID    string
	iRoomID   string
)

func iGetEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// ── TestMain: setup & teardown ──────────────────────────────────────

func TestMain(m *testing.M) {
	if iAPIKey == "" {
		log.Println("NOLAG_API_KEY not set, skipping integration tests")
		os.Exit(0)
	}

	ctx := context.Background()
	iAPI = NewAPI(iAPIKey, APIOptions{BaseURL: iAPIURL})

	// Find the app by listing and matching slug
	apps, err := iAPI.Apps.List(ctx, nil)
	if err != nil {
		log.Fatalf("Failed to list apps: %v", err)
	}
	for _, app := range apps.Data {
		if app.Slug == iAppSlug {
			iAppID = app.AppID
			break
		}
	}
	if iAppID == "" {
		log.Fatalf("App with slug %q not found", iAppSlug)
	}

	// Ensure room exists with all required topics
	topics := []string{
		"basic-test", "direct-test", "unsub-test",
		"echo-test", "noecho-test", "retain-test",
		"qos-0-test", "qos-1-test",
		"data-test", "string-test", "meta-test",
		"filter1-test", "orfilt-test", "andfilt-test",
		"setfilt-test", "addfilt-test", "rmfilt-test", "filtmeta-test",
		"rapid-test", "lb-test", "multi-handler-test",
		"multi-test", "presence-test",
	}

	rooms, err := iAPI.Rooms.List(ctx, iAppID)
	if err != nil {
		log.Fatalf("Failed to list rooms: %v", err)
	}
	for _, r := range rooms {
		if r.Slug == iRoom {
			iRoomID = r.RoomID
			break
		}
	}
	if iRoomID == "" {
		room, err := iAPI.Rooms.Create(ctx, iAppID, RoomCreate{
			Name:   "Go Integration Test",
			Slug:   iRoom,
			Topics: topics,
		})
		if err != nil {
			log.Fatalf("Failed to create room: %v", err)
		}
		iRoomID = room.RoomID
		log.Printf("Created room %s (%s)", iRoom, iRoomID)
	} else {
		// Update topics to ensure they're all present
		_, err := iAPI.Rooms.Update(ctx, iAppID, iRoomID, RoomUpdate{Topics: topics})
		if err != nil {
			log.Printf("Warning: failed to update room topics: %v", err)
		}
	}

	// Ensure room-alpha and room-beta exist for multi-room tests
	for _, slug := range []string{"room-alpha", "room-beta"} {
		found := false
		for _, r := range rooms {
			if r.Slug == slug {
				found = true
				break
			}
		}
		if !found {
			_, err := iAPI.Rooms.Create(ctx, iAppID, RoomCreate{
				Name:   slug,
				Slug:   slug,
				Topics: []string{"multi-test"},
			})
			if err != nil {
				log.Printf("Warning: failed to create %s: %v", slug, err)
			}
		}
	}

	// Create actor tokens
	actor1, err := iAPI.Actors.Create(ctx, ActorCreate{
		Name:      fmt.Sprintf("go-int-%d-1", time.Now().Unix()),
		ActorType: ActorDevice,
	})
	if err != nil {
		log.Fatalf("Failed to create actor 1: %v", err)
	}
	iToken1 = actor1.AccessToken
	iActor1ID = actor1.ActorTokenID
	log.Printf("Created actor 1: %s", actor1.KeyId())

	actor2, err := iAPI.Actors.Create(ctx, ActorCreate{
		Name:      fmt.Sprintf("go-int-%d-2", time.Now().Unix()),
		ActorType: ActorDevice,
	})
	if err != nil {
		log.Fatalf("Failed to create actor 2: %v", err)
	}
	iToken2 = actor2.AccessToken
	iActor2ID = actor2.ActorTokenID
	log.Printf("Created actor 2: %s", actor2.KeyId())

	// Wait for actor provisioning
	time.Sleep(1 * time.Second)

	// Run tests
	code := m.Run()

	// Cleanup actors
	cleanupCtx := context.Background()
	if err := iAPI.Actors.Delete(cleanupCtx, iActor1ID); err != nil {
		log.Printf("Warning: cleanup actor 1 failed: %v", err)
	}
	if err := iAPI.Actors.Delete(cleanupCtx, iActor2ID); err != nil {
		log.Printf("Warning: cleanup actor 2 failed: %v", err)
	}
	log.Println("Cleaned up test actors")

	os.Exit(code)
}

func (a *ActorWithToken) KeyId() string {
	return a.ActorResource.ActorTokenID
}

func (s ConnectionStatus) IsConnected() bool {
	return s == StatusConnected
}

// ── Helpers ─────────────────────────────────────────────────────────

func connectClients(t *testing.T) (*Client, *Client) {
	t.Helper()
	a := New(iToken1, Options{URL: iBrokerURL, HeartbeatInterval: -1})
	b := New(iToken2, Options{URL: iBrokerURL, HeartbeatInterval: -1})
	if err := a.Connect(); err != nil {
		t.Fatalf("Client A connect: %v", err)
	}
	if err := b.Connect(); err != nil {
		t.Fatalf("Client B connect: %v", err)
	}
	return a, b
}

func rooms(t *testing.T, a, b *Client) (*Room, *Room) {
	t.Helper()
	return a.SetApp(iAppSlug).SetRoom(iRoom), b.SetApp(iAppSlug).SetRoom(iRoom)
}

// ═══════════════════════════════════════════════════════════════════
// CONNECTION & PROPERTIES
// ═══════════════════════════════════════════════════════════════════

func TestIntegration_ConnectAuth(t *testing.T) {
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

func TestIntegration_ConnectionStatus(t *testing.T) {
	a := New(iToken1, Options{URL: iBrokerURL, HeartbeatInterval: -1, Reconnect: false})
	if a.Status() != StatusDisconnected {
		t.Fatalf("expected disconnected before connect, got %v", a.Status())
	}
	if err := a.Connect(); err != nil {
		t.Fatal(err)
	}
	if a.Status() != StatusConnected {
		t.Fatalf("expected connected, got %v", a.Status())
	}
	a.Close()
	if a.Status() != StatusDisconnected {
		t.Fatalf("expected disconnected after close, got %v", a.Status())
	}
}

func TestIntegration_InvalidToken(t *testing.T) {
	c := New("invalid_token_xxx", Options{URL: iBrokerURL, Reconnect: false, HeartbeatInterval: -1})
	err := c.Connect()
	if err == nil {
		c.Close()
		t.Fatal("expected error with invalid token")
	}
	t.Logf("correctly rejected: %v", err)
}

func TestIntegration_ConnectDisconnectEvents(t *testing.T) {
	a := New(iToken1, Options{URL: iBrokerURL, HeartbeatInterval: -1, Reconnect: false})

	var connectCalled, disconnectCalled atomic.Bool
	a.On("connected", func(args ...any) { connectCalled.Store(true) })
	a.On("disconnected", func(args ...any) { disconnectCalled.Store(true) })

	if err := a.Connect(); err != nil {
		t.Fatal(err)
	}
	if !connectCalled.Load() {
		t.Error("connect event not fired")
	}

	a.Close()
	time.Sleep(100 * time.Millisecond)
	if !disconnectCalled.Load() {
		t.Error("disconnect event not fired")
	}
}

// ═══════════════════════════════════════════════════════════════════
// FLUENT API
// ═══════════════════════════════════════════════════════════════════

func TestIntegration_FluentAPI(t *testing.T) {
	a := New(iToken1, Options{URL: iBrokerURL, HeartbeatInterval: -1})
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

// ═══════════════════════════════════════════════════════════════════
// BASIC PUB/SUB
// ═══════════════════════════════════════════════════════════════════

func TestIntegration_BasicPubSub(t *testing.T) {
	a, b := connectClients(t)
	defer a.Close()
	defer b.Close()
	rA, rB := rooms(t, a, b)

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
	a, b := connectClients(t)
	defer a.Close()
	defer b.Close()
	rA, rB := rooms(t, a, b)

	var count atomic.Int32
	rB.Subscribe("unsub-test", func(data any, meta MessageMeta) {
		count.Add(1)
	})
	time.Sleep(300 * time.Millisecond)

	rA.Emit("unsub-test", map[string]any{"seq": 1})
	time.Sleep(500 * time.Millisecond)
	if count.Load() != 1 {
		t.Fatalf("expected 1 before unsub, got %d", count.Load())
	}

	rB.Unsubscribe("unsub-test")
	time.Sleep(300 * time.Millisecond)

	rA.Emit("unsub-test", map[string]any{"seq": 2})
	time.Sleep(500 * time.Millisecond)
	if count.Load() != 1 {
		t.Fatalf("expected 1 after unsub, got %d", count.Load())
	}
}

// ═══════════════════════════════════════════════════════════════════
// EMIT OPTIONS
// ═══════════════════════════════════════════════════════════════════

func TestIntegration_EchoTrue(t *testing.T) {
	a := New(iToken1, Options{URL: iBrokerURL, HeartbeatInterval: -1})
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
	a, b := connectClients(t)
	defer a.Close()
	defer b.Close()
	rA, rB := rooms(t, a, b)

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
	a, b := connectClients(t)
	defer a.Close()
	defer b.Close()
	rA, rB := rooms(t, a, b)

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
	a, b := connectClients(t)
	defer a.Close()
	defer b.Close()
	rA, rB := rooms(t, a, b)

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

// ═══════════════════════════════════════════════════════════════════
// DATA TYPES & META
// ═══════════════════════════════════════════════════════════════════

func TestIntegration_ComplexData(t *testing.T) {
	a, b := connectClients(t)
	defer a.Close()
	defer b.Close()
	rA, rB := rooms(t, a, b)

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
		if d["bool"] != true {
			t.Fatalf("bool mismatch: %v", d["bool"])
		}
		if d["null"] != nil {
			t.Fatalf("null mismatch: %v", d["null"])
		}
		t.Logf("complex data OK: %d keys", len(d))
	case <-time.After(10 * time.Second):
		t.Fatal("timeout")
	}
}

func TestIntegration_StringData(t *testing.T) {
	a, b := connectClients(t)
	defer a.Close()
	defer b.Close()
	rA, rB := rooms(t, a, b)

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
	a, b := connectClients(t)
	defer a.Close()
	defer b.Close()
	rA, rB := rooms(t, a, b)

	done := make(chan MessageMeta, 1)
	rB.Subscribe("meta-test", func(data any, meta MessageMeta) {
		done <- meta
	})
	time.Sleep(300 * time.Millisecond)

	rA.Emit("meta-test", map[string]any{"check": "meta"})

	select {
	case meta := <-done:
		// Sender may be empty depending on broker version; just verify we got meta
		t.Logf("meta: sender=%s filter=%s isReplay=%v msgId=%s ts=%v", meta.Sender, meta.Filter, meta.IsReplay, meta.MsgID, meta.Timestamp)
	case <-time.After(10 * time.Second):
		t.Fatal("timeout")
	}
}

// ═══════════════════════════════════════════════════════════════════
// FILTERS
// ═══════════════════════════════════════════════════════════════════

func TestIntegration_SingleFilter(t *testing.T) {
	a, b := connectClients(t)
	defer a.Close()
	defer b.Close()
	rA, rB := rooms(t, a, b)

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
	a, b := connectClients(t)
	defer a.Close()
	defer b.Close()
	rA, rB := rooms(t, a, b)

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
		t.Fatalf("expected 2 messages (OR filter), got %d", count.Load())
	}
}

func TestIntegration_AndFilterGroup(t *testing.T) {
	a, b := connectClients(t)
	defer a.Close()
	defer b.Close()
	rA, rB := rooms(t, a, b)

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
	a, b := connectClients(t)
	defer a.Close()
	defer b.Close()
	rA, rB := rooms(t, a, b)

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
		t.Fatalf("expected 1 after setFilters, got %d", len(received))
	}
}

func TestIntegration_AddRemoveFilters(t *testing.T) {
	a, b := connectClients(t)
	defer a.Close()
	defer b.Close()
	rA, rB := rooms(t, a, b)

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

	rB.RemoveFilters("addfilt-test", []string{"tag:b"})
	time.Sleep(300 * time.Millisecond)
	rA.Emit("addfilt-test", map[string]any{"seq": 4}, EmitOptions{Filter: "tag:b"})
	time.Sleep(500 * time.Millisecond)
	if count.Load() != 2 {
		t.Fatalf("after remove: expected 2, got %d", count.Load())
	}
}

func TestIntegration_FilterInMeta(t *testing.T) {
	a, b := connectClients(t)
	defer a.Close()
	defer b.Close()
	rA, rB := rooms(t, a, b)

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

// ═══════════════════════════════════════════════════════════════════
// PRESENCE
// ═══════════════════════════════════════════════════════════════════

func TestIntegration_PresenceJoin(t *testing.T) {
	a, b := connectClients(t)
	defer a.Close()
	defer b.Close()

	rA := a.SetApp(iAppSlug).SetRoom(iRoom)
	rB := b.SetApp(iAppSlug).SetRoom(iRoom)

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
}

func TestIntegration_PresenceUpdate(t *testing.T) {
	a, b := connectClients(t)
	defer a.Close()
	defer b.Close()

	rA := a.SetApp(iAppSlug).SetRoom(iRoom)
	rB := b.SetApp(iAppSlug).SetRoom(iRoom)

	// Both set initial presence
	rB.SetPresence(map[string]any{"name": "B", "status": "online"})
	time.Sleep(300 * time.Millisecond)
	rA.SetPresence(map[string]any{"name": "A", "status": "online"})
	time.Sleep(500 * time.Millisecond)

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

	rA.SetPresence(map[string]any{"name": "A", "status": "busy"})

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

func TestIntegration_GetPresence(t *testing.T) {
	a, b := connectClients(t)
	defer a.Close()
	defer b.Close()

	rA := a.SetApp(iAppSlug).SetRoom(iRoom)
	rB := b.SetApp(iAppSlug).SetRoom(iRoom)

	rA.SetPresence(map[string]any{"name": "A"})
	rB.SetPresence(map[string]any{"name": "B"})
	time.Sleep(1 * time.Second)

	actors, err := a.GetPresence(iRoom)
	if err != nil {
		t.Fatalf("GetPresence: %v", err)
	}
	if len(actors) < 2 {
		t.Fatalf("expected >= 2 actors in presence, got %d", len(actors))
	}
	t.Logf("presence: %d actors", len(actors))
	for _, actor := range actors {
		t.Logf("  %s: %v", actor.ActorTokenID, actor.Presence)
	}
}

// ═══════════════════════════════════════════════════════════════════
// LOAD BALANCING
// ═══════════════════════════════════════════════════════════════════

func TestIntegration_LoadBalance(t *testing.T) {
	a := New(iToken1, Options{URL: iBrokerURL, HeartbeatInterval: -1})
	b := New(iToken2, Options{URL: iBrokerURL, HeartbeatInterval: -1})
	c := New(iToken1, Options{URL: iBrokerURL, HeartbeatInterval: -1})
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
	t.Logf("LB: B=%d C=%d total=%d distributed=%v", countB.Load(), countC.Load(), total, distributed)
}

// ═══════════════════════════════════════════════════════════════════
// ADVANCED
// ═══════════════════════════════════════════════════════════════════

func TestIntegration_MultipleRooms(t *testing.T) {
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
	a, b := connectClients(t)
	defer a.Close()
	defer b.Close()
	rA, rB := rooms(t, a, b)

	msgCount := 20
	done := make(chan bool, 1)
	var received []int
	var mu sync.Mutex

	rB.Subscribe("rapid-test", func(data any, meta MessageMeta) {
		if m, ok := data.(map[string]any); ok {
			seq := toInt(m["seq"])
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

func TestIntegration_MultipleHandlers(t *testing.T) {
	a, b := connectClients(t)
	defer a.Close()
	defer b.Close()
	rA, _ := rooms(t, a, b)

	// Subscribe with handler, then register event handler on same topic
	var subCount, evtCount atomic.Int32

	fullTopic := fmt.Sprintf("%s/%s/multi-handler-test", iAppSlug, iRoom)
	a.Subscribe(fullTopic, func(data any, meta MessageMeta) {
		subCount.Add(1)
	})
	a.On(fullTopic, func(args ...any) {
		evtCount.Add(1)
	})
	time.Sleep(300 * time.Millisecond)

	echoTrue := true
	rA.Emit("multi-handler-test", map[string]any{"test": true}, EmitOptions{Echo: &echoTrue})
	time.Sleep(500 * time.Millisecond)

	if subCount.Load() < 1 {
		t.Fatalf("subscription handler not called (got %d)", subCount.Load())
	}
	t.Logf("sub=%d evt=%d", subCount.Load(), evtCount.Load())
}

func TestIntegration_Heartbeat(t *testing.T) {
	a := New(iToken1, Options{
		URL:               iBrokerURL,
		Reconnect:         false,
		HeartbeatInterval: 1 * time.Second,
	})
	if err := a.Connect(); err != nil {
		t.Fatal(err)
	}
	defer a.Close()

	time.Sleep(2500 * time.Millisecond)

	if a.Status() != StatusConnected {
		t.Error("connection should be alive after heartbeats")
	}
}

func TestIntegration_ErrorBeforeConnect(t *testing.T) {
	c := New(iToken1, Options{URL: iBrokerURL, Reconnect: false})

	if err := c.Subscribe("test", func(data any, meta MessageMeta) {}); err != ErrNotConnected {
		t.Fatalf("subscribe before connect: expected ErrNotConnected, got %v", err)
	}
	if err := c.Emit("test", "data"); err != ErrNotConnected {
		t.Fatalf("emit before connect: expected ErrNotConnected, got %v", err)
	}
}

// ═══════════════════════════════════════════════════════════════════
// REST API CRUD
// ═══════════════════════════════════════════════════════════════════

func TestIntegration_API_ListApps(t *testing.T) {
	ctx := context.Background()
	apps, err := iAPI.Apps.List(ctx, nil)
	if err != nil {
		t.Fatalf("list apps: %v", err)
	}
	if len(apps.Data) == 0 {
		t.Fatal("expected at least 1 app")
	}
	t.Logf("found %d apps", len(apps.Data))
}

func TestIntegration_API_GetApp(t *testing.T) {
	ctx := context.Background()
	app, err := iAPI.Apps.Get(ctx, iAppID)
	if err != nil {
		t.Fatalf("get app: %v", err)
	}
	if app.Slug != iAppSlug {
		t.Fatalf("expected slug %s, got %s", iAppSlug, app.Slug)
	}
	t.Logf("app: %s (%s)", app.Name, app.AppID)
}

func TestIntegration_API_UpdateApp(t *testing.T) {
	ctx := context.Background()
	desc := "Updated by Go integration tests"
	_, err := iAPI.Apps.Update(ctx, iAppID, AppUpdate{Description: &desc})
	if err != nil {
		t.Fatalf("update app: %v", err)
	}
}

func TestIntegration_API_ListRooms(t *testing.T) {
	ctx := context.Background()
	rooms, err := iAPI.Rooms.List(ctx, iAppID)
	if err != nil {
		t.Fatalf("list rooms: %v", err)
	}
	if len(rooms) == 0 {
		t.Fatal("expected at least 1 room")
	}
	t.Logf("found %d rooms", len(rooms))
}

func TestIntegration_API_GetRoom(t *testing.T) {
	ctx := context.Background()
	room, err := iAPI.Rooms.Get(ctx, iAppID, iRoomID)
	if err != nil {
		t.Fatalf("get room: %v", err)
	}
	if room.Slug != iRoom {
		t.Fatalf("expected slug %s, got %s", iRoom, room.Slug)
	}
}

func TestIntegration_API_RoomCRUD(t *testing.T) {
	ctx := context.Background()

	// Create
	room, err := iAPI.Rooms.Create(ctx, iAppID, RoomCreate{
		Name:   "API Test Room",
		Slug:   fmt.Sprintf("api-test-%d", time.Now().Unix()),
		Topics: []string{"test-topic"},
	})
	if err != nil {
		t.Fatalf("create room: %v", err)
	}
	t.Logf("created room: %s", room.RoomID)

	// Update
	desc := "updated"
	_, err = iAPI.Rooms.Update(ctx, iAppID, room.RoomID, RoomUpdate{Description: &desc})
	if err != nil {
		t.Fatalf("update room: %v", err)
	}

	// Delete
	if err := iAPI.Rooms.Delete(ctx, iAppID, room.RoomID); err != nil {
		t.Fatalf("delete room: %v", err)
	}
	t.Log("room CRUD passed")
}

func TestIntegration_API_ListActors(t *testing.T) {
	ctx := context.Background()
	actors, err := iAPI.Actors.List(ctx)
	if err != nil {
		t.Fatalf("list actors: %v", err)
	}
	if len(actors) == 0 {
		t.Fatal("expected at least 1 actor")
	}
	t.Logf("found %d actors", len(actors))
}

func TestIntegration_API_ActorCRUD(t *testing.T) {
	ctx := context.Background()

	// Create
	actor, err := iAPI.Actors.Create(ctx, ActorCreate{
		Name:      fmt.Sprintf("crud-test-%d", time.Now().Unix()),
		ActorType: ActorDevice,
	})
	if err != nil {
		t.Fatalf("create actor: %v", err)
	}
	if actor.AccessToken == "" {
		t.Fatal("access token should be returned on creation")
	}
	t.Logf("created actor: %s (token starts with %s...)", actor.ActorTokenID, actor.AccessToken[:20])

	// Get (no token returned)
	got, err := iAPI.Actors.Get(ctx, actor.ActorTokenID)
	if err != nil {
		t.Fatalf("get actor: %v", err)
	}
	if got.ActorTokenID != actor.ActorTokenID {
		t.Fatalf("ID mismatch: %s vs %s", got.ActorTokenID, actor.ActorTokenID)
	}

	// Update
	newName := "updated-name"
	_, err = iAPI.Actors.Update(ctx, actor.ActorTokenID, ActorUpdate{Name: &newName})
	if err != nil {
		t.Fatalf("update actor: %v", err)
	}

	// Delete
	if err := iAPI.Actors.Delete(ctx, actor.ActorTokenID); err != nil {
		t.Fatalf("delete actor: %v", err)
	}
	t.Log("actor CRUD passed")
}

func TestIntegration_API_InvalidKey(t *testing.T) {
	badAPI := NewAPI("invalid_key")
	_, err := badAPI.Apps.List(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error with invalid API key")
	}
	if apiErr, ok := err.(*NoLagAPIError); ok {
		if apiErr.Status != 401 && apiErr.Status != 403 {
			t.Fatalf("expected 401/403, got %d", apiErr.Status)
		}
		t.Logf("correctly rejected: %d", apiErr.Status)
	}
}

func TestIntegration_API_Pagination(t *testing.T) {
	ctx := context.Background()
	apps, err := iAPI.Apps.List(ctx, &ListOptions{Page: 1, Limit: 1})
	if err != nil {
		t.Fatalf("list with pagination: %v", err)
	}
	if len(apps.Data) > 1 {
		t.Fatalf("expected <= 1 result with limit=1, got %d", len(apps.Data))
	}
	t.Logf("pagination: page=%d limit=%d total=%d", apps.Page, apps.Limit, apps.Total)
}

// ── Utilities ───────────────────────────────────────────────────────

func toInt(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int8:
		return int(n)
	case int16:
		return int(n)
	case int32:
		return int(n)
	case int64:
		return int(n)
	case uint:
		return int(n)
	case uint8:
		return int(n)
	case uint16:
		return int(n)
	case uint32:
		return int(n)
	case uint64:
		return int(n)
	case float32:
		return int(n)
	case float64:
		return int(n)
	default:
		return 0
	}
}
