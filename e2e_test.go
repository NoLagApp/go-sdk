// E2E Tests for NoLag Go SDK with Kraken-v2
//
// Prerequisites:
// 1. Start kraken-v2: cd kraken-v2 && docker-compose up -d
// 2. Start Titus: cd titus && npm run dev
// 3. Set environment variables:
//    - NOLAG_TEST_TOKEN: Valid access token from Titus
//    - NOLAG_TEST_TOKEN_2: Second valid access token (for multi-client tests)
//    - NOLAG_TEST_URL: WebSocket URL (default: ws://localhost:8080/ws)
//
// Run tests:
//   NOLAG_TEST_TOKEN=at_xxx go test -v -tags=e2e

//go:build e2e

package nolag

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var (
	testURL    = getEnvOrDefault("NOLAG_TEST_URL", "ws://localhost:8080/ws")
	testToken  = os.Getenv("NOLAG_TEST_TOKEN")
	testToken2 = os.Getenv("NOLAG_TEST_TOKEN_2")
)

func getEnvOrDefault(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func skipIfNoToken(t *testing.T) {
	if testToken == "" {
		t.Skip("NOLAG_TEST_TOKEN not set")
	}
}

func skipIfNoSecondToken(t *testing.T) {
	if testToken == "" || testToken2 == "" {
		t.Skip("NOLAG_TEST_TOKEN or NOLAG_TEST_TOKEN_2 not set")
	}
}

func waitFor(condition func() bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

// Connection Tests

func TestConnectAndAuthenticate(t *testing.T) {
	skipIfNoToken(t)

	client := New(testToken, Options{
		URL:               testURL,
		Debug:             true,
		Reconnect:         false,
		HeartbeatInterval: 0,
	})

	err := client.Connect()
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer client.Close()

	if client.Status() != StatusConnected {
		t.Errorf("Expected status connected, got %v", client.Status())
	}
}

func TestFailWithInvalidToken(t *testing.T) {
	client := New("invalid_token", Options{
		URL:       testURL,
		Reconnect: false,
	})

	err := client.Connect()
	if err == nil {
		client.Close()
		t.Fatal("Expected error with invalid token")
	}

	if client.Status() != StatusDisconnected {
		t.Errorf("Expected status disconnected, got %v", client.Status())
	}
}

func TestDisconnectCleanly(t *testing.T) {
	skipIfNoToken(t)

	client := New(testToken, Options{
		URL:               testURL,
		Debug:             true,
		Reconnect:         false,
		HeartbeatInterval: 0,
	})

	err := client.Connect()
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	if client.Status() != StatusConnected {
		t.Errorf("Expected connected, got %v", client.Status())
	}

	client.Close()

	if client.Status() != StatusDisconnected {
		t.Errorf("Expected disconnected, got %v", client.Status())
	}
}

func TestEmitConnectEvent(t *testing.T) {
	skipIfNoToken(t)

	client := New(testToken, Options{
		URL:               testURL,
		Debug:             true,
		Reconnect:         false,
		HeartbeatInterval: 0,
	})

	var connectCalled atomic.Bool

	client.On("connected", func(args ...any) {
		connectCalled.Store(true)
	})

	err := client.Connect()
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer client.Close()

	if !connectCalled.Load() {
		t.Error("Connect event was not emitted")
	}
}

func TestEmitDisconnectEvent(t *testing.T) {
	skipIfNoToken(t)

	client := New(testToken, Options{
		URL:               testURL,
		Debug:             true,
		Reconnect:         false,
		HeartbeatInterval: 0,
	})

	var disconnectCalled atomic.Bool

	client.On("disconnected", func(args ...any) {
		disconnectCalled.Store(true)
	})

	err := client.Connect()
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	client.Close()

	time.Sleep(100 * time.Millisecond)

	if !disconnectCalled.Load() {
		t.Error("Disconnect event was not emitted")
	}
}

// Subscribe/Unsubscribe Tests

func TestSubscribeToTopic(t *testing.T) {
	skipIfNoToken(t)

	client := New(testToken, Options{
		URL:               testURL,
		Debug:             true,
		Reconnect:         false,
		HeartbeatInterval: 0,
	})

	err := client.Connect()
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer client.Close()

	err = client.Subscribe("test/room/messages", func(data any, meta MessageMeta) {})
	if err != nil {
		t.Errorf("Subscribe failed: %v", err)
	}
}

func TestUnsubscribeFromTopic(t *testing.T) {
	skipIfNoToken(t)

	client := New(testToken, Options{
		URL:               testURL,
		Debug:             true,
		Reconnect:         false,
		HeartbeatInterval: 0,
	})

	err := client.Connect()
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer client.Close()

	err = client.Subscribe("test/room/messages", func(data any, meta MessageMeta) {})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	err = client.Unsubscribe("test/room/messages")
	if err != nil {
		t.Errorf("Unsubscribe failed: %v", err)
	}
}

// Publish/Subscribe Messaging Tests

func TestReceiveOwnPublishedMessage(t *testing.T) {
	skipIfNoToken(t)

	client := New(testToken, Options{
		URL:               testURL,
		Debug:             true,
		Reconnect:         false,
		HeartbeatInterval: 0,
	})

	err := client.Connect()
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer client.Close()

	topic := fmt.Sprintf("test/e2e/%d", time.Now().UnixMilli())
	testData := map[string]any{"message": "hello", "timestamp": time.Now().Unix()}

	var receivedData any
	var mu sync.Mutex

	err = client.Subscribe(topic, func(data any, meta MessageMeta) {
		mu.Lock()
		receivedData = data
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	err = client.Emit(topic, testData)
	if err != nil {
		t.Fatalf("Emit failed: %v", err)
	}

	success := waitFor(func() bool {
		mu.Lock()
		defer mu.Unlock()
		return receivedData != nil
	}, 5*time.Second)

	if !success {
		t.Fatal("Timeout waiting for message")
	}
}

// Fluent API Tests

func TestSetAppSetRoomPattern(t *testing.T) {
	skipIfNoToken(t)

	client := New(testToken, Options{
		URL:               testURL,
		Debug:             true,
		Reconnect:         false,
		HeartbeatInterval: 0,
	})

	err := client.Connect()
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer client.Close()

	room := client.SetApp("myapp").SetRoom("lobby")
	testData := map[string]any{"user": "alice", "message": "hi"}

	var receivedData any
	var mu sync.Mutex

	err = room.Subscribe("chat", func(data any, meta MessageMeta) {
		mu.Lock()
		receivedData = data
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	err = room.Emit("chat", testData)
	if err != nil {
		t.Fatalf("Emit failed: %v", err)
	}

	success := waitFor(func() bool {
		mu.Lock()
		defer mu.Unlock()
		return receivedData != nil
	}, 5*time.Second)

	if !success {
		t.Fatal("Timeout waiting for message")
	}
}

func TestCorrectPrefix(t *testing.T) {
	client := New("token", Options{
		URL: testURL,
	})

	room := client.SetApp("testapp").SetRoom("testroom")

	if room.Prefix() != "testapp/testroom" {
		t.Errorf("Expected prefix testapp/testroom, got %s", room.Prefix())
	}
}

// Load Balancing Tests

func TestSubscribeWithLoadBalancing(t *testing.T) {
	skipIfNoToken(t)

	client := New(testToken, Options{
		URL:               testURL,
		Debug:             true,
		Reconnect:         false,
		HeartbeatInterval: 0,
		LoadBalance:       true,
		LoadBalanceGroup:  "test-workers",
	})

	err := client.Connect()
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer client.Close()

	err = client.Subscribe("jobs/process", func(data any, meta MessageMeta) {})
	if err != nil {
		t.Errorf("Subscribe with load balancing failed: %v", err)
	}
}

func TestOverrideLoadBalancePerSubscription(t *testing.T) {
	skipIfNoToken(t)

	client := New(testToken, Options{
		URL:               testURL,
		Debug:             true,
		Reconnect:         false,
		HeartbeatInterval: 0,
		LoadBalance:       false,
	})

	err := client.Connect()
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer client.Close()

	lb := true
	err = client.Subscribe("jobs/process", func(data any, meta MessageMeta) {}, SubscribeOptions{
		LoadBalance:      &lb,
		LoadBalanceGroup: "workers",
	})
	if err != nil {
		t.Errorf("Subscribe with per-topic load balancing failed: %v", err)
	}
}

func TestLoadBalanceDistribution(t *testing.T) {
	skipIfNoSecondToken(t)

	// Create 2 clients in the same load balance group
	client1 := New(testToken, Options{
		URL:               testURL,
		Debug:             true,
		Reconnect:         false,
		HeartbeatInterval: 0,
	})

	client2 := New(testToken, Options{
		URL:               testURL,
		Debug:             true,
		Reconnect:         false,
		HeartbeatInterval: 0,
	})

	// Publisher client (not in load balance group)
	publisher := New(testToken2, Options{
		URL:               testURL,
		Debug:             true,
		Reconnect:         false,
		HeartbeatInterval: 0,
	})

	// Connect all clients
	if err := client1.Connect(); err != nil {
		t.Fatalf("Client1 connect failed: %v", err)
	}
	defer client1.Close()

	if err := client2.Connect(); err != nil {
		t.Fatalf("Client2 connect failed: %v", err)
	}
	defer client2.Close()

	if err := publisher.Connect(); err != nil {
		t.Fatalf("Publisher connect failed: %v", err)
	}
	defer publisher.Close()

	topic := fmt.Sprintf("test/loadbalance/%d", time.Now().UnixMilli())

	var client1Count, client2Count atomic.Int32

	lb := true
	err := client1.Subscribe(topic, func(data any, meta MessageMeta) {
		client1Count.Add(1)
	}, SubscribeOptions{LoadBalance: &lb, LoadBalanceGroup: "lb-test"})
	if err != nil {
		t.Fatalf("Client1 subscribe failed: %v", err)
	}

	err = client2.Subscribe(topic, func(data any, meta MessageMeta) {
		client2Count.Add(1)
	}, SubscribeOptions{LoadBalance: &lb, LoadBalanceGroup: "lb-test"})
	if err != nil {
		t.Fatalf("Client2 subscribe failed: %v", err)
	}

	time.Sleep(300 * time.Millisecond)

	// Publish multiple messages
	for i := 0; i < 6; i++ {
		err := publisher.Emit(topic, map[string]any{"id": i})
		if err != nil {
			t.Fatalf("Emit failed: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Wait for messages to be distributed
	success := waitFor(func() bool {
		return client1Count.Load()+client2Count.Load() >= 6
	}, 5*time.Second)

	if !success {
		t.Fatalf("Timeout waiting for messages. Got client1=%d, client2=%d",
			client1Count.Load(), client2Count.Load())
	}

	// Both clients should have received some messages (round-robin)
	total := client1Count.Load() + client2Count.Load()
	if total != 6 {
		t.Errorf("Expected 6 total messages, got %d", total)
	}

	// Each client should have received approximately half
	if client1Count.Load() < 2 {
		t.Errorf("Client 1 should receive at least 2 messages, got %d", client1Count.Load())
	}
	if client2Count.Load() < 2 {
		t.Errorf("Client 2 should receive at least 2 messages, got %d", client2Count.Load())
	}
}

// Echo Tests

func TestReceiveOwnMessageEchoTrue(t *testing.T) {
	skipIfNoToken(t)

	client := New(testToken, Options{
		URL:               testURL,
		Debug:             true,
		Reconnect:         false,
		HeartbeatInterval: 0,
	})

	err := client.Connect()
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer client.Close()

	topic := fmt.Sprintf("test/echo/enabled/%d", time.Now().UnixMilli())
	testData := map[string]any{"message": "echo me"}

	var receivedData any
	var mu sync.Mutex

	err = client.Subscribe(topic, func(data any, meta MessageMeta) {
		mu.Lock()
		receivedData = data
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// Emit with echo=true (default)
	err = client.Emit(topic, testData)
	if err != nil {
		t.Fatalf("Emit failed: %v", err)
	}

	success := waitFor(func() bool {
		mu.Lock()
		defer mu.Unlock()
		return receivedData != nil
	}, 5*time.Second)

	if !success {
		t.Fatal("Timeout waiting for message")
	}
}

func TestNotReceiveOwnMessageEchoFalse(t *testing.T) {
	skipIfNoToken(t)

	client := New(testToken, Options{
		URL:               testURL,
		Debug:             true,
		Reconnect:         false,
		HeartbeatInterval: 0,
	})

	err := client.Connect()
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer client.Close()

	topic := fmt.Sprintf("test/echo/disabled/%d", time.Now().UnixMilli())
	testData := map[string]any{"message": "no echo"}

	var receivedData any
	var mu sync.Mutex

	err = client.Subscribe(topic, func(data any, meta MessageMeta) {
		mu.Lock()
		receivedData = data
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// Emit with echo=false
	echoFalse := false
	err = client.Emit(topic, testData, EmitOptions{Echo: &echoFalse})
	if err != nil {
		t.Fatalf("Emit failed: %v", err)
	}

	// Wait a bit to ensure message had time to arrive if it was going to
	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if receivedData != nil {
		t.Error("Should NOT have received the message when echo=false")
	}
}

// Multi-Client Tests

func TestDeliverMessagesBetweenClients(t *testing.T) {
	skipIfNoSecondToken(t)

	client1 := New(testToken, Options{
		URL:               testURL,
		Debug:             true,
		Reconnect:         false,
		HeartbeatInterval: 0,
	})

	client2 := New(testToken2, Options{
		URL:               testURL,
		Debug:             true,
		Reconnect:         false,
		HeartbeatInterval: 0,
	})

	if err := client1.Connect(); err != nil {
		t.Fatalf("Client1 connect failed: %v", err)
	}
	defer client1.Close()

	if err := client2.Connect(); err != nil {
		t.Fatalf("Client2 connect failed: %v", err)
	}
	defer client2.Close()

	topic := fmt.Sprintf("test/multiclient/%d", time.Now().UnixMilli())
	testData := map[string]any{"from": "client1", "value": 123}

	var client2Received any
	var mu sync.Mutex

	// Client 2 subscribes
	err := client2.Subscribe(topic, func(data any, meta MessageMeta) {
		mu.Lock()
		client2Received = data
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("Client2 subscribe failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// Client 1 publishes
	err = client1.Emit(topic, testData)
	if err != nil {
		t.Fatalf("Client1 emit failed: %v", err)
	}

	success := waitFor(func() bool {
		mu.Lock()
		defer mu.Unlock()
		return client2Received != nil
	}, 5*time.Second)

	if !success {
		t.Fatal("Timeout waiting for message")
	}
}

func TestBidirectionalMessaging(t *testing.T) {
	skipIfNoSecondToken(t)

	client1 := New(testToken, Options{
		URL:               testURL,
		Debug:             true,
		Reconnect:         false,
		HeartbeatInterval: 0,
	})

	client2 := New(testToken2, Options{
		URL:               testURL,
		Debug:             true,
		Reconnect:         false,
		HeartbeatInterval: 0,
	})

	if err := client1.Connect(); err != nil {
		t.Fatalf("Client1 connect failed: %v", err)
	}
	defer client1.Close()

	if err := client2.Connect(); err != nil {
		t.Fatalf("Client2 connect failed: %v", err)
	}
	defer client2.Close()

	topic := fmt.Sprintf("test/bidirectional/%d", time.Now().UnixMilli())

	var client1Count, client2Count atomic.Int32

	// Both subscribe
	err := client1.Subscribe(topic, func(data any, meta MessageMeta) {
		client1Count.Add(1)
	})
	if err != nil {
		t.Fatalf("Client1 subscribe failed: %v", err)
	}

	err = client2.Subscribe(topic, func(data any, meta MessageMeta) {
		client2Count.Add(1)
	})
	if err != nil {
		t.Fatalf("Client2 subscribe failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// Both publish
	err = client1.Emit(topic, map[string]any{"from": "client1"})
	if err != nil {
		t.Fatalf("Client1 emit failed: %v", err)
	}

	err = client2.Emit(topic, map[string]any{"from": "client2"})
	if err != nil {
		t.Fatalf("Client2 emit failed: %v", err)
	}

	success := waitFor(func() bool {
		return client1Count.Load() >= 2 && client2Count.Load() >= 2
	}, 5*time.Second)

	if !success {
		t.Fatalf("Timeout waiting for messages. Got client1=%d, client2=%d",
			client1Count.Load(), client2Count.Load())
	}
}

// Heartbeat Tests

func TestHeartbeatKeepsConnection(t *testing.T) {
	skipIfNoToken(t)

	client := New(testToken, Options{
		URL:               testURL,
		Debug:             true,
		Reconnect:         false,
		HeartbeatInterval: 1 * time.Second, // 1 second for testing
	})

	err := client.Connect()
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer client.Close()

	if client.Status() != StatusConnected {
		t.Errorf("Expected connected, got %v", client.Status())
	}

	// Wait for at least 2 heartbeats
	time.Sleep(2500 * time.Millisecond)

	// If we're still connected, heartbeats are working
	if client.Status() != StatusConnected {
		t.Error("Connection should still be alive after heartbeats")
	}
}

// Error Handling Tests

func TestHandleSubscribeBeforeConnect(t *testing.T) {
	client := New(testToken, Options{
		URL:       testURL,
		Reconnect: false,
	})

	err := client.Subscribe("test/topic", func(data any, meta MessageMeta) {})
	if err == nil {
		t.Error("Expected error when subscribing before connect")
	}

	if err != ErrNotConnected {
		t.Errorf("Expected ErrNotConnected, got %v", err)
	}
}

func TestHandleEmitBeforeConnect(t *testing.T) {
	client := New(testToken, Options{
		URL:       testURL,
		Reconnect: false,
	})

	err := client.Emit("test/topic", map[string]any{"data": "test"})
	if err == nil {
		t.Error("Expected error when emitting before connect")
	}

	if err != ErrNotConnected {
		t.Errorf("Expected ErrNotConnected, got %v", err)
	}
}
