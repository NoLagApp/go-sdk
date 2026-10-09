package nolag

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vmihailenco/msgpack/v5"
)

// fakeBroker is an in-process WebSocket server that speaks enough of the
// kraken protocol to exercise reconnects: it answers auth (with a
// restoredSubscriptions list chosen per connection) and lobbySubscribe,
// records every frame, and can drop or write to a connection.
type fakeBroker struct {
	t   *testing.T
	srv *httptest.Server

	mu    sync.Mutex
	conns []*fakeConn
	// restored returns the auth reply's restoredSubscriptions for the
	// connection with the given index (0 is the first).
	restored func(conn int) []any
}

type fakeConn struct {
	ws      *websocket.Conn
	writeMu sync.Mutex

	mu     sync.Mutex
	frames []map[string]any
}

func newFakeBroker(t *testing.T) *fakeBroker {
	b := &fakeBroker{t: t}
	upgrader := websocket.Upgrader{}
	b.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		fc := &fakeConn{ws: ws}
		b.mu.Lock()
		idx := len(b.conns)
		b.conns = append(b.conns, fc)
		b.mu.Unlock()
		go b.serve(idx, fc)
	}))
	t.Cleanup(func() {
		b.mu.Lock()
		for _, fc := range b.conns {
			fc.ws.Close()
		}
		b.mu.Unlock()
		b.srv.Close()
	})
	return b
}

func (b *fakeBroker) url() string {
	return "ws" + strings.TrimPrefix(b.srv.URL, "http")
}

func (b *fakeBroker) serve(idx int, fc *fakeConn) {
	for {
		_, data, err := fc.ws.ReadMessage()
		if err != nil {
			return
		}
		if len(data) == 0 {
			continue // heartbeat
		}
		var frame map[string]any
		if err := msgpack.Unmarshal(data, &frame); err != nil {
			b.t.Errorf("fake broker: bad frame: %v", err)
			return
		}
		fc.mu.Lock()
		fc.frames = append(fc.frames, frame)
		fc.mu.Unlock()

		switch frame["type"] {
		case "auth":
			var restored []any
			b.mu.Lock()
			if b.restored != nil {
				restored = b.restored(idx)
			}
			b.mu.Unlock()
			fc.write(b.t, map[string]any{
				"type":                  "auth",
				"success":               true,
				"actorTokenId":          "alice",
				"protocolVersion":       2,
				"restoredSubscriptions": restored,
			})
		case "lobbySubscribe":
			fc.write(b.t, map[string]any{
				"type":     "lobbySubscribed",
				"lobbyId":  frame["lobbyId"],
				"presence": map[string]any{},
			})
		case "getLobbyPresence":
			fc.write(b.t, map[string]any{
				"type":     "lobbyPresenceList",
				"lobbyId":  frame["lobbyId"],
				"presence": map[string]any{},
			})
		case "getPresence":
			fc.write(b.t, map[string]any{
				"type": "presenceList",
				"data": []any{},
			})
		}
	}
}

func (fc *fakeConn) write(t *testing.T, frame map[string]any) {
	data, err := msgpack.Marshal(frame)
	if err != nil {
		t.Errorf("fake broker: marshal: %v", err)
		return
	}
	fc.writeMu.Lock()
	defer fc.writeMu.Unlock()
	_ = fc.ws.WriteMessage(websocket.BinaryMessage, data)
}

func (b *fakeBroker) setRestored(f func(conn int) []any) {
	b.mu.Lock()
	b.restored = f
	b.mu.Unlock()
}

func (b *fakeBroker) conn(idx int) *fakeConn {
	b.mu.Lock()
	defer b.mu.Unlock()
	if idx >= len(b.conns) {
		return nil
	}
	return b.conns[idx]
}

func (b *fakeBroker) connCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.conns)
}

// drop closes connection idx from the server side, as a restarted proxy or
// broker would.
func (b *fakeBroker) drop(idx int) {
	b.conn(idx).ws.Close()
}

// framesOf returns a copy of the frames received on connection idx.
func (b *fakeBroker) framesOf(idx int) []map[string]any {
	fc := b.conn(idx)
	if fc == nil {
		return nil
	}
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return append([]map[string]any(nil), fc.frames...)
}

// framesOfType returns the frames of one type received on connection idx.
func (b *fakeBroker) framesOfType(idx int, typ string) []map[string]any {
	var out []map[string]any
	for _, f := range b.framesOf(idx) {
		if f["type"] == typ {
			out = append(out, f)
		}
	}
	return out
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// newFakeClient connects a client to b with fast reconnects and no
// heartbeat, and closes it when the test ends.
func newFakeClient(t *testing.T, b *fakeBroker) *Client {
	t.Helper()
	c := New("dev-token-alice", Options{
		URL:                  b.url(),
		ReconnectInterval:    20 * time.Millisecond,
		MaxReconnectAttempts: -1,
		HeartbeatInterval:    -1,
	})
	if err := c.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

var flushSeq atomic.Int64

// flush waits until the broker has read every frame the client sent so far:
// a request-reply on the same connection is answered only after them.
func flush(t *testing.T, c *Client) {
	t.Helper()
	lobby := c.SetApp("demo").SetLobby(fmt.Sprintf("flush-%d", flushSeq.Add(1)))
	if _, err := lobby.FetchPresence(); err != nil {
		t.Fatalf("flush: %v", err)
	}
}

// reconnected drops connection idx, waits until the client is connected
// again on connection idx+1, and flushes.
func reconnected(t *testing.T, b *fakeBroker, c *Client, idx int) {
	t.Helper()
	b.drop(idx)
	eventually(t, "a new connection", func() bool { return b.connCount() > idx+1 })
	eventually(t, "the client to reconnect", func() bool { return c.Status() == StatusConnected })
	flush(t, c)
}

func noop(any, MessageMeta) {}

// filterStrings turns a decoded filter list into comparable strings.
func filterStrings(v any) []string {
	list, _ := v.([]any)
	out := make([]string, 0, len(list))
	for _, item := range list {
		out = append(out, fmt.Sprint(item))
	}
	return out
}

func TestSessionTracksSubscriptionsAndCurrentFilters(t *testing.T) {
	b := newFakeBroker(t)
	c := newFakeClient(t, b)

	lb := true
	if err := c.Subscribe("demo/general/a", noop, SubscribeOptions{
		QoS:              QoSLevel(QoSAtMostOnce),
		LoadBalance:      &lb,
		LoadBalanceGroup: "workers",
		Filters:          []any{"x"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.Subscribe("demo/general/b", noop); err != nil {
		t.Fatal(err)
	}

	get := func(topic string) (subscription, bool) {
		c.mu.RLock()
		defer c.mu.RUnlock()
		s, ok := c.subscriptions[topic]
		return s, ok
	}

	a, ok := get("demo/general/a")
	if !ok {
		t.Fatal("subscription a not tracked")
	}
	if a.qos != QoSAtMostOnce || !a.loadBalance || a.loadBalanceGroup != "workers" {
		t.Fatalf("options not tracked: %+v", a)
	}
	if !reflect.DeepEqual(a.filters, []any{"x"}) {
		t.Fatalf("filters: %#v", a.filters)
	}

	if err := c.SetFilters("demo/general/a", []any{"y", []string{"A", "b"}}); err != nil {
		t.Fatal(err)
	}
	a, _ = get("demo/general/a")
	if filterKey(a.filters) != filterKey([]any{"y", []string{"a", "b"}}) {
		t.Fatalf("SetFilters not tracked: %#v", a.filters)
	}
	if a.qos != QoSAtMostOnce || !a.loadBalance || a.loadBalanceGroup != "workers" {
		t.Fatalf("SetFilters lost the options: %+v", a)
	}

	if err := c.AddFilters("demo/general/a", []string{"z"}); err != nil {
		t.Fatal(err)
	}
	if err := c.RemoveFilters("demo/general/a", []string{"y"}); err != nil {
		t.Fatal(err)
	}
	a, _ = get("demo/general/a")
	if filterKey(a.filters) != filterKey([]any{"z", []string{"a", "b"}}) {
		t.Fatalf("Add/RemoveFilters not tracked: %#v", a.filters)
	}

	if err := c.SetFilters("demo/general/a", nil); err != nil {
		t.Fatal(err)
	}
	if a, _ = get("demo/general/a"); a.filters != nil {
		t.Fatalf("clearing filters not tracked: %#v", a.filters)
	}

	if err := c.Unsubscribe("demo/general/b"); err != nil {
		t.Fatal(err)
	}
	if _, ok := get("demo/general/b"); ok {
		t.Fatal("Unsubscribe did not forget the topic")
	}
}

// The caller's slices must not alias the session: changing them after the
// call must not change what a reconnect sends.
func TestSessionCopiesCallerFilters(t *testing.T) {
	b := newFakeBroker(t)
	c := newFakeClient(t, b)

	group := []string{"a", "b"}
	filters := []any{"x", group}
	if err := c.Subscribe("demo/general/a", noop, SubscribeOptions{Filters: filters}); err != nil {
		t.Fatal(err)
	}
	filters[0] = "changed"
	group[0] = "changed"

	reconnected(t, b, c, 0)
	subs := b.framesOfType(1, "subscribe")
	if len(subs) != 1 {
		t.Fatalf("want 1 resubscribe, got %d", len(subs))
	}
	got := subs[0]["filters"].([]any)
	if got[0] != "x" || fmt.Sprint(got[1]) != "[a b]" {
		t.Fatalf("filters changed with the caller's slice: %#v", got)
	}
}

// After a broker restart nothing is restored, so every subscription must be
// sent again with the options it was made with and its current filters.
// v0.5.0 resubscribed with no options at all.
func TestReconnectResubscribesWithOptionsAndCurrentFilters(t *testing.T) {
	b := newFakeBroker(t)
	c := newFakeClient(t, b)

	lb := true
	if err := c.Subscribe("demo/general/a", noop, SubscribeOptions{
		QoS:              QoSLevel(QoSExactlyOnce),
		LoadBalance:      &lb,
		LoadBalanceGroup: "workers",
		Filters:          []any{"old"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.SetFilters("demo/general/a", []any{"x", []string{"p", "q"}}); err != nil {
		t.Fatal(err)
	}
	if err := c.Subscribe("demo/general/b", noop, SubscribeOptions{QoS: QoSLevel(QoSAtMostOnce)}); err != nil {
		t.Fatal(err)
	}

	reconnected(t, b, c, 0)

	auth := b.framesOfType(1, "auth")
	if len(auth) != 1 || auth[0]["reconnect"] != true {
		t.Fatalf("reconnect auth should carry reconnect=true: %#v", auth)
	}

	subs := b.framesOfType(1, "subscribe")
	if len(subs) != 2 {
		t.Fatalf("want 2 resubscribes, got %d: %#v", len(subs), subs)
	}
	byTopic := map[string]map[string]any{}
	for _, f := range subs {
		byTopic[f["topic"].(string)] = f
	}

	a := byTopic["demo/general/a"]
	if a == nil {
		t.Fatal("demo/general/a not resubscribed")
	}
	if got := filterStrings(a["filters"]); !reflect.DeepEqual(got, []string{"x", "[p q]"}) {
		t.Fatalf("a: want the current filters [x [p q]], got %v", got)
	}
	if fmt.Sprint(a["qos"]) != "2" || a["loadBalance"] != true || a["loadBalanceGroup"] != "workers" {
		t.Fatalf("a: options lost: %#v", a)
	}

	bf := byTopic["demo/general/b"]
	if bf == nil {
		t.Fatal("demo/general/b not resubscribed")
	}
	if _, has := bf["filters"]; has {
		t.Fatalf("b: unexpected filters: %#v", bf)
	}
	if fmt.Sprint(bf["qos"]) != "0" || bf["loadBalance"] != nil {
		t.Fatalf("b: options wrong: %#v", bf)
	}
}

// A broker that kept the session lists it in restoredSubscriptions; those
// topics must not be subscribed again, unless the broker holds other filters.
func TestReconnectSkipsTopicsTheBrokerRestored(t *testing.T) {
	b := newFakeBroker(t)
	b.setRestored(func(conn int) []any {
		if conn == 0 {
			return nil
		}
		return []any{
			// kraken's own memory: the subscribe request as sent
			map[string]any{"topic": "demo/general/a", "filters": []any{[]any{"Q", "p"}, "x"}, "qos": 1},
			// plain string form
			"demo/general/b",
			// restored, but with filters the client no longer has
			map[string]any{"topic": "demo/general/c", "filters": []any{"old"}},
		}
	})
	c := newFakeClient(t, b)

	if err := c.Subscribe("demo/general/a", noop, SubscribeOptions{Filters: []any{"x", []string{"p", "q"}}}); err != nil {
		t.Fatal(err)
	}
	if err := c.Subscribe("demo/general/b", noop); err != nil {
		t.Fatal(err)
	}
	if err := c.Subscribe("demo/general/c", noop, SubscribeOptions{Filters: []any{"old"}}); err != nil {
		t.Fatal(err)
	}
	if err := c.SetFilters("demo/general/c", []any{"new"}); err != nil {
		t.Fatal(err)
	}
	if err := c.Subscribe("demo/general/d", noop); err != nil {
		t.Fatal(err)
	}

	reconnected(t, b, c, 0)

	var topics []string
	for _, f := range b.framesOfType(1, "subscribe") {
		topics = append(topics, f["topic"].(string))
		if f["topic"] == "demo/general/c" {
			if got := filterStrings(f["filters"]); !reflect.DeepEqual(got, []string{"new"}) {
				t.Fatalf("c: want filters [new], got %v", got)
			}
		}
	}
	if !reflect.DeepEqual(topics, []string{"demo/general/c", "demo/general/d"}) {
		t.Fatalf("want only c (other filters) and d (not restored) resubscribed, got %v", topics)
	}
}

func TestParseRestored(t *testing.T) {
	// Decode through msgpack so the shapes are what the client really sees.
	wire, err := msgpack.Marshal(map[string]any{
		"type": "auth",
		"restoredSubscriptions": []any{
			"plain/topic",
			"",
			map[string]any{"topic": "resume/topic", "filters": []any{"x", []string{"a", "b"}}, "loadBalance": true},
			map[string]any{"pattern": "app/room/t", "topic": "internal-uuid/t"},
			map[string]any{"name": "named/topic", "filters": nil},
			map[string]any{"pattern": nil, "name": "fallback/topic"},
			map[string]any{"filters": []any{"orphan"}},
			42,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var msg protocolMessage
	if err := msgpack.Unmarshal(wire, &msg); err != nil {
		t.Fatal(err)
	}

	got := parseRestored(msg.RestoredSubscriptions)
	want := map[string]string{
		"plain/topic":    "",
		"resume/topic":   filterKey([]any{"x", []string{"a", "b"}}),
		"app/room/t":     "",
		"named/topic":    "",
		"fallback/topic": "",
	}
	if len(got) != len(want) {
		t.Fatalf("want %d topics, got %d: %#v", len(want), len(got), got)
	}
	for topic, key := range want {
		filters, ok := got[topic]
		if !ok {
			t.Errorf("missing %q", topic)
			continue
		}
		if filterKey(filters) != key {
			t.Errorf("%q: filters %#v", topic, filters)
		}
	}
	if _, ok := got["internal-uuid/t"]; ok {
		t.Error("pattern must win over an internal topic id")
	}
}

func TestFilterKey(t *testing.T) {
	same := [][2][]any{
		{nil, {}},
		{{"x", "y"}, {"y", "x"}},
		{{"x", "x"}, {"x"}},
		{{[]string{"B", "a"}}, {[]any{"a", "b"}}},
		{{"x", []string{"p", "q"}}, {[]any{"Q", "P"}, "x"}},
	}
	for _, p := range same {
		if filterKey(p[0]) != filterKey(p[1]) {
			t.Errorf("%#v and %#v should select the same messages", p[0], p[1])
		}
	}
	different := [][2][]any{
		{nil, {"x"}},
		{{"x"}, {"X"}}, // plain filters are case-sensitive in kraken
		{{"a", "b"}, {[]string{"a", "b"}}},
		{{"x"}, {"y"}},
	}
	for _, p := range different {
		if filterKey(p[0]) == filterKey(p[1]) {
			t.Errorf("%#v and %#v should differ", p[0], p[1])
		}
	}
}

// The broker drops presence and lobby membership with the connection, so a
// reconnect must send them again.
func TestReconnectRestoresPresenceAndLobbies(t *testing.T) {
	b := newFakeBroker(t)
	c := newFakeClient(t, b)

	data := map[string]any{"status": "online", "nested": map[string]any{"n": 1}}
	if err := c.SetPresence(data, "general"); err != nil {
		t.Fatal(err)
	}
	data["status"] = "changed after the call"
	if err := c.SetPresence(map[string]any{"project": true}); err != nil {
		t.Fatal(err)
	}
	lobby := c.SetApp("demo").SetLobby("everyone")
	if _, err := lobby.Subscribe(); err != nil {
		t.Fatal(err)
	}

	reconnected(t, b, c, 0)

	var room, project map[string]any
	for _, f := range b.framesOfType(1, "presence") {
		if f["roomId"] == "general" {
			room = f
		} else {
			project = f
		}
	}
	if room == nil {
		t.Fatal("room presence not sent again")
	}
	if d := room["data"].(map[string]any); d["status"] != "online" {
		t.Fatalf("room presence: %#v", d)
	}
	if project == nil || project["data"].(map[string]any)["project"] != true {
		t.Fatalf("presence without a room not sent again: %#v", project)
	}
	lobbies := b.framesOfType(1, "lobbySubscribe")
	if len(lobbies) != 1 || lobbies[0]["lobbyId"] != "everyone" {
		t.Fatalf("lobby not subscribed again: %#v", lobbies)
	}

	// A lobby that was unsubscribed is not restored.
	if err := lobby.Unsubscribe(); err != nil {
		t.Fatal(err)
	}
	reconnected(t, b, c, 1)
	if n := len(b.framesOfType(2, "lobbySubscribe")); n != 0 {
		t.Fatalf("unsubscribed lobby restored %d time(s)", n)
	}
}

// The session is restored before "connected" fires, so a handler that
// subscribes again has the last word, and the client keeps one handler per
// topic: each message is delivered once.
func TestConnectHandlerSubscribeDoesNotDuplicate(t *testing.T) {
	b := newFakeBroker(t)
	c := newFakeClient(t, b)

	var first, second atomic.Int32
	if err := c.Subscribe("demo/general/a", func(any, MessageMeta) { first.Add(1) },
		SubscribeOptions{Filters: []any{"x"}}); err != nil {
		t.Fatal(err)
	}
	c.On("connected", func(...any) {
		if err := c.Subscribe("demo/general/a", func(any, MessageMeta) { second.Add(1) },
			SubscribeOptions{Filters: []any{"x"}}); err != nil {
			t.Errorf("subscribe in connected handler: %v", err)
		}
	})

	reconnected(t, b, c, 0)
	eventually(t, "the handler's subscribe", func() bool {
		return len(b.framesOfType(1, "subscribe")) == 2
	})

	// Restore first, then the handler's own subscribe.
	subs := b.framesOfType(1, "subscribe")
	for i, f := range subs {
		if got := filterStrings(f["filters"]); !reflect.DeepEqual(got, []string{"x"}) {
			t.Fatalf("subscribe %d: filters %v", i, got)
		}
	}

	b.conn(1).write(t, map[string]any{"type": "message", "topic": "demo/general/a", "data": "hello"})
	eventually(t, "delivery", func() bool { return second.Load() == 1 })
	time.Sleep(50 * time.Millisecond)
	if first.Load() != 0 || second.Load() != 1 {
		t.Fatalf("want exactly one delivery to the newest handler, got first=%d second=%d", first.Load(), second.Load())
	}
}

// Close ends the session: nothing is tracked, a later Connect starts fresh,
// and a reconnect that was waiting does not happen.
func TestCloseEndsSession(t *testing.T) {
	b := newFakeBroker(t)
	c := newFakeClient(t, b)

	if err := c.Subscribe("demo/general/a", noop, SubscribeOptions{Filters: []any{"x"}}); err != nil {
		t.Fatal(err)
	}
	if err := c.SetPresence(map[string]any{"s": 1}, "general"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetApp("demo").SetLobby("everyone").Subscribe(); err != nil {
		t.Fatal(err)
	}

	c.Close()

	c.mu.RLock()
	empty := len(c.subscriptions) == 0 && c.roomPresence == nil && c.projectPresence == nil && len(c.lobbies) == 0
	c.mu.RUnlock()
	if !empty {
		t.Fatal("Close did not clear the session")
	}

	if err := c.Connect(); err != nil {
		t.Fatal(err)
	}
	flush(t, c)
	if auth := b.framesOfType(1, "auth"); len(auth) != 1 || auth[0]["reconnect"] != nil {
		t.Fatalf("connect after Close must be fresh: %#v", auth)
	}
	for _, f := range b.framesOf(1) {
		if f["type"] != "auth" && f["type"] != "getLobbyPresence" {
			t.Fatalf("connect after Close sent %v", f)
		}
	}
}

func TestCloseStopsPendingReconnect(t *testing.T) {
	b := newFakeBroker(t)
	c := New("dev-token-alice", Options{
		URL:                  b.url(),
		ReconnectInterval:    200 * time.Millisecond,
		MaxReconnectAttempts: -1,
		HeartbeatInterval:    -1,
	})
	if err := c.Connect(); err != nil {
		t.Fatal(err)
	}
	if err := c.Subscribe("demo/general/a", noop); err != nil {
		t.Fatal(err)
	}

	b.drop(0)
	eventually(t, "reconnecting", func() bool { return c.Status() == StatusReconnecting })
	c.Close()

	time.Sleep(400 * time.Millisecond)
	if n := b.connCount(); n != 1 {
		t.Fatalf("client reconnected after Close: %d connections", n)
	}
	if s := c.Status(); s != StatusDisconnected {
		t.Fatalf("status after Close: %v", s)
	}
}

// Unsubscribe while the connection is down still leaves the session, so the
// topic is not brought back.
func TestUnsubscribeWhileDisconnectedForgetsTopic(t *testing.T) {
	b := newFakeBroker(t)
	c := New("dev-token-alice", Options{
		URL:                  b.url(),
		ReconnectInterval:    200 * time.Millisecond,
		MaxReconnectAttempts: -1,
		HeartbeatInterval:    -1,
	})
	if err := c.Connect(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	if err := c.Subscribe("demo/general/a", noop); err != nil {
		t.Fatal(err)
	}
	if err := c.Subscribe("demo/general/b", noop); err != nil {
		t.Fatal(err)
	}

	b.drop(0)
	eventually(t, "reconnecting", func() bool { return c.Status() == StatusReconnecting })
	if err := c.Unsubscribe("demo/general/a"); err != ErrNotConnected {
		t.Fatalf("want ErrNotConnected, got %v", err)
	}

	eventually(t, "reconnect", func() bool { return c.Status() == StatusConnected })
	flush(t, c)
	var topics []string
	for _, f := range b.framesOfType(1, "subscribe") {
		topics = append(topics, f["topic"].(string))
	}
	if !reflect.DeepEqual(topics, []string{"demo/general/b"}) {
		t.Fatalf("want only b restored, got %v", topics)
	}
}

// Session calls racing with repeated drops must be free of data races (run
// with -race) and end with the broker and the session in agreement.
func TestSessionChangesDuringReconnectsAreRaceFree(t *testing.T) {
	b := newFakeBroker(t)
	c := newFakeClient(t, b)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for w := 0; w < 4; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			topic := fmt.Sprintf("demo/general/w%d", w)
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				_ = c.Subscribe(topic, noop, SubscribeOptions{Filters: []any{fmt.Sprintf("f%d", i%3)}})
				_ = c.AddFilters(topic, []string{"extra"})
				_ = c.SetPresence(map[string]any{"i": i}, "general")
				_ = c.Emit(topic, i)
				_ = c.Status()
				time.Sleep(time.Millisecond)
			}
		}()
	}

	for i := 0; i < 5; i++ {
		time.Sleep(30 * time.Millisecond)
		reconnected(t, b, c, i)
	}
	close(stop)
	wg.Wait()

	// Settle: one more reconnect, then the restore must match the session.
	reconnected(t, b, c, 5)
	c.mu.RLock()
	want := map[string]string{}
	for topic, s := range c.subscriptions {
		want[topic] = filterKey(s.filters)
	}
	c.mu.RUnlock()
	got := map[string]string{}
	for _, f := range b.framesOfType(6, "subscribe") {
		filters, _ := f["filters"].([]any)
		got[f["topic"].(string)] = filterKey(filters)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("restore does not match the session:\n got %v\nwant %v", got, want)
	}
}
