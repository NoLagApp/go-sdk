package nolag

import (
	"fmt"
	"sort"
	"strings"
)

// The session is what the caller has asked for on a Client: its
// subscriptions, presence and lobby subscriptions. A broker connection can
// drop at any time, and the broker may or may not still hold the session when
// the client reconnects (kraken keeps subscriptions in RAM, so a broker
// restart forgets them; presence and lobbies always belong to one
// connection). The client therefore keeps the session itself and puts back
// whatever the broker did not restore (see restoreSession). Close ends the
// session.

// subscription is one subscribed topic: its handler, the options resolved
// when Subscribe was called, and its current filters (kept in step with
// SetFilters, AddFilters and RemoveFilters). It holds everything needed to
// send the same subscribe frame again after a reconnect.
//
// A subscription value is never modified in place; changes store a new copy,
// so a snapshot taken under c.mu stays valid after the lock is released.
type subscription struct {
	handler          MessageHandler
	qos              QoS
	loadBalance      bool
	loadBalanceGroup string
	filters          []any
}

// roomPresence is the last presence set for a room. The broker keeps one
// room presence per connection (setting it for another room leaves the
// previous one), so the last call is the whole state.
type roomPresence struct {
	roomID string
	data   map[string]any
}

// newSubscription resolves the options of a Subscribe call against the
// connection-level defaults.
func (c *Client) newSubscription(handler MessageHandler, opts []SubscribeOptions) subscription {
	s := subscription{
		handler:          handler,
		qos:              *c.options.QoS,
		loadBalance:      c.options.LoadBalance,
		loadBalanceGroup: c.options.LoadBalanceGroup,
	}
	if len(opts) > 0 {
		opt := opts[0]
		if opt.QoS != nil {
			s.qos = *opt.QoS
		}
		if opt.LoadBalance != nil {
			s.loadBalance = *opt.LoadBalance
		}
		if opt.LoadBalanceGroup != "" {
			s.loadBalanceGroup = opt.LoadBalanceGroup
		}
		s.filters = cloneFilters(opt.Filters)
	}
	return s
}

// frame builds the subscribe frame for topic.
func (s subscription) frame(topic string) *protocolMessage {
	// Before v0.5.0 the QoS options existed but were never put on the wire,
	// so every Go client ran at the broker default of 1.
	qos := s.qos
	msg := &protocolMessage{
		Type:  msgTypeSubscribe,
		Topic: topic,
		QoS:   &qos,
	}
	if len(s.filters) > 0 {
		msg.Filters = s.filters
	}
	// Only include loadBalance fields when actually using load balancing
	if s.loadBalance {
		msg.LoadBalance = true
		msg.LoadBalanceGroup = s.loadBalanceGroup
	}
	return msg
}

func presenceFrame(data map[string]any, roomID string) *protocolMessage {
	return &protocolMessage{Type: msgTypePresSet, Data: data, RoomID: roomID}
}

// clearSessionLocked forgets the session. c.mu must be held for writing.
func (c *Client) clearSessionLocked() {
	c.subscriptions = make(map[string]subscription)
	c.roomPresence = nil
	c.projectPresence = nil
	c.lobbies = make(map[string]struct{})
}

// restoreSession puts the session back on a connection that has just
// authenticated, before it is reported as connected. restored is the auth
// reply's restoredSubscriptions: what the broker already put back by itself.
//
//   - A tracked subscription the broker did not restore is subscribed again
//     with its options and current filters. One the broker restored with
//     other filters (a setFilters that never reached it, or another
//     connection of the same actor) is subscribed again too: a subscribe
//     replaces the broker's subscription for that topic.
//   - Presence and lobby subscriptions belong to a connection, so the broker
//     never restores them; they are always sent again.
//
// The frames are fire-and-forget like Subscribe. An error means a frame
// could not be written, so the connection is unusable and connect fails.
func (c *Client) restoreSession(restoredRaw []any) error {
	c.sessionMu.Lock()
	defer c.sessionMu.Unlock()

	restored := parseRestored(restoredRaw)

	c.mu.RLock()
	subs := make(map[string]subscription, len(c.subscriptions))
	for topic, s := range c.subscriptions {
		subs[topic] = s
	}
	room := c.roomPresence
	project := c.projectPresence
	lobbies := make([]string, 0, len(c.lobbies))
	for id := range c.lobbies {
		lobbies = append(lobbies, id)
	}
	c.mu.RUnlock()

	topics := make([]string, 0, len(subs))
	for topic := range subs {
		topics = append(topics, topic)
	}
	sort.Strings(topics)
	sort.Strings(lobbies)

	for _, topic := range topics {
		s := subs[topic]
		if filters, ok := restored[topic]; ok && filterKey(filters) == filterKey(s.filters) {
			c.log("Restored by the broker:", topic)
			continue
		}
		if err := c.send(s.frame(topic)); err != nil {
			return fmt.Errorf("resubscribe %s: %w", topic, err)
		}
		c.log("Resubscribed to:", topic)
	}

	if project != nil {
		if err := c.send(presenceFrame(project, "")); err != nil {
			return fmt.Errorf("restore presence: %w", err)
		}
	}
	if room != nil {
		if err := c.send(presenceFrame(room.data, room.roomID)); err != nil {
			return fmt.Errorf("restore presence in %s: %w", room.roomID, err)
		}
	}
	for _, id := range lobbies {
		if err := c.send(&protocolMessage{Type: msgTypeLobbySubscribe, LobbyID: id}); err != nil {
			return fmt.Errorf("resubscribe lobby %s: %w", id, err)
		}
	}
	return nil
}

// parseRestored normalises the auth reply's restoredSubscriptions into
// topic -> filters (nil for none). Entries are plain topic strings, or maps
// that name the topic as "pattern" or "name" (a control plane that persists
// subscriptions, where "topic" can be an internal id) or as "topic"
// (kraken's own reconnect memory, which keeps the subscribe request as sent).
func parseRestored(raw []any) map[string][]any {
	out := make(map[string][]any, len(raw))
	for _, item := range raw {
		switch v := item.(type) {
		case string:
			if v != "" {
				out[v] = nil
			}
		case map[string]any:
			topic := ""
			for _, key := range []string{"pattern", "name", "topic"} {
				if s, ok := v[key].(string); ok && s != "" {
					topic = s
					break
				}
			}
			if topic == "" {
				continue
			}
			filters, _ := v["filters"].([]any)
			out[topic] = filters
		}
	}
	return out
}

// filterKey reduces a filter list to the set of sub-topics kraken derives
// from it: plain filters as they are, AND groups lowercased, sorted and
// joined with "|". Lists with the same key select the same messages; an
// empty key means no filters (every message on the topic).
func filterKey(filters []any) string {
	if len(filters) == 0 {
		return ""
	}
	set := make(map[string]struct{}, len(filters))
	for _, f := range filters {
		switch v := f.(type) {
		case string:
			set[v] = struct{}{}
		case []string:
			set[andGroupKey(v)] = struct{}{}
		case []any:
			parts := make([]string, len(v))
			for i, p := range v {
				parts[i] = fmt.Sprint(p)
			}
			set[andGroupKey(parts)] = struct{}{}
		default:
			set[fmt.Sprint(v)] = struct{}{}
		}
	}
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return strings.Join(keys, "\x00")
}

func andGroupKey(parts []string) string {
	lowered := make([]string, len(parts))
	for i, p := range parts {
		lowered[i] = strings.ToLower(p)
	}
	sort.Strings(lowered)
	return strings.Join(lowered, "|")
}

// cloneFilters copies a filter list (nil for an empty one), so the session
// does not share memory with the caller's slice.
func cloneFilters(filters []any) []any {
	if len(filters) == 0 {
		return nil
	}
	out := make([]any, len(filters))
	for i, f := range filters {
		out[i] = cloneValue(f)
	}
	return out
}

// cloneMap copies a map and the maps and slices inside it.
func cloneMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = cloneValue(v)
	}
	return out
}

func cloneValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		return cloneMap(t)
	case []any:
		if t == nil {
			return t
		}
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = cloneValue(e)
		}
		return out
	case []string:
		if t == nil {
			return t
		}
		out := make([]string, len(t))
		copy(out, t)
		return out
	default:
		return v
	}
}
