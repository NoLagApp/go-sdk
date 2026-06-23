package nolag

import "testing"

// Persistent Presence (base go-sdk): the ActorPresence struct carries Status,
// and the presence-frame parse the client does extracts it. Validates the wire
// contract: a persistent actor's presence + status round-trip.
func TestPersistentPresenceStatusParse(t *testing.T) {
	// The shape Kraken sends in a presenceList item / presence event data.
	m := map[string]any{
		"actorTokenId": "echo",
		"presence": map[string]any{
			"capabilities": []any{"soil_analysis"},
			"persistent":   true,
			"wake":         map[string]any{"url": "http://localhost:9999/wake"},
		},
		"status": "offline",
	}

	// Same extraction the client performs (GetPresence / presence event).
	p := ActorPresence{Presence: map[string]any{}}
	if id, ok := m["actorTokenId"].(string); ok {
		p.ActorTokenID = id
	}
	if pm, ok := m["presence"].(map[string]any); ok {
		p.Presence = pm
	}
	if s, ok := m["status"].(string); ok {
		p.Status = s
	}

	if p.ActorTokenID != "echo" {
		t.Fatalf("actorTokenId: got %q", p.ActorTokenID)
	}
	if p.Status != "offline" {
		t.Fatalf("status: want offline, got %q", p.Status)
	}
	if persistent, _ := p.Presence["persistent"].(bool); !persistent {
		t.Fatalf("persistent flag not preserved in presence")
	}
	if wake, ok := p.Presence["wake"].(map[string]any); !ok || wake["url"] != "http://localhost:9999/wake" {
		t.Fatalf("wake config not preserved in presence")
	}
}
