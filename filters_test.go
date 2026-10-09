package nolag

import "testing"

// An empty list is what switches a subscription back to wildcard, and kraken
// ignores a setFilters frame without the field. v0.5.0 lost the empty list to
// omitempty, so clearing filters (SetFilters with an empty or nil list, or
// RemoveFilters of the last filter) never reached the broker.
func TestSetFiltersSendsAnEmptyList(t *testing.T) {
	b := newFakeBroker(t)
	c := newFakeClient(t, b)

	topic := "demo/general/a"
	if err := c.Subscribe(topic, noop, SubscribeOptions{Filters: []any{"x"}}); err != nil {
		t.Fatal(err)
	}
	steps := []func() error{
		func() error { return c.SetFilters(topic, nil) },
		func() error { return c.SetFilters(topic, []any{}) },
		func() error { return c.SetFilters(topic, []any{"x"}) },
		func() error { return c.RemoveFilters(topic, []string{"x"}) },
	}
	for i, step := range steps {
		if err := step(); err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
	}
	flush(t, c)

	sets := b.framesOfType(0, "setFilters")
	if len(sets) != len(steps) {
		t.Fatalf("want %d setFilters frames, got %d", len(steps), len(sets))
	}
	for _, i := range []int{0, 1, 3} {
		f, present := sets[i]["filters"]
		if !present || len(filterStrings(f)) != 0 {
			t.Errorf("frame %d must carry an empty filters list: %#v", i, sets[i])
		}
	}
	if got := filterStrings(sets[2]["filters"]); len(got) != 1 || got[0] != "x" {
		t.Errorf("frame 2: %#v", sets[2])
	}
}
