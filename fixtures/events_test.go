package fixtures

import (
	"testing"

	"github.com/winghv/agentwharf/store"
)

func TestEventCountFixturesAreSyntheticAndBounded(t *testing.T) {
	for _, count := range EventCountFixtures {
		events := EventCountFixture("fixture", count)
		if len(events) != count {
			t.Fatalf("fixture size = %d, want %d", len(events), count)
		}
		for _, event := range events {
			if event.Type == "" || len(event.Payload) == 0 {
				t.Fatal("fixture event is incomplete")
			}
			if string(event.Payload) == "" || string(event.Payload) == "null" {
				t.Fatal("fixture payload is empty")
			}
		}
	}
	if got := EventCountFixture("fixture", -1); len(got) != 0 {
		t.Fatalf("negative fixture size = %d", len(got))
	}
	var _ store.PendingEvent
}
