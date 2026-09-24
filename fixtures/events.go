package fixtures

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/winghv/agentwharf/store"
)

// EventCountFixtures are deterministic, synthetic sizes used by workbench
// performance tests. Payloads contain no prompt, tool output, paths, tokens, or
// other user-controlled values.
var EventCountFixtures = []int{1_000, 10_000, 100_000}

func EventCountFixture(sessionID string, count int) []store.PendingEvent {
	if sessionID == "" {
		sessionID = "fixture-session"
	}
	if count < 0 {
		count = 0
	}
	events := make([]store.PendingEvent, count)
	base := time.UnixMilli(1_700_000_000_000).UTC()
	for index := range events {
		seq := index + 1
		typeName := "session.message"
		if seq%10 == 0 {
			typeName = "session.tool_call"
		} else if seq%25 == 0 {
			typeName = "permission.request"
		}
		payload, _ := json.Marshal(map[string]string{
			"fixture": "workbench",
			"ordinal": strconv.Itoa(seq),
		})
		events[index] = store.PendingEvent{Type: typeName, Time: base.Add(time.Duration(seq) * time.Millisecond), Payload: payload}
	}
	return events
}
