package storetest

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/winghv/agentwharf/store"
)

func BootstrapContract(t *testing.T, events store.BootstrapStore) {
	t.Helper()
	ctx := context.Background()
	const sessionID = "ses_bootstrap"
	pending := []store.PendingEvent{
		{Type: "session.state", Payload: []byte(`{"state":"ready"}`)},
		{Type: "session.run.capabilities", Payload: []byte(`{"opaque":"original-carrier"}`)},
		{Type: "permission.request", Payload: []byte(`{"request_id":"request_1"}`)},
		{Type: "permission.decision", Payload: []byte(`{"request_id":"request_1","decision":"allow"}`)},
		{Type: "session.state", Payload: []byte(`{"state":"busy"}`)},
	}
	for index := 0; index < 10_000; index++ {
		pending = append(pending, store.PendingEvent{Type: "session.message", Payload: []byte(fmt.Sprintf(`{"n":%d}`, index))})
	}
	for index := range pending {
		pending[index].Time = time.Unix(int64(index+1), 0)
	}
	if _, err := events.Append(ctx, sessionID, pending); err != nil {
		t.Fatal(err)
	}
	watermark := int64(len(pending))
	if _, err := events.Append(ctx, sessionID, []store.PendingEvent{{Type: "session.state", Time: time.Unix(20_000, 0), Payload: []byte(`{"state":"completed"}`)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := events.Append(ctx, "ses_other", []store.PendingEvent{{Type: "session.state", Time: time.Unix(1, 0), Payload: []byte(`{"state":"ready"}`)}}); err != nil {
		t.Fatal(err)
	}
	state, err := events.Bootstrap(ctx, sessionID, watermark)
	if err != nil {
		t.Fatal(err)
	}
	var seqs []int64
	for _, event := range state {
		if event.SessionID != sessionID {
			t.Fatal("cross-session bootstrap event")
		}
		seqs = append(seqs, event.Seq)
	}
	if !reflect.DeepEqual(seqs, []int64{2, 3, 4, 5}) {
		t.Fatalf("bootstrap seqs = %v", seqs)
	}
	if string(state[0].Payload) != string(pending[1].Payload) {
		t.Fatal("bootstrap changed opaque payload")
	}
	for _, session := range []string{sessionID, "ses_missing"} {
		empty, err := events.Bootstrap(ctx, session, 0)
		if err != nil || len(empty) != 0 {
			t.Fatalf("empty bootstrap = %v, %v", empty, err)
		}
	}
}
