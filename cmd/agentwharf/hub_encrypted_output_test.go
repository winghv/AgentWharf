package main

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/winghv/agentwharf/experimental/e2ee"
	"github.com/winghv/agentwharf/protocol"
	"nhooyr.io/websocket"
)

func TestRequiredHubOutputSealsBeforeNetwork(t *testing.T) {
	for _, kind := range []string{"session.message", "agent.activity", "log.tail", "resource.sample", "presence"} {
		t.Run(kind, func(t *testing.T) { testRequiredHubOutputSealsBeforeNetwork(t, kind) })
	}
}
func testRequiredHubOutputSealsBeforeNetwork(t *testing.T, kind string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	runtime, err := openMachineE2EERuntime(ctx, filepath.Join(t.TempDir(), "endpoint"), "machine", "account")
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.database.Close()
	public, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.executor.ReplaceGrants(ctx, "session", "key", 0, []e2ee.DeviceGrant{{DeviceID: "client", VerifyKey: public, Control: true}}); err != nil {
		t.Fatal(err)
	}
	observed := make(chan *protocol.Event, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		frame, err := readCLIProtocolFrame(ctx, conn)
		if err != nil {
			t.Error(err)
			return
		}
		event, ok := frame.(*protocol.Event)
		if !ok {
			t.Error("not event")
			return
		}
		observed <- event
	}))
	defer server.Close()
	socket, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer socket.CloseNow()
	connection := newHubConnection(wrapConfig{SessionID: "session", ContentMode: protocol.ContentModeRequired, ProtocolVersion: 2, e2eeRuntime: runtime}, socket, nil)
	event := &protocol.Event{SessionID: "session", Type: kind, Time: 1, Payload: []byte(`{"role":"agent","content":[{"kind":"text","text":"secret-output-canary"}]}`)}
	if kind != "session.message" {
		event.Payload = []byte(`{"detail":"secret-output-canary"}`)
	}
	if err := connection.write(ctx, event); err != nil {
		t.Fatal(err)
	}
	select {
	case wire := <-observed:
		if strings.Contains(string(wire.Payload), "secret-output-canary") || (wire.ProposalID == "") != isCLIEventEphemeral(kind) {
			t.Fatal("unsealed network event")
		}
		if _, err := protocol.DecodeEncryptedPacketCarrier(wire.Payload, "event", kind, ""); err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("no encrypted output")
	}
	if !strings.Contains(string(event.Payload), "secret-output-canary") {
		t.Fatal("caller payload mutated")
	}
}

func TestRequiredACPStreamSealsLargeWriteDiffAndProseWithinE2EEBudget(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	runtime, err := openMachineE2EERuntime(ctx, filepath.Join(t.TempDir(), "endpoint"), "machine", "account")
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.database.Close()
	public, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.executor.ReplaceGrants(ctx, "session", "key", 0, []e2ee.DeviceGrant{{DeviceID: "client", VerifyKey: public, Control: true}}); err != nil {
		t.Fatal(err)
	}
	observed := make(chan *protocol.Event, 16)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(observed)
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		for {
			frame, err := readCLIProtocolFrame(ctx, conn)
			if err != nil {
				return
			}
			if event, ok := frame.(*protocol.Event); ok {
				observed <- event
			}
		}
	}))
	defer server.Close()
	socket, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer socket.CloseNow()

	cfg := wrapConfig{
		SessionID:       "session",
		Provider:        "pi",
		Format:          "acp",
		ContentMode:     protocol.ContentModeRequired,
		ProtocolVersion: protocol.ProtocolVersionV2,
		e2eeRuntime:     runtime,
	}
	connection := newHubConnection(cfg, socket, nil)

	largeJava := strings.Repeat("if (left < right && map.get(key) > 0) { list.add(entry); }\n", 700) // ~41 KB
	oldJava := strings.Repeat("old line <T>\n", 1600)                                                // ~20 KB
	chunkA := strings.Repeat("a", 20*1024)
	chunkB := strings.Repeat("b", 20*1024)

	lines := []string{
		`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"acp_1","update":{"sessionUpdate":"tool_call","toolCallId":"call_big_write","title":"write","kind":"edit","status":"in_progress","rawInput":{"path":"/Users/dev/aime-data/SqlIntOverflowUtils.java","content":` + mustMarshalJSONString(t, largeJava) + `}}}}`,
		`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"acp_1","update":{"sessionUpdate":"tool_call_update","toolCallId":"call_big_write","status":"completed","content":[{"type":"diff","path":"/Users/dev/aime-data/SqlIntOverflowUtils.java","oldText":` + mustMarshalJSONString(t, oldJava) + `,"newText":` + mustMarshalJSONString(t, largeJava) + `}]}}}`,
		`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"acp_1","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":` + mustMarshalJSONString(t, chunkA) + `}}}}`,
		`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"acp_1","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":` + mustMarshalJSONString(t, chunkB) + `}}}}`,
		`{"jsonrpc":"2.0","id":9,"result":{"stopReason":"end_turn"}}`,
	}
	scanner := newLargeLineScanner(strings.Join(lines, "\n") + "\n")
	if err := streamACPProviderOutput(ctx, cfg, scanner, nil, nil, func(event protocol.Event) error {
		return connection.write(ctx, &event)
	}); err != nil {
		t.Fatalf("streamACPProviderOutput under required E2EE failed: %v", err)
	}
	_ = socket.Close(websocket.StatusNormalClosure, "")
	count := 0
	for wire := range observed {
		count++
		if _, err := protocol.DecodeEncryptedPacketCarrier(wire.Payload, "event", wire.Type, ""); err != nil {
			t.Fatalf("event %d (%s) carrier invalid: %v", count, wire.Type, err)
		}
	}
	if count < 4 {
		t.Fatalf("observed %d encrypted events, want at least start, result, message, and ready", count)
	}
}

func TestBoundEncryptedEventPayloadPreservesPublicRoutingAndCallerSlice(t *testing.T) {
	t.Parallel()

	oversizedMessage, err := json.Marshal(map[string]any{
		"message_id": "msg_1",
		"role":       "agent",
		"content": []map[string]string{{
			"kind": "text",
			"text": strings.Repeat("[MASKED]", 5000),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	originalCopy := append([]byte(nil), oversizedMessage...)
	bounded := boundEncryptedEventPayload("session.message", oversizedMessage)
	if len(bounded) > encryptedEventPayloadBudget {
		t.Fatalf("bounded message = %d bytes, exceeds %d", len(bounded), encryptedEventPayloadBudget)
	}
	if string(oversizedMessage) != string(originalCopy) {
		t.Fatal("boundEncryptedEventPayload mutated caller slice")
	}
	if meta, err := e2ee.ProjectPublicMetadata("session.message", bounded); err != nil || meta.Role != "agent" {
		t.Fatalf("ProjectPublicMetadata(session.message) = %+v, %v", meta, err)
	}

	oversizedState, err := json.Marshal(map[string]any{
		"state":    "ready",
		"provider": "pi",
		"metadata": map[string]string{"huge": strings.Repeat("z", 40*1024)},
	})
	if err != nil {
		t.Fatal(err)
	}
	boundedState := boundEncryptedEventPayload("session.state", oversizedState)
	if len(boundedState) > encryptedEventPayloadBudget {
		t.Fatalf("bounded state = %d bytes, exceeds %d", len(boundedState), encryptedEventPayloadBudget)
	}
	if meta, err := e2ee.ProjectPublicMetadata("session.state", boundedState); err != nil || meta.State != "ready" {
		t.Fatalf("ProjectPublicMetadata(session.state) = %+v, %v", meta, err)
	}
}

func mustMarshalJSONString(t *testing.T, value string) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func newLargeLineScanner(input string) *bufio.Scanner {
	scanner := bufio.NewScanner(strings.NewReader(input))
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	return scanner
}
