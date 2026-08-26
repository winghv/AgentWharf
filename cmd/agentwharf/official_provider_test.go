package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/winghv/agentwharf/adapter/core"
	"github.com/winghv/agentwharf/protocol"
	"nhooyr.io/websocket"
)

func transcriptJSON(t *testing.T, value map[string]any) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal transcript: %v", err)
	}
	return encoded
}

func TestTranslateTranscriptUserMessage(t *testing.T) {
	line := transcriptJSON(t, map[string]any{
		"type": "user", "uuid": "u1",
		"message": map[string]any{"role": "user", "content": "hello"},
	})
	events, err := claudeProvider{}.translateLine("ses_1", line)
	if err != nil {
		t.Fatalf("translateLine() error = %v", err)
	}
	if len(events) != 1 || events[0].Type != "session.message" {
		t.Fatalf("events = %+v", events)
	}
	var payload map[string]any
	if err := json.Unmarshal(events[0].Payload, &payload); err != nil {
		t.Fatal(err)
	}
	content := payload["content"].([]any)
	block := content[0].(map[string]any)
	if block["kind"] != "text" || block["text"] != "hello" {
		t.Fatalf("payload = %+v", payload)
	}
}

func TestTranslateTranscriptSplitsLargeMessage(t *testing.T) {
	text := strings.Repeat("大输出🙂 ", 20000)
	line := transcriptJSON(t, map[string]any{
		"type": "assistant", "uuid": "a-large",
		"message": map[string]any{"role": "assistant", "content": text},
	})
	events, err := claudeProvider{}.translateLine("ses_1", line)
	if err != nil {
		t.Fatalf("translateLine() error = %v", err)
	}
	if len(events) < 2 {
		t.Fatalf("events = %d, want split output", len(events))
	}
	var rebuilt strings.Builder
	for _, event := range events {
		if event.Type != "session.message" || len(event.Payload) > protocol.MaxEventPayloadBytes {
			t.Fatalf("event = type:%s payload_bytes:%d", event.Type, len(event.Payload))
		}
		var payload map[string]any
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			t.Fatal(err)
		}
		content := payload["content"].([]any)
		rebuilt.WriteString(content[0].(map[string]any)["text"].(string))
	}
	if rebuilt.String() != text {
		t.Fatalf("rebuilt text differs: got %d bytes, want %d", rebuilt.Len(), len(text))
	}
}

func TestTranslateTranscriptAssistantToolCall(t *testing.T) {
	line := transcriptJSON(t, map[string]any{
		"type": "assistant", "uuid": "a1",
		"message": map[string]any{"role": "assistant", "content": []any{
			map[string]any{"type": "text", "text": "running"},
			map[string]any{"type": "tool_use", "id": "t1", "name": "Bash", "input": map[string]any{"cmd": "ls"}},
		}},
	})
	events, err := claudeProvider{}.translateLine("ses_1", line)
	if err != nil {
		t.Fatalf("translateLine() error = %v", err)
	}
	var sawMessage, sawTool bool
	for _, event := range events {
		switch event.Type {
		case "session.message":
			sawMessage = true
		case "session.tool_call":
			sawTool = true
			var payload map[string]any
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				t.Fatal(err)
			}
			if payload["name"] != "Bash" {
				t.Fatalf("tool payload = %+v", payload)
			}
		}
	}
	if !sawMessage || !sawTool {
		t.Fatalf("events = %+v, sawMessage=%t sawTool=%t", events, sawMessage, sawTool)
	}
}

func TestTranslateTranscriptTurnCompletion(t *testing.T) {
	line := transcriptJSON(t, map[string]any{
		"type": "system", "subtype": "turn_duration", "uuid": "turn_1",
		"sessionId": "provider_1", "durationMs": 1250,
	})
	events, err := claudeProvider{}.translateLine("ses_1", line)
	if err != nil {
		t.Fatalf("translateLine() error = %v", err)
	}
	if len(events) != 1 || events[0].Type != "agent.activity" || events[0].Seq != nil {
		t.Fatalf("events = %+v", events)
	}
	var payload map[string]any
	if err := json.Unmarshal(events[0].Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["kind"] != "turn_completed" || payload["turn_id"] != "turn_1" || payload["provider_session_id"] != "provider_1" || payload["duration_ms"] != float64(1250) {
		t.Fatalf("payload = %+v", payload)
	}
}

func TestTranslateTranscriptSkipsInternalEntries(t *testing.T) {
	for _, value := range []map[string]any{
		{"type": "summary", "summary": "x", "leafUuid": "u1"},
		{"type": "system", "uuid": "u1"},
	} {
		events, err := claudeProvider{}.translateLine("ses_1", transcriptJSON(t, value))
		if err != nil || len(events) != 0 {
			t.Fatalf("translateLine(%v) = %+v, %v", value, events, err)
		}
	}
}

func TestOfficialProviderForAgent(t *testing.T) {
	if got := officialProviderForAgent("claude").command(); got != "claude" {
		t.Fatalf("officialProviderForAgent(claude).command() = %q", got)
	}
	if got := officialProviderForAgent("claude-code").command(); got != "claude" {
		t.Fatalf("officialProviderForAgent(claude-code).command() = %q", got)
	}
	if got := officialProviderForAgent("codex").command(); got != "codex" {
		t.Fatalf("officialProviderForAgent(codex).command() = %q", got)
	}
	if got := officialProviderForAgent("gemini").command(); got != "gemini" {
		t.Fatalf("officialProviderForAgent(gemini).command() = %q", got)
	}
	if got := officialProviderForAgent("unknown").command(); got != "unknown" {
		t.Fatalf("officialProviderForAgent(unknown).command() = %q", got)
	}
}

func TestSuperviseOfficialAdapterHeartbeatClosesTimedOutTransport(t *testing.T) {
	accepted := make(chan *websocket.Conn, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		accepted <- conn
		<-r.Context().Done()
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")
	select {
	case <-accepted:
	case <-time.After(time.Second):
		t.Fatal("server did not accept adapter transport")
	}

	connection := newHubConnection(wrapConfig{}, conn, nil)
	go superviseOfficialAdapterHeartbeat(ctx, heartbeatConfig{
		Interval: time.Millisecond,
		Timeout:  10 * time.Millisecond,
	}, connection, func(protocol.Frame) error {
		return nil
	}, &officialHeartbeatPongRouter{}, nil)

	deadline := time.Now().Add(time.Second)
	for connection.current() != nil && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if connection.current() != nil {
		t.Fatal("timed-out official heartbeat did not close the Hub transport")
	}
}

func TestOfficialProviderRotatesCredentialBeforeReconnect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	serverDone := make(chan error, 1)
	var connections atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{CompressionMode: websocket.CompressionDisabled})
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "")

		frame, err := readFrameFromConn(ctx, conn)
		hello, ok := frame.(*protocol.Hello)
		if err != nil || !ok {
			serverDone <- fmt.Errorf("read hello: %T %v", frame, err)
			return
		}
		connectionNumber := connections.Add(1)
		if connectionNumber == 1 {
			if hello.Token != "old-token" || hello.Resume {
				serverDone <- fmt.Errorf("initial hello = %+v", hello)
				return
			}
			ack := reconnectHelloAck("ses_official", 1, 1)
			ack.ConnectionAuthority.ExpiresAt = time.Now().Add(4 * time.Minute).UnixMilli()
			if err := writeFrameToConn(ctx, conn, ack); err != nil {
				serverDone <- err
				return
			}
			if err := writeFrameToConn(ctx, conn, &protocol.Pong{Nonce: "rotation-heartbeat"}); err != nil {
				serverDone <- err
				return
			}
			frame, err = readFrameFromConn(ctx, conn)
			request, ok := frame.(*protocol.CredentialRotationRequest)
			if err != nil || !ok || request.RotationID == "" {
				serverDone <- fmt.Errorf("rotation request = %T %+v, %v", frame, frame, err)
				return
			}
			credential := &protocol.CredentialRotationCredential{
				SessionID: "ses_official", RotationID: request.RotationID, Generation: 2,
				Credential: "new-token", ExpiresAt: time.Now().Add(15 * time.Minute).UnixMilli(),
			}
			if err := writeFrameToConn(ctx, conn, credential); err != nil {
				serverDone <- err
				return
			}
			frame, err = readFrameFromConn(ctx, conn)
			possession, ok := frame.(*protocol.CredentialRotationPossession)
			if err != nil || !ok || possession.RotationID != request.RotationID {
				serverDone <- fmt.Errorf("rotation possession = %T %+v, %v", frame, frame, err)
				return
			}
			if err := writeFrameToConn(ctx, conn, &protocol.CredentialRotationActivation{
				RotationID: request.RotationID, Generation: 2, ConnectionEpoch: 2, AcceptedFence: 2,
			}); err != nil {
				serverDone <- err
				return
			}
			_ = conn.Close(websocket.StatusGoingAway, "verify rotated reconnect")
			return
		}

		if hello.Token != "new-token" || !hello.Resume {
			serverDone <- fmt.Errorf("reconnect hello = %+v", hello)
			return
		}
		if err := writeFrameToConn(ctx, conn, reconnectHelloAck("ses_official", 3, 2)); err != nil {
			serverDone <- err
			return
		}
		if err := writeFrameToConn(ctx, conn, &protocol.Ping{Nonce: "rotated-reconnect"}); err != nil {
			serverDone <- err
			return
		}
		frame, err = readFrameFromConn(ctx, conn)
		pong, ok := frame.(*protocol.Pong)
		if err != nil || !ok || pong.Nonce != "rotated-reconnect" {
			serverDone <- fmt.Errorf("reconnect pong = %T %+v, %v", frame, frame, err)
			return
		}
		serverDone <- nil
	}))
	defer server.Close()

	hubURL := "ws" + strings.TrimPrefix(server.URL, "http")
	initial, _, err := websocket.Dial(ctx, hubURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeFrameToConn(ctx, initial, &protocol.Hello{
		ProtocolVersion: protocol.ProtocolVersionV2, Role: protocol.RoleAdapter,
		Token: "old-token", SessionID: "ses_official", Provider: "claude-code",
	}); err != nil {
		t.Fatal(err)
	}
	frame, err := readFrameFromConn(ctx, initial)
	ack, ok := frame.(*protocol.HelloAck)
	if err != nil || !ok {
		t.Fatalf("initial ack = %T %+v, %v", frame, frame, err)
	}
	cfg := wrapConfig{
		HubURL: hubURL, SessionID: "ses_official", Provider: "claude-code",
		AdapterToken: "old-token", ProtocolVersion: protocol.ProtocolVersionV2,
	}
	connection := newHubConnection(cfg, initial, ack.ConnectionAuthority)
	defer connection.close()
	writeFrame := func(frame protocol.Frame) error { return connection.write(ctx, frame) }
	rotation := newCredentialRotationManager(ctx, ack.ConnectionAuthority, writeFrame, connection.credentials, connection.currentAuthority)

	commandDone := make(chan error, 1)
	go func() {
		commandDone <- forwardHubCommandsToOfficialCLI(ctx, cfg, connection, writeFrame, nil, nil, nil, &atomic.Bool{}, nil, nil, nil, rotation)
	}()
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	cancel()
	select {
	case err := <-commandDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("official command loop did not stop")
	}
}

func TestRunOfficialProviderBootstrapsCredentialRotation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	binDir := t.TempDir()
	providerPath := filepath.Join(binDir, "claude")
	if err := os.WriteFile(providerPath, []byte("#!/bin/sh\nwhile :; do sleep 1; done\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("HOME", t.TempDir())

	activated := make(chan struct{}, 1)
	serverErr := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{CompressionMode: websocket.CompressionDisabled})
		if err != nil {
			serverErr <- err
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "")
		frame, err := readFrameFromConn(ctx, conn)
		hello, ok := frame.(*protocol.Hello)
		if err != nil || !ok || hello.ProtocolVersion != protocol.ProtocolVersionV2 {
			serverErr <- fmt.Errorf("official hello = %T %+v, %v", frame, frame, err)
			return
		}
		ack := reconnectHelloAck("ses_official_runtime", 1, 1)
		ack.ConnectionAuthority.ExpiresAt = time.Now().Add(time.Minute).UnixMilli()
		if err := writeFrameToConn(ctx, conn, ack); err != nil {
			serverErr <- err
			return
		}
		rotationID := ""
		for {
			frame, err = readFrameFromConn(ctx, conn)
			if err != nil {
				if ctx.Err() == nil {
					serverErr <- err
				}
				return
			}
			switch typed := frame.(type) {
			case *protocol.Ping:
				if err := writeFrameToConn(ctx, conn, &protocol.Pong{Nonce: typed.Nonce}); err != nil {
					serverErr <- err
					return
				}
			case *protocol.CredentialRotationRequest:
				rotationID = typed.RotationID
				if rotationID == "" {
					serverErr <- errors.New("official rotation ID is empty")
					return
				}
				if err := writeFrameToConn(ctx, conn, &protocol.CredentialRotationCredential{
					SessionID: "ses_official_runtime", RotationID: rotationID, Generation: 2,
					Credential: "new-token", ExpiresAt: time.Now().Add(15 * time.Minute).UnixMilli(),
				}); err != nil {
					serverErr <- err
					return
				}
			case *protocol.CredentialRotationPossession:
				if typed.RotationID != rotationID || typed.Generation != 2 {
					serverErr <- fmt.Errorf("official possession = %+v", typed)
					return
				}
				if err := writeFrameToConn(ctx, conn, &protocol.CredentialRotationActivation{
					RotationID: rotationID, Generation: 2, ConnectionEpoch: 2, AcceptedFence: 2,
				}); err != nil {
					serverErr <- err
					return
				}
				activated <- struct{}{}
			}
		}
	}))
	defer server.Close()

	hubURL := "ws" + strings.TrimPrefix(server.URL, "http")
	initial, _, err := websocket.Dial(ctx, hubURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeFrameToConn(ctx, initial, &protocol.Hello{
		ProtocolVersion: protocol.ProtocolVersionV2, Role: protocol.RoleAdapter,
		Token: "old-token", SessionID: "ses_official_runtime", Provider: "claude-code",
	}); err != nil {
		t.Fatal(err)
	}
	frame, err := readFrameFromConn(ctx, initial)
	ack, ok := frame.(*protocol.HelloAck)
	if err != nil || !ok {
		t.Fatalf("official ack = %T %+v, %v", frame, frame, err)
	}
	cfg := wrapConfig{
		HubURL: hubURL, SessionID: "ses_official_runtime", Agent: "claude", Provider: "claude-code",
		AdapterToken: "old-token", ProtocolVersion: protocol.ProtocolVersionV2,
		ProviderCommand: []string{"claude"}, WorkingDirectory: t.TempDir(), Stdin: strings.NewReader(""),
		Heartbeat: heartbeatConfig{Interval: 10 * time.Millisecond, Timeout: time.Second}, Stderr: io.Discard,
	}
	connection := newHubConnection(cfg, initial, ack.ConnectionAuthority)
	defer connection.close()
	done := make(chan error, 1)
	go func() {
		done <- runOfficialProvider(ctx, cfg, connection, core.NewEventMasker(nil), core.NewAdapterMetrics())
	}()

	select {
	case <-activated:
		cancel()
	case err := <-serverErr:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("official provider did not stop after rotation")
	}
}

func TestClaudeTranscriptPathUsesProjectSlug(t *testing.T) {
	path, err := claudeTranscriptPath("/Users/me/proj", "ses-123")
	if err != nil {
		t.Fatalf("claudeTranscriptPath() error = %v", err)
	}
	if !strings.HasSuffix(path, "ses-123.jsonl") {
		t.Fatalf("path = %q", path)
	}
	if !strings.Contains(path, "-Users-me-proj") {
		t.Fatalf("path = %q, want project slug", path)
	}
}

func TestResetTranscriptOffsetIfRewritten(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ses.jsonl")
	if err := os.WriteFile(path, []byte(strings.Repeat("a", 200)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	offset, err := resetTranscriptOffsetIfRewritten(path, 200)
	if err != nil || offset != 200 {
		t.Fatalf("offset = %d, err = %v, want unchanged 200", offset, err)
	}
	if err := os.WriteFile(path, []byte("short\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	offset, err = resetTranscriptOffsetIfRewritten(path, 200)
	if err != nil || offset != 0 {
		t.Fatalf("offset = %d, err = %v, want 0 after shrink", offset, err)
	}
}

func TestResetTranscriptTailCursorIfRewrittenDetectsSameSizeReplacement(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ses.jsonl")
	if err := os.WriteFile(path, []byte("first transcript line\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cursor, err := transcriptTailCursorAt(path, int64(len("first transcript line\n")))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("other transcript line\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	next, err := resetTranscriptTailCursorIfRewritten(path, cursor)
	if err != nil || next.offset != 0 {
		t.Fatalf("cursor = %+v, err = %v, want rewind after same-size replacement", next, err)
	}
}

func TestResetTranscriptTailCursorIfRewrittenKeepsAppendCursor(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ses.jsonl")
	first := "first transcript line\n"
	if err := os.WriteFile(path, []byte(first), 0o600); err != nil {
		t.Fatal(err)
	}
	cursor, err := transcriptTailCursorAt(path, int64(len(first)))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(first+"appended transcript line\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	next, err := resetTranscriptTailCursorIfRewritten(path, cursor)
	if err != nil || next.offset != cursor.offset {
		t.Fatalf("cursor = %+v, err = %v, want append cursor %d", next, err, cursor.offset)
	}
}

func TestAlreadyMirroredTranscriptLineSkipsRewindDuplicates(t *testing.T) {
	seen := make(map[string]struct{})
	first := []byte(`{"type":"assistant","uuid":"a1","message":{"role":"assistant","content":"hi"}}`)
	if alreadyMirroredTranscriptLine(seen, first) {
		t.Fatal("first line should be new")
	}
	if !alreadyMirroredTranscriptLine(seen, first) {
		t.Fatal("same uuid should be skipped after rewind")
	}
	second := []byte(`{"type":"assistant","uuid":"a2","message":{"role":"assistant","content":"later"}}`)
	if alreadyMirroredTranscriptLine(seen, second) {
		t.Fatal("new uuid should not be skipped")
	}
}

func TestClaudeLaunchSettings(t *testing.T) {
	settings := claudeProvider{}.launchSettings([]string{
		"--model", "sonnet", "--permission-mode", "acceptEdits", "--reasoning-effort", "high",
	})
	if settings.model != "sonnet" || settings.permission != "acceptEdits" || settings.reasoning != "high" {
		t.Fatalf("parsed = %+v", settings)
	}

	settings = claudeProvider{}.launchSettings([]string{"--model=opus", "--permission-mode=default"})
	if settings.model != "opus" || settings.permission != "default" || settings.reasoning != "" {
		t.Fatalf("parsed = %+v", settings)
	}
}

func TestCodexLaunchSettings(t *testing.T) {
	settings := codexProvider{}.launchSettings([]string{
		"-m", "gpt-5", "-a", "never", "-c", "reasoning_effort=high",
	})
	if settings.model != "gpt-5" || settings.permission != "never" || settings.reasoning != "high" {
		t.Fatalf("parsed = %+v", settings)
	}

	settings = codexProvider{}.launchSettings([]string{"-s", "read-only", "-c", "approval_policy=on-request"})
	if settings.permission != "on-request" {
		t.Fatalf("parsed permission = %+v", settings)
	}
}

func TestPublishOfficialSettingsCapability(t *testing.T) {
	var frames []protocol.Event
	write := func(frame protocol.Frame) error {
		if event, ok := frame.(*protocol.Event); ok {
			frames = append(frames, *event)
		}
		return nil
	}
	settings := launchSettings{model: "sonnet", permission: "acceptEdits"}
	if err := publishOfficialSettingsCapability(write, "ses_1", settings); err != nil {
		t.Fatalf("publishOfficialSettingsCapability() error = %v", err)
	}
	if len(frames) != 1 || frames[0].Type != "session.settings.capabilities" {
		t.Fatalf("frames = %+v", frames)
	}
	if _, err := protocol.DecodeSettingsCapabilityPayload(frames[0].Payload); err != nil {
		t.Fatalf("capability payload is not canonical: %v", err)
	}
	var wire struct {
		ReasoningEfforts []json.RawMessage `json:"reasoning_efforts"`
	}
	if err := json.Unmarshal(frames[0].Payload, &wire); err != nil {
		t.Fatalf("decode capability wire payload: %v", err)
	}
	if wire.ReasoningEfforts == nil {
		t.Fatalf("reasoning_efforts = null, want []")
	}
}

func TestPublishOfficialRunControlCapabilityIsStopOnly(t *testing.T) {
	frames := make([]protocol.Frame, 0, 1)
	if err := publishOfficialRunControlCapability(wrapConfig{
		ProtocolVersion: protocol.ProtocolVersionV2,
		SessionID:       "ses_official_control",
	}, func(frame protocol.Frame) error {
		frames = append(frames, frame)
		return nil
	}); err != nil {
		t.Fatalf("publishOfficialRunControlCapability() error = %v", err)
	}
	if len(frames) != 1 {
		t.Fatalf("frames = %d, want 1", len(frames))
	}
	event, ok := frames[0].(*protocol.Event)
	if !ok || event.Type != "session.run.capabilities" || event.ProposalID == "" {
		t.Fatalf("capability frame = %#v", frames[0])
	}
	capability, err := protocol.DecodeRunControlCapabilityPayload(event.Payload)
	if err != nil {
		t.Fatalf("decode capability: %v", err)
	}
	if capability.InterruptSupported || !capability.StopSupported {
		t.Fatalf("capability = %+v, want stop only", capability)
	}
}

func TestStopOfficialCLIReportsEndedOutcome(t *testing.T) {
	child := exec.Command(os.Args[0], "-test.run=^TestOfficialStopHelperProcess$")
	child.Env = append(os.Environ(), "AGENTWHARF_OFFICIAL_STOP_HELPER=1")
	if err := child.Start(); err != nil {
		t.Fatalf("start helper: %v", err)
	}
	frames := make([]protocol.Frame, 0, 2)
	write := func(frame protocol.Frame) error {
		frames = append(frames, frame)
		return nil
	}
	read := func(context.Context) (protocol.Frame, error) {
		outcome, ok := frames[len(frames)-1].(*protocol.Event)
		if !ok {
			return nil, io.ErrUnexpectedEOF
		}
		return &protocol.EventReceipt{ProposalID: outcome.ProposalID, Seq: 1, Status: protocol.EventReceiptAccepted}, nil
	}
	command := &protocol.Command{CommandID: "cmd_official_stop", Type: protocol.CommandSessionStop, SessionID: "ses_official_stop"}
	var stopInProgress atomic.Bool
	if err := stopOfficialCLI(command, read, write, wrapConfig{
		ProtocolVersion: protocol.ProtocolVersionV2,
		SessionID:       command.SessionID,
	}, child.Process, &stopInProgress); err != nil {
		t.Fatalf("stopOfficialCLI() error = %v", err)
	}
	_ = child.Wait()
	if !stopInProgress.Load() {
		t.Fatal("stop should be recorded after the child process is terminated")
	}
	if len(frames) != 2 {
		t.Fatalf("frames = %d, want ack and outcome", len(frames))
	}
	if ack, ok := frames[0].(*protocol.CommandAck); !ok || ack.CommandID != command.CommandID || ack.Status != protocol.AckAccepted {
		t.Fatalf("ack = %#v", frames[0])
	}
	outcome, ok := frames[1].(*protocol.Event)
	if !ok || outcome.Type != "session.run.outcome" {
		t.Fatalf("outcome = %#v", frames[1])
	}
	decoded, err := protocol.DecodeRunControlOutcomePayload(outcome.Payload)
	if err != nil {
		t.Fatalf("decode outcome: %v", err)
	}
	if decoded.CommandID != command.CommandID || decoded.Operation != "stop" || decoded.Outcome != "completed" || decoded.CompletionState == nil || *decoded.CompletionState != "ended" {
		t.Fatalf("outcome = %+v", decoded)
	}
}

func TestOfficialStopHelperProcess(t *testing.T) {
	if os.Getenv("AGENTWHARF_OFFICIAL_STOP_HELPER") != "1" {
		return
	}
	select {}
}

func TestInjectedPromptTrackerDedupes(t *testing.T) {
	tracker := &injectedPromptTracker{pending: make(map[string]int)}
	tracker.add("hello")
	if !tracker.consume("hello") {
		t.Fatal("consume(hello) = false, want true")
	}
	if tracker.consume("hello") {
		t.Fatal("second consume(hello) = true, want false")
	}
	if tracker.consume("other") {
		t.Fatal("consume(other) = true, want false")
	}
}

func TestInjectedPromptTrackerRemovesFailedWrite(t *testing.T) {
	tracker := &injectedPromptTracker{pending: make(map[string]int)}
	tracker.add("hello")
	tracker.remove("hello")
	if tracker.consume("hello") {
		t.Fatal("consume(hello) = true after remove, want false")
	}
}

func TestInjectedPromptTrackerHasPending(t *testing.T) {
	tracker := &injectedPromptTracker{pending: make(map[string]int)}
	if tracker.hasPending("hello") {
		t.Fatal("hasPending on empty tracker")
	}
	tracker.add("hello")
	if !tracker.hasPending("hello") {
		t.Fatal("hasPending after add = false")
	}
	tracker.consume("hello")
	if tracker.hasPending("hello") {
		t.Fatal("hasPending after consume = true")
	}
}

func TestConfirmOfficialCLIPromptRetriesAfterIdleMiss(t *testing.T) {
	tracker := &injectedPromptTracker{pending: make(map[string]int)}
	tracker.add("wake up")
	var buf bytes.Buffer
	var mu sync.Mutex
	go func() {
		time.Sleep(80 * time.Millisecond)
		tracker.consume("wake up")
	}()
	confirmOfficialCLIPrompt(context.Background(), &buf, &mu, "ses_1", "wake up", tracker, func(protocol.Frame) error {
		t.Fatal("unexpected miss event")
		return nil
	}, officialPromptInjection{firstWait: 30 * time.Millisecond, retryWait: 200 * time.Millisecond, wakePause: 0})
	if tracker.hasPending("wake up") {
		t.Fatal("prompt still pending after delayed consume")
	}
	if !strings.Contains(buf.String(), "\x1b[200~wake up\x1b[201~\r") {
		t.Fatalf("retry write = %q, want bracketed-paste resubmit", buf.String())
	}
	if !strings.Contains(buf.String(), "\x1b") || !strings.Contains(buf.String(), "\x15") {
		t.Fatalf("retry write = %q, want ESC and Ctrl+U wakeup", buf.String())
	}
}

func TestConfirmOfficialCLIPromptPublishesMissWhenTUIIgnoresPrompt(t *testing.T) {
	tracker := &injectedPromptTracker{pending: make(map[string]int)}
	tracker.add("ignored")
	var buf bytes.Buffer
	var frames []protocol.Frame
	confirmOfficialCLIPrompt(context.Background(), &buf, nil, "ses_1", "ignored", tracker, func(frame protocol.Frame) error {
		frames = append(frames, frame)
		return nil
	}, officialPromptInjection{firstWait: 20 * time.Millisecond, retryWait: 20 * time.Millisecond, wakePause: 0})
	if len(frames) != 1 {
		t.Fatalf("frames = %#v, want one miss message", frames)
	}
	event, ok := frames[0].(*protocol.Event)
	if !ok || event.Type != "session.message" || event.SessionID != "ses_1" {
		t.Fatalf("frame = %#v", frames[0])
	}
	if !strings.Contains(string(event.Payload), "did not accept this instruction") && !strings.Contains(string(event.Payload), "没有收下") {
		t.Fatalf("payload = %s", event.Payload)
	}
	if strings.Count(buf.String(), "\x1b[200~ignored\x1b[201~\r") != 2 {
		t.Fatalf("retry writes = %q, want two bracketed-paste resubmits", buf.String())
	}
}

func TestWriteOfficialCLIPromptUsesBracketedPaste(t *testing.T) {
	var buf bytes.Buffer
	if err := writeOfficialCLIPrompt(&buf, "你好"); err != nil {
		t.Fatal(err)
	}
	if buf.String() != "\x1b[200~你好\x1b[201~\r" {
		t.Fatalf("write = %q", buf.String())
	}
}

func TestCopyLockedSerializesWithConcurrentWriter(t *testing.T) {
	pr, pw := io.Pipe()
	var mu sync.Mutex
	var dst bytes.Buffer
	var dstMu sync.Mutex
	writer := writerFunc(func(p []byte) (int, error) {
		dstMu.Lock()
		n, err := dst.Write(p)
		dstMu.Unlock()
		return n, err
	})
	done := make(chan error, 1)
	go func() { done <- copyLocked(writer, pr, &mu) }()
	mu.Lock()
	if _, err := writer.Write([]byte("HUB")); err != nil {
		t.Fatal(err)
	}
	if _, err := pw.Write([]byte("local")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	dstMu.Lock()
	if dst.String() != "HUB" {
		t.Fatalf("interleaved write = %q, want HUB while lock held", dst.String())
	}
	dstMu.Unlock()
	mu.Unlock()
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil && err != io.EOF {
		t.Fatalf("copyLocked: %v", err)
	}
	if dst.String() != "HUBlocal" {
		t.Fatalf("copied = %q, want HUBlocal", dst.String())
	}
}

type writerFunc func([]byte) (int, error)

func (w writerFunc) Write(p []byte) (int, error) { return w(p) }

func TestTranscriptUserText(t *testing.T) {
	line := transcriptJSON(t, map[string]any{
		"type": "user", "uuid": "u1",
		"message": map[string]any{"role": "user", "content": "hello"},
	})
	if got := (claudeProvider{}).transcriptUserText(line); got != "hello" {
		t.Fatalf("transcriptUserText = %q, want hello", got)
	}
	assistant := transcriptJSON(t, map[string]any{
		"type": "assistant", "uuid": "a1",
		"message": map[string]any{"role": "assistant", "content": "hi"},
	})
	if got := (claudeProvider{}).transcriptUserText(assistant); got != "" {
		t.Fatalf("transcriptUserText(assistant) = %q, want empty", got)
	}
	blocks := transcriptJSON(t, map[string]any{
		"type": "user", "uuid": "u2",
		"message": map[string]any{"role": "user", "content": []any{
			map[string]any{"type": "text", "text": "modern "},
			map[string]any{"type": "image", "source": "ignored"},
			map[string]any{"type": "text", "text": "prompt"},
		}},
	})
	if got := (claudeProvider{}).transcriptUserText(blocks); got != "modern prompt" {
		t.Fatalf("transcriptUserText(blocks) = %q, want modern prompt", got)
	}
}

func TestCodexTranslateLine(t *testing.T) {
	user := transcriptJSON(t, map[string]any{
		"timestamp": "2025-01-01T00:00:00Z",
		"type":      "event_msg",
		"payload":   map[string]any{"type": "user_message", "message": "hello"},
	})
	events, err := codexProvider{}.translateLine("ses_1", user)
	if err != nil || len(events) != 1 || events[0].Type != "session.message" {
		t.Fatalf("user events = %+v, %v", events, err)
	}

	currentUser := transcriptJSON(t, map[string]any{
		"type": "event_msg",
		"payload": map[string]any{"type": "item_completed", "item": map[string]any{
			"type":    "UserMessage",
			"content": []any{map[string]any{"type": "text", "text": "current hello"}},
		}},
	})
	events, err = codexProvider{}.translateLine("ses_1", currentUser)
	if err != nil || len(events) != 1 || events[0].Type != "session.message" {
		t.Fatalf("current user events = %+v, %v", events, err)
	}
	if got := (codexProvider{}).transcriptUserText(currentUser); got != "current hello" {
		t.Fatalf("current codex transcriptUserText = %q", got)
	}

	currentAgent := transcriptJSON(t, map[string]any{
		"type": "event_msg",
		"payload": map[string]any{"type": "item_completed", "item": map[string]any{
			"type":    "AgentMessage",
			"content": []any{map[string]any{"type": "Text", "text": "current done"}},
		}},
	})
	events, err = codexProvider{}.translateLine("ses_1", currentAgent)
	if err != nil || len(events) != 1 || events[0].Type != "session.message" {
		t.Fatalf("current agent events = %+v, %v", events, err)
	}

	commentary := transcriptJSON(t, map[string]any{
		"type":    "event_msg",
		"payload": map[string]any{"type": "agent_message", "phase": "commentary", "message": "thinking"},
	})
	events, err = codexProvider{}.translateLine("ses_1", commentary)
	if err != nil || len(events) != 0 {
		t.Fatalf("commentary events = %+v, %v", events, err)
	}

	agent := transcriptJSON(t, map[string]any{
		"type":    "event_msg",
		"payload": map[string]any{"type": "agent_message", "message": "done"},
	})
	events, err = codexProvider{}.translateLine("ses_1", agent)
	if err != nil || len(events) != 1 || events[0].Type != "session.message" {
		t.Fatalf("agent events = %+v, %v", events, err)
	}

	tool := transcriptJSON(t, map[string]any{
		"type":    "response_item",
		"payload": map[string]any{"type": "function_call", "name": "shell", "call_id": "c1", "arguments": "{}"},
	})
	events, err = codexProvider{}.translateLine("ses_1", tool)
	if err != nil || len(events) != 1 || events[0].Type != "session.tool_call" {
		t.Fatalf("tool events = %+v, %v", events, err)
	}
}

func TestCodexTranscriptUserText(t *testing.T) {
	user := transcriptJSON(t, map[string]any{
		"type":    "event_msg",
		"payload": map[string]any{"type": "user_message", "message": "hello"},
	})
	if got := (codexProvider{}).transcriptUserText(user); got != "hello" {
		t.Fatalf("codex transcriptUserText = %q, want hello", got)
	}
	sessionMeta := transcriptJSON(t, map[string]any{
		"type":    "session_meta",
		"payload": map[string]any{"session_id": "s1"},
	})
	if got := (codexProvider{}).transcriptUserText(sessionMeta); got != "" {
		t.Fatalf("codex transcriptUserText(session_meta) = %q, want empty", got)
	}
}

func TestNewestCodexRollout(t *testing.T) {
	dir := t.TempDir()
	sub := filepath.Join(dir, "2025", "01", "02")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	old := filepath.Join(sub, "rollout-old.jsonl")
	recent := filepath.Join(sub, "rollout-recent.jsonl")
	for _, p := range []string{old, recent} {
		if err := os.WriteFile(p, []byte("{}"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	base := time.Now().Add(-time.Hour)
	if err := os.Chtimes(old, base, base); err != nil {
		t.Fatal(err)
	}
	recentTime := base.Add(30 * time.Minute)
	if err := os.Chtimes(recent, recentTime, recentTime); err != nil {
		t.Fatal(err)
	}
	got, found, err := newestCodexRollout(dir, base.Add(-time.Minute))
	if err != nil || !found {
		t.Fatalf("newestCodexRollout = %q, %t, %v", got, found, err)
	}
	if got != recent {
		t.Fatalf("got %q, want %q", got, recent)
	}
}
