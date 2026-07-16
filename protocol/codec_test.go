package protocol

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestDecodeClientHelloExample(t *testing.T) {
	raw := []byte(`{
		"frame": "hello",
		"protocol_version": 1,
		"role": "client",
		"token": "<scope-bound token>",
		"subscriptions": [
			{ "session_id": "ses_01H8X", "last_seq": 41 }
		]
	}`)

	frame, err := Decode(raw)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	hello, ok := frame.(*Hello)
	if !ok {
		t.Fatalf("Decode() = %T, want *Hello", frame)
	}
	if hello.ProtocolVersion != ProtocolVersion || hello.Role != RoleClient {
		t.Fatalf("hello version/role = %d/%q", hello.ProtocolVersion, hello.Role)
	}
	if got := hello.Subscriptions[0].LastSeq; got != 41 {
		t.Fatalf("last_seq = %d, want 41", got)
	}
}

func TestDecodeAdapterHelloExample(t *testing.T) {
	raw := []byte(`{
		"frame": "hello",
		"protocol_version": 1,
		"role": "adapter",
		"token": "<session-bound adapter token>",
		"session_id": "ses_01H8X",
		"provider": "claude-code",
		"resume": true
	}`)

	frame, err := Decode(raw)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	hello := frame.(*Hello)
	if hello.Role != RoleAdapter || hello.SessionID != "ses_01H8X" || !hello.Resume {
		t.Fatalf("adapter hello = %+v", hello)
	}
}

func TestEncodeHelloAckExample(t *testing.T) {
	encoded, err := Encode(&HelloAck{
		ProtocolVersion: ProtocolVersion,
		Sessions: []SessionSummary{{
			SessionID:  "ses_01H8X",
			State:      "ready",
			Provider:   "claude-code",
			LatestSeq:  57,
			ReplayFrom: 42,
		}},
	})
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatalf("encoded JSON invalid: %v", err)
	}
	if got["frame"] != string(FrameHelloAck) {
		t.Fatalf("frame = %v, want %q", got["frame"], FrameHelloAck)
	}
	if got["protocol_version"].(float64) != 1 {
		t.Fatalf("protocol_version = %v, want 1", got["protocol_version"])
	}
}

func TestDecodeDurableAndEphemeralEvents(t *testing.T) {
	durableRaw := []byte(`{
		"frame": "event",
		"type": "session.message",
		"session_id": "ses_01H8X",
		"seq": 42,
		"time": 1764937200123,
		"payload": {"message_id":"msg_01H8Y","role":"user","content":[{"kind":"text","text":"Continue"}]}
	}`)
	ephemeralRaw := []byte(`{
		"frame": "event",
		"type": "log.tail",
		"session_id": "ses_01H8X",
		"time": 1764937200123,
		"payload": {}
	}`)

	durable, err := Decode(durableRaw)
	if err != nil {
		t.Fatalf("Decode(durable) error = %v", err)
	}
	ev := durable.(*Event)
	if !ev.Durable() || ev.Seq == nil || *ev.Seq != 42 {
		t.Fatalf("durable event seq = %v", ev.Seq)
	}

	ephemeral, err := Decode(ephemeralRaw)
	if err != nil {
		t.Fatalf("Decode(ephemeral) error = %v", err)
	}
	if ev := ephemeral.(*Event); ev.Durable() {
		t.Fatalf("log.tail should be ephemeral: %+v", ev)
	}
}

func TestUnknownEventTypeIsDecodedForForwardCompatibility(t *testing.T) {
	raw := []byte(`{
		"frame": "event",
		"type": "x.future",
		"session_id": "ses_01H8X",
		"seq": 99,
		"time": 1764937200123,
		"payload": {"unknown": true}
	}`)

	frame, err := Decode(raw)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	ev := frame.(*Event)
	if ev.Type != "x.future" {
		t.Fatalf("type = %q, want x.future", ev.Type)
	}
	if string(ev.Payload) != `{"unknown":true}` {
		t.Fatalf("payload = %s", ev.Payload)
	}
}

func TestDecodeCommandAndAckExamples(t *testing.T) {
	commandRaw := []byte(`{
		"frame": "command",
		"cmd_id": "cmd_01H91",
		"type": "session.send",
		"session_id": "ses_01H8X",
		"payload": {
			"content": [{ "kind": "text", "text": "Continue" }]
		}
	}`)
	ackRaw := []byte(`{
		"frame": "command.ack",
		"cmd_id": "cmd_01H91",
		"status": "accepted",
		"reason": ""
	}`)

	cmdFrame, err := Decode(commandRaw)
	if err != nil {
		t.Fatalf("Decode(command) error = %v", err)
	}
	cmd := cmdFrame.(*Command)
	if cmd.Type != CommandSessionSend || cmd.CommandID != "cmd_01H91" {
		t.Fatalf("command = %+v", cmd)
	}

	ackFrame, err := Decode(ackRaw)
	if err != nil {
		t.Fatalf("Decode(command.ack) error = %v", err)
	}
	ack := ackFrame.(*CommandAck)
	if ack.Status != AckAccepted {
		t.Fatalf("ack status = %q, want accepted", ack.Status)
	}
}

func TestDecodeRejectsUnknownFrame(t *testing.T) {
	_, err := Decode([]byte(`{"frame":"future.frame"}`))
	if !errors.Is(err, ErrUnknownFrame) {
		t.Fatalf("Decode() error = %v, want ErrUnknownFrame", err)
	}
}

func TestNegotiateVersion(t *testing.T) {
	got, err := NegotiateVersion([]int{3, 2, 1}, []int{1})
	if err != nil {
		t.Fatalf("NegotiateVersion() error = %v", err)
	}
	if got != ProtocolVersion {
		t.Fatalf("version = %d, want %d", got, ProtocolVersion)
	}
	if _, err := NegotiateVersion([]int{2}, []int{1}); !errors.Is(err, ErrNoCompatibleVersion) {
		t.Fatalf("NegotiateVersion() error = %v, want ErrNoCompatibleVersion", err)
	}
}

func TestNegotiateHighestVersion(t *testing.T) {
	for _, test := range []struct {
		name             string
		peerHighest      int
		hubHighest       int
		want             int
		wantIncompatible bool
	}{
		{name: "v2", peerHighest: 2, hubHighest: 2, want: 2},
		{name: "client fallback", peerHighest: 2, hubHighest: 1, want: 1},
		{name: "hub fallback", peerHighest: 1, hubHighest: 2, want: 1},
		{name: "invalid peer", peerHighest: 0, hubHighest: 2, wantIncompatible: true},
		{name: "invalid hub", peerHighest: 2, hubHighest: 0, wantIncompatible: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := NegotiateHighestVersion(test.peerHighest, test.hubHighest)
			if test.wantIncompatible {
				if !errors.Is(err, ErrNoCompatibleVersion) {
					t.Fatalf("NegotiateHighestVersion() error = %v, want ErrNoCompatibleVersion", err)
				}
				return
			}
			if err != nil || got != test.want {
				t.Fatalf("NegotiateHighestVersion() = %d, %v; want %d, nil", got, err, test.want)
			}
		})
	}
}

func TestHelloAckOmitsUnavailableCapabilities(t *testing.T) {
	encoded, err := Encode(&HelloAck{ProtocolVersion: ProtocolVersionV2, Sessions: []SessionSummary{}})
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatalf("decode ack: %v", err)
	}
	if _, advertised := fields["capabilities"]; advertised {
		t.Fatalf("unavailable capabilities advertised: %s", encoded)
	}
}

func TestEventTypeAllowed(t *testing.T) {
	for _, test := range []struct {
		version   int
		eventType string
		want      bool
	}{
		{version: ProtocolVersion, eventType: "session.idle_warning", want: true},
		{version: ProtocolVersionV2, eventType: "session.idle_warning", want: false},
		{version: ProtocolVersion, eventType: "x.vm.idle_warning", want: false},
		{version: ProtocolVersionV2, eventType: "x.vm.idle_warning", want: true},
		{version: ProtocolVersionV2, eventType: "session.message", want: true},
	} {
		if got := EventTypeAllowed(test.version, test.eventType); got != test.want {
			t.Fatalf("EventTypeAllowed(%d, %q) = %v, want %v", test.version, test.eventType, got, test.want)
		}
	}
}
