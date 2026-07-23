package protocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
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

func TestDecodeFileReferenceSendPayload(t *testing.T) {
	maxBytes := int64(10485760)
	reason := "provider_unsupported"
	capability := FileReferenceCapabilityPayload{
		SchemaVersion: 1, MaxReferences: 8, MaxTotalBytes: 10485760,
		File:  FileReferenceDispositionCapability{Mode: "allowed", MaxBytes: &maxBytes},
		Image: FileReferenceImageCapability{Mode: "unsupported", MediaTypes: []string{}, Reason: &reason},
	}
	capability.Fingerprint = FileReferenceCapabilityFingerprint(capability)
	valid := []byte(`{"content":[{"kind":"text","text":"Review this"},{"kind":"file_reference","disposition":"file","path":"src/app.ts","version":"version_1","content_digest":"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","bytes":123,"media_type":"text/plain"}],"capability_fingerprint":"` + capability.Fingerprint + `"}`)
	decoded, err := DecodeFileReferenceSendPayload(valid)
	if err != nil || !decoded.HasReferences || decoded.ReferenceCount != 1 || decoded.CapabilityFingerprint != capability.Fingerprint || decoded.RequestFingerprint == "" {
		t.Fatalf("DecodeFileReferenceSendPayload(valid) = %+v, %v", decoded, err)
	}
	reordered := []byte(`{"capability_fingerprint":"` + capability.Fingerprint + `","content":[{"text":"Review this","kind":"text"},{"media_type":"text/plain","bytes":123,"content_digest":"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","version":"version_1","path":"src/app.ts","disposition":"file","kind":"file_reference"}]}`)
	if got, err := DecodeFileReferenceSendPayload(reordered); err != nil || got.RequestFingerprint != decoded.RequestFingerprint {
		t.Fatalf("canonical file-reference request = %+v, %v", got, err)
	}
	for _, raw := range [][]byte{
		[]byte(`{"content":[{"kind":"file_reference","disposition":"file","path":"../secret","version":"version_1","content_digest":"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","bytes":123,"media_type":"text/plain"}],"capability_fingerprint":"` + capability.Fingerprint + `"}`),
		[]byte(`{"content":[{"kind":"file_reference","disposition":"file","path":"src/app.ts","version":"version_1","content_digest":"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","bytes":123,"media_type":"text/plain","url":"https://invalid"}],"capability_fingerprint":"` + capability.Fingerprint + `"}`),
		[]byte(`{"content":[{"kind":"file_reference","disposition":"file","path":"src/app.ts","version":"version_1","content_digest":"sha256:ABCDEF","bytes":123,"media_type":"text/plain"}],"capability_fingerprint":"` + capability.Fingerprint + `"}`),
		[]byte(`{"content":[{"kind":"file_reference","disposition":"file","path":"src/app.ts","version":"version_1","content_digest":"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","bytes":10485761,"media_type":"text/plain"}],"capability_fingerprint":"` + capability.Fingerprint + `"}`),
	} {
		if _, err := DecodeFileReferenceSendPayload(raw); err == nil {
			t.Fatalf("DecodeFileReferenceSendPayload(%s) unexpectedly succeeded", raw)
		}
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

func TestHelloAckConnectionAuthorityReceiptIsMinimalAndRoundTrips(t *testing.T) {
	ack := &HelloAck{ProtocolVersion: ProtocolVersionV2, Sessions: []SessionSummary{}, ConnectionAuthority: &ConnectionAuthorityReceipt{
		SessionID: "ses_01H8X", ConnectionEpoch: 7, CredentialGeneration: 3, AcceptedFence: 11,
		WriterLeaseID: "opaque-lease", ExpiresAt: 1764937800000,
	}}
	encoded, err := Encode(ack)
	if err != nil {
		t.Fatalf("Encode(hello.ack) error = %v", err)
	}
	for _, forbidden := range []string{"token", "bearer", "credential", "provider", "path", "content", "summary", "bytes"} {
		if bytes.Contains(encoded, []byte(`\"`+forbidden+`\"`)) {
			t.Fatalf("connection authority payload contains forbidden field %q: %s", forbidden, encoded)
		}
	}
	frame, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode(hello.ack) error = %v", err)
	}
	got := frame.(*HelloAck).ConnectionAuthority
	if got == nil || *got != *ack.ConnectionAuthority {
		t.Fatalf("connection authority receipt = %+v, want %+v", got, ack.ConnectionAuthority)
	}
}

func TestProviderStartFramesAreStrictAndReferenceOnly(t *testing.T) {
	for _, frame := range []Frame{&ProviderStart{}, &ProviderStartPrepare{}, &ProviderStartStarted{}, &ProviderStartAck{Status: ProviderStartAdmitted}, &ProviderStartAck{Status: ProviderStartRejected}} {
		encoded, err := Encode(frame)
		if err != nil {
			t.Fatalf("Encode(%T): %v", frame, err)
		}
		if _, err := Decode(encoded); err != nil {
			t.Fatalf("Decode(%T): %v", frame, err)
		}
	}
	for _, raw := range []string{
		`{"frame":"provider.start","workspace_key":"forbidden"}`,
		`{"frame":"provider.start.prepare","lease_id":"forbidden"}`,
		`{"frame":"provider.start.started","provider":"forbidden"}`,
		`{"frame":"provider.start.ack","status":"admitted","provider":"forbidden"}`,
		`{"frame":"provider.start.ack","status":"unknown"}`,
	} {
		if _, err := Decode([]byte(raw)); err == nil {
			t.Fatalf("Decode(%s) accepted non-minimal provider-start frame", raw)
		}
	}
}

func TestSettingsCommandRejectsNonCanonicalPayloads(t *testing.T) {
	valid := `{"frame":"command","cmd_id":"cmd_settings_1","type":"session.settings.change","session_id":"ses_1","payload":{"capability_fingerprint":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","model_id":"reasoning"}}`
	if _, err := Decode([]byte(valid)); err != nil {
		t.Fatalf("Decode(valid settings command) error = %v", err)
	}
	for _, raw := range []string{
		`{"frame":"command","cmd_id":"cmd_settings_1","type":"session.settings.change","session_id":"ses_1","payload":{"capability_fingerprint":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}`,
		`{"frame":"command","cmd_id":"cmd_settings_1","type":"session.settings.change","session_id":"ses_1","payload":{"capability_fingerprint":"sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","model_id":"reasoning"}}`,
		`{"frame":"command","cmd_id":"cmd_settings_1","type":"session.settings.change","session_id":"ses_1","payload":{"capability_fingerprint":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","model_id":"reasoning","extra":true}}`,
		`{"frame":"command","cmd_id":"cmd_settings_1","type":"session.settings.change","session_id":"ses_1","payload":{"capability_fingerprint":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","model_id":"reasoning","model_id":"balanced"}}`,
		`{"frame":"command","cmd_id":"cmd_settings_1","type":"session.settings.change","session_id":"ses_1","payload":{"capability_fingerprint":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","model_id":"reasoning"},"extra":true}`,
	} {
		if _, err := Decode([]byte(raw)); err == nil {
			t.Fatalf("Decode(%s) unexpectedly succeeded", raw)
		}
	}
}

func TestSettingsCapabilityPayloadRejectsNonCanonicalChoices(t *testing.T) {
	for _, raw := range []string{
		`{"schema_version":1,"fingerprint":"sha256:a77186c8bf756736dc64be46864c21e4b10fd8ad8d719abf2e00dfa51c341000","models":[{"id":"balanced","label":"Balanced","provider":"hidden"},{"id":"reasoning","label":"Reasoning"}],"permission_modes":[{"id":"ask","label":"Ask first"},{"id":"workspace","label":"Workspace"}],"effective_model_id":"balanced","effective_permission_mode_id":"ask","model_change":"allowed","permission_change":"allowed","model_read_only_reason":null,"permission_read_only_reason":null}`,
		`{"schema_version":1,"fingerprint":"sha256:a77186c8bf756736dc64be46864c21e4b10fd8ad8d719abf2e00dfa51c341000","models":[{"id":"balanced","id":"balanced","label":"Balanced"},{"id":"reasoning","label":"Reasoning"}],"permission_modes":[{"id":"ask","label":"Ask first"},{"id":"workspace","label":"Workspace"}],"effective_model_id":"balanced","effective_permission_mode_id":"ask","model_change":"allowed","permission_change":"allowed","model_read_only_reason":null,"permission_read_only_reason":null}`,
		`{"schema_version":1,"fingerprint":"sha256:a77186c8bf756736dc64be46864c21e4b10fd8ad8d719abf2e00dfa51c341000","models":[{"id":"balanced","label":"Balanced"},{"id":"reasoning","label":"Reasoning"}],"permission_modes":[{"id":"ask","label":"Ask first","extra":true},{"id":"workspace","label":"Workspace"}],"effective_model_id":"balanced","effective_permission_mode_id":"ask","model_change":"allowed","permission_change":"allowed","model_read_only_reason":null,"permission_read_only_reason":null}`,
	} {
		if _, err := DecodeSettingsCapabilityPayload([]byte(raw)); err == nil {
			t.Fatalf("DecodeSettingsCapabilityPayload(%s) unexpectedly succeeded", raw)
		}
	}
}

func TestSettingsDeliveryExecuteFrameIsStrictAndV2Bounded(t *testing.T) {
	valid := `{"frame":"settings.delivery.execute","session_id":"ses_1","cmd_id":"cmd_settings_1","reservation_version":7,"operation_timeout_ms":30000}`
	frame, err := Decode([]byte(valid))
	if err != nil {
		t.Fatalf("Decode(valid settings delivery) error = %v", err)
	}
	if frame.FrameName() != FrameName("settings.delivery.execute") {
		t.Fatalf("decoded frame = %q", frame.FrameName())
	}
	for _, raw := range []string{
		`{"frame":"settings.delivery.execute","session_id":"ses_1","cmd_id":"cmd_settings_1","reservation_version":0,"operation_timeout_ms":30000}`,
		`{"frame":"settings.delivery.execute","session_id":"ses_1","cmd_id":"cmd_settings_1","reservation_version":7,"operation_timeout_ms":1}`,
		`{"frame":"settings.delivery.execute","session_id":"ses_1","cmd_id":"cmd_settings_1","reservation_version":7,"operation_timeout_ms":30000,"extra":true}`,
		`{"frame":"settings.delivery.execute","session_id":"ses_1","cmd_id":"cmd_settings_1","reservation_version":7,"reservation_version":8,"operation_timeout_ms":30000}`,
	} {
		if _, err := Decode([]byte(raw)); err == nil {
			t.Fatalf("Decode(%s) unexpectedly succeeded", raw)
		}
	}
}

func TestSettingsEffectivePayloadRejectsNonCanonicalPayloads(t *testing.T) {
	valid := []byte(`{"cmd_id":"cmd_settings_1","request_fingerprint":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","effective_fingerprint":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","outcome":"applied","effective_model_id":"reasoning","effective_permission_mode_id":"ask","reason_code":null}`)
	if _, err := DecodeSettingsEffectivePayload(valid); err != nil {
		t.Fatalf("DecodeSettingsEffectivePayload(valid) error = %v", err)
	}
	for _, raw := range []string{
		`{"cmd_id":"cmd_settings_1","request_fingerprint":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","effective_fingerprint":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","outcome":"applied","effective_model_id":"reasoning","effective_permission_mode_id":"ask","reason_code":"unexpected"}`,
		`{"cmd_id":"cmd_settings_1","request_fingerprint":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","effective_fingerprint":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","outcome":"rejected","effective_model_id":"reasoning","effective_permission_mode_id":"ask","reason_code":null}`,
		`{"cmd_id":"cmd_settings_1","request_fingerprint":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","effective_fingerprint":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","outcome":"applied","effective_model_id":"reasoning","effective_permission_mode_id":"ask","reason_code":null,"extra":true}`,
		`{"cmd_id":"cmd_settings_1","request_fingerprint":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","effective_fingerprint":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","outcome":"applied","effective_model_id":"reasoning","effective_permission_mode_id":"ask","reason_code":null,"reason_code":null}`,
	} {
		if _, err := DecodeSettingsEffectivePayload([]byte(raw)); err == nil {
			t.Fatalf("DecodeSettingsEffectivePayload(%s) unexpectedly succeeded", raw)
		}
	}
}

func TestRunControlPayloadsRequireExactCanonicalMembers(t *testing.T) {
	capability := []byte(`{"schema_version":1,"interrupt_supported":true,"stop_supported":false}`)
	if _, err := DecodeRunControlCapabilityPayload(capability); err != nil {
		t.Fatalf("DecodeRunControlCapabilityPayload(valid) error = %v", err)
	}
	for _, raw := range []string{
		`{"schema_version":1,"interrupt_supported":true,"stop_supported":false,"provider":"hidden"}`,
		`{"schema_version":1,"interrupt_supported":true,"interrupt_supported":false,"stop_supported":false}`,
		`{"schema_version":1,"interrupt_supported":"true","stop_supported":false}`,
		`{"schema_version":1,"interrupt_supported":null,"stop_supported":false}`,
	} {
		if _, err := DecodeRunControlCapabilityPayload([]byte(raw)); err == nil {
			t.Fatalf("DecodeRunControlCapabilityPayload(%s) unexpectedly succeeded", raw)
		}
	}

	completed := []byte(`{"cmd_id":"cmd_interrupt_1","operation":"interrupt","outcome":"completed","completion_state":"ready","reason_code":null}`)
	if _, err := DecodeRunControlOutcomePayload(completed); err != nil {
		t.Fatalf("DecodeRunControlOutcomePayload(completed) error = %v", err)
	}
	for _, raw := range []string{
		`{"cmd_id":"cmd_interrupt_1","operation":"interrupt","outcome":"completed","completion_state":null,"reason_code":null}`,
		`{"cmd_id":"cmd_stop_1","operation":"stop","outcome":"completed","completion_state":"ready","reason_code":null}`,
		`{"cmd_id":"cmd_interrupt_1","operation":"interrupt","outcome":"rejected","completion_state":null,"reason_code":null}`,
		`{"cmd_id":"cmd_interrupt_1","operation":"interrupt","outcome":"completed","completion_state":"ready","reason_code":null,"writer_lease_id":"secret"}`,
		`{"cmd_id":"cmd_interrupt_1","operation":"interrupt","outcome":"completed","completion_state":"ready","reason_code":null,"reason_code":null}`,
	} {
		if _, err := DecodeRunControlOutcomePayload([]byte(raw)); err == nil {
			t.Fatalf("DecodeRunControlOutcomePayload(%s) unexpectedly succeeded", raw)
		}
	}
}

func TestEventReceiptRoundTripsStrictReferenceOnlyFields(t *testing.T) {
	receipt := &EventReceipt{ProposalID: "proposal_01H8X", Seq: 42, Status: EventReceiptAccepted}
	encoded, err := Encode(receipt)
	if err != nil {
		t.Fatalf("Encode(event receipt) error = %v", err)
	}
	if string(encoded) != `{"frame":"event.receipt","proposal_id":"proposal_01H8X","seq":42,"status":"accepted"}` {
		t.Fatalf("encoded event receipt = %s", encoded)
	}
	frame, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode(event receipt) error = %v", err)
	}
	if got, ok := frame.(*EventReceipt); !ok || *got != *receipt {
		t.Fatalf("decoded event receipt = %#v", frame)
	}
	for _, raw := range []string{
		`{"frame":"event.receipt","proposal_id":"proposal_01H8X","seq":42,"status":"accepted","payload":{}}`,
		`{"frame":"event.receipt","proposal_id":"proposal_01H8X","proposal_id":"duplicate","seq":42,"status":"accepted"}`,
		`{"frame":"event.receipt","proposal_id":"","seq":42,"status":"accepted"}`,
		`{"frame":"event.receipt","proposal_id":"proposal_01H8X","seq":0,"status":"accepted"}`,
		`{"frame":"event.receipt","proposal_id":"proposal_01H8X","seq":42,"status":"duplicate"}`,
	} {
		if _, err := Decode([]byte(raw)); err == nil {
			t.Fatalf("Decode(%s) unexpectedly succeeded", raw)
		}
	}
}

func TestDecodeRejectsUnknownFrame(t *testing.T) {
	_, err := Decode([]byte(`{"frame":"future.frame"}`))
	if !errors.Is(err, ErrUnknownFrame) {
		t.Fatalf("Decode() error = %v, want ErrUnknownFrame", err)
	}
}

func TestCommandSessionAttachRoundTrips(t *testing.T) {
	frame := &Command{CommandID: "attach_1", Type: CommandSessionAttach, SessionID: "ses_target", Payload: json.RawMessage(`{"grant":"opaque"}`)}
	encoded, err := Encode(frame)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if got := decoded.(*Command); got.Type != CommandSessionAttach {
		t.Fatalf("command type = %q", got.Type)
	}
}

func TestDecodeAttachGrantPayloadIsStrictAndBounded(t *testing.T) {
	grant, err := DecodeAttachGrantPayload(json.RawMessage(`{"grant":"opaque"}`))
	if err != nil || grant != "opaque" {
		t.Fatalf("DecodeAttachGrantPayload() = %q, %v", grant, err)
	}
	for _, raw := range []json.RawMessage{
		nil,
		json.RawMessage(`null`),
		json.RawMessage(`{}`),
		json.RawMessage(`{"grant":""}`),
		json.RawMessage(`{"grant":null}`),
		json.RawMessage(`{"grant":"opaque","extra":true}`),
		json.RawMessage(`{"grant":"one","grant":"two"}`),
		json.RawMessage(`{"grant":"` + strings.Repeat("x", MaxAttachGrantBytes+1) + `"}`),
	} {
		if _, err := DecodeAttachGrantPayload(raw); err == nil {
			t.Fatalf("DecodeAttachGrantPayload(%s) succeeded", raw)
		}
	}
}

func TestTargetJoinFramesAreStrictAndBounded(t *testing.T) {
	join := `{"frame":"target.join","protocol_version":2,"join_nonce":"0123456789abcdef0123456789abcdef"}`
	frame, err := Decode([]byte(join))
	if err != nil {
		t.Fatalf("Decode(target.join) error = %v", err)
	}
	if got, ok := frame.(*TargetJoin); !ok || got.ProtocolVersion != ProtocolVersionV2 || got.JoinNonce == "" {
		t.Fatalf("Decode(target.join) = %#v", frame)
	}
	for _, raw := range []string{
		`{"frame":"target.join","protocol_version":1,"join_nonce":"0123456789abcdef0123456789abcdef"}`,
		`{"frame":"target.join","protocol_version":2,"join_nonce":"short"}`,
		`{"frame":"target.join","protocol_version":2,"join_nonce":"0123456789abcdef0123456789abcdef","token":"bearer"}`,
		`{"frame":"target.join","protocol_version":2,"join_nonce":"one","join_nonce":"two"}`,
	} {
		if _, err := Decode([]byte(raw)); err == nil {
			t.Fatalf("Decode(%s) unexpectedly succeeded", raw)
		}
	}
	for _, raw := range []string{
		`{"frame":"target.join.challenge","target_session_id":"ses_target","join_nonce":"0123456789abcdef0123456789abcdef","expires_at":1764937800000}`,
		`{"frame":"target.join.credential","credential":"test-only-bearer","target_session_id":"ses_target","target_credential_lineage_ref":"lin_ref","generation":1,"expires_at":1764937800000}`,
	} {
		if _, err := Decode([]byte(raw)); err != nil {
			t.Fatalf("Decode(%s) error = %v", raw, err)
		}
	}
	for _, raw := range []string{
		`{"frame":"target.join.challenge","target_session_id":"ses_target","join_nonce":"short","expires_at":1764937800000}`,
		`{"frame":"target.join.credential","credential":"","target_session_id":"ses_target","target_credential_lineage_ref":"lin_ref","generation":1,"expires_at":1764937800000}`,
		`{"frame":"target.join.credential","credential":"test-only-bearer","target_session_id":"ses_target","target_credential_lineage_ref":"lin_ref","generation":0,"expires_at":1764937800000}`,
	} {
		if _, err := Decode([]byte(raw)); err == nil {
			t.Fatalf("Decode(%s) unexpectedly succeeded", raw)
		}
	}

	credential := &TargetJoinCredential{
		Credential: "test-only-bearer", TargetSessionID: "ses_target", TargetCredentialLineageRef: "lin_ref", Generation: 1, ExpiresAt: 1764937800000,
	}
	encoded, err := Encode(credential)
	if err != nil {
		t.Fatalf("Encode(target.join.credential) error = %v", err)
	}
	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode(target.join.credential) error = %v", err)
	}
	if got, ok := decoded.(*TargetJoinCredential); !ok || got.Credential != credential.Credential || got.TargetCredentialLineageRef != credential.TargetCredentialLineageRef {
		t.Fatalf("target credential round trip = %#v", decoded)
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
		{version: ProtocolVersion, eventType: "publisher.notice", want: true},
		{version: ProtocolVersionV2, eventType: "publisher.notice", want: true},
		{version: ProtocolVersionV2, eventType: "session.message", want: true},
	} {
		if got := EventTypeAllowed(test.version, test.eventType, false); got != test.want {
			t.Fatalf("EventTypeAllowed(%d, %q, false) = %v, want %v", test.version, test.eventType, got, test.want)
		}
	}
	for _, eventType := range []string{"presence", "agent.activity", "log.tail", "resource.sample"} {
		if EventTypeAllowed(ProtocolVersionV2, eventType, true) {
			t.Fatalf("durable ephemeral event type %q is allowed", eventType)
		}
	}
	if !PeerEventTypeAllowed("publisher.notice") {
		t.Fatal("generic peer event type is unexpectedly rejected")
	}
}

func TestDecodeHistoryPageRequestStrictly(t *testing.T) {
	frame, err := Decode([]byte(`{"frame":"history.page","request_id":"hist_1","session_id":"ses_1","before_seq":58,"limit":100}`))
	if err != nil {
		t.Fatalf("Decode(history.page) error = %v", err)
	}
	request, ok := frame.(*HistoryPageRequest)
	if !ok || request.RequestID != "hist_1" || request.SessionID != "ses_1" ||
		request.BeforeSeq == nil || *request.BeforeSeq != 58 || request.Limit != 100 {
		t.Fatalf("history request = %#v", frame)
	}

	for _, raw := range []string{
		`{"frame":"history.page","request_id":"","session_id":"ses_1","limit":1}`,
		`{"frame":"history.page","request_id":"hist_1","session_id":"","limit":1}`,
		`{"frame":"history.page","request_id":"hist_1","session_id":"ses_1","limit":0}`,
		`{"frame":"history.page","request_id":"hist_1","session_id":"ses_1","limit":101}`,
		`{"frame":"history.page","request_id":"hist_1","session_id":"ses_1","before_seq":0,"limit":1}`,
		`{"frame":"history.page","request_id":"hist_1","request_id":"hist_2","session_id":"ses_1","limit":1}`,
		`{"frame":"history.page","request_id":"hist_1","session_id":"ses_1","limit":1,"unknown":true}`,
	} {
		if _, err := Decode([]byte(raw)); err == nil {
			t.Fatalf("Decode(%s) unexpectedly succeeded", raw)
		}
	}
}

func TestEncodeHistoryPageResponse(t *testing.T) {
	seq := int64(52)
	encoded, err := Encode(&HistoryPageResponse{
		RequestID: "hist_1", SessionID: "ses_1",
		Events: []HistoryPageEvent{{
			Frame: FrameEvent, Type: "session.message", SessionID: "ses_1", Seq: seq,
			Time: 1764937200123, Payload: json.RawMessage(`{"role":"agent"}`),
		}},
		LatestSeq: 57, NextBeforeSeq: nil, RetentionState: "complete",
	})
	if err != nil {
		t.Fatalf("Encode(history.page) error = %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got["frame"] != "history.page" || got["next_before_seq"] != nil {
		t.Fatalf("history response envelope = %s", encoded)
	}
	events := got["events"].([]any)
	event := events[0].(map[string]any)
	if event["frame"] != "event" || event["seq"] != float64(seq) {
		t.Fatalf("nested history event = %#v", event)
	}
}

func TestDecodeHistoryPageResponseStrictly(t *testing.T) {
	valid := `{"frame":"history.page","request_id":"hist_1","session_id":"ses_1","events":[{"frame":"event","type":"session.message","session_id":"ses_1","seq":1,"time":1001,"payload":{}}],"latest_seq":1,"next_before_seq":null,"retention_state":"complete"}`
	if frame, err := Decode([]byte(valid)); err != nil {
		t.Fatalf("Decode(valid history response) error = %v", err)
	} else if _, ok := frame.(*HistoryPageResponse); !ok {
		t.Fatalf("Decode(valid history response) = %T", frame)
	}
	for _, raw := range []string{
		`{"frame":"history.page","request_id":"hist_1","session_id":"ses_1","events":null,"latest_seq":0,"next_before_seq":null,"retention_state":"complete"}`,
		`{"frame":"history.page","request_id":"hist_1","session_id":"ses_1","events":[{"frame":"command","type":"x","session_id":"ses_1","seq":1,"time":1,"payload":{}}],"latest_seq":1,"next_before_seq":null,"retention_state":"complete"}`,
		`{"frame":"history.page","request_id":"hist_1","session_id":"ses_1","events":[{"frame":"event","type":"x","session_id":"ses_1","seq":1,"time":1,"payload":{},"extra":true}],"latest_seq":1,"next_before_seq":null,"retention_state":"complete"}`,
		`{"frame":"history.page","request_id":"hist_1","session_id":"ses_1","events":[{"frame":"event","type":"x","session_id":"ses_2","seq":1,"time":1,"payload":{}}],"latest_seq":1,"next_before_seq":null,"retention_state":"complete"}`,
		`{"frame":"history.page","request_id":"hist_1","session_id":"ses_1","events":[{"frame":"event","type":"x","session_id":"ses_1","seq":2,"time":1,"payload":{}},{"frame":"event","type":"x","session_id":"ses_1","seq":1,"time":1,"payload":{}}],"latest_seq":2,"next_before_seq":null,"retention_state":"complete"}`,
		`{"frame":"history.page","request_id":"hist_1","session_id":"ses_1","events":[{"frame":"event","type":"x","session_id":"ses_1","seq":2,"time":1,"payload":{}}],"latest_seq":1,"next_before_seq":null,"retention_state":"complete"}`,
		`{"frame":"history.page","request_id":"hist_1","session_id":"ses_1","events":[{"frame":"event","type":"x","session_id":"ses_1","seq":1,"time":1,"payload":{}}],"latest_seq":1,"next_before_seq":2,"retention_state":"complete"}`,
		`{"frame":"history.page","request_id":"hist_1","session_id":"ses_1","events":[],"latest_seq":0,"next_before_seq":null,"retention_state":"unknown"}`,
	} {
		if _, err := Decode([]byte(raw)); err == nil {
			t.Fatalf("Decode(%s) unexpectedly succeeded", raw)
		}
	}
	ephemeral := strings.Replace(valid, `"type":"session.message"`, `"type":"log.tail"`, 1)
	oversized := strings.Replace(valid, `"payload":{}`, `"payload":"`+strings.Repeat("a", 64*1024)+`"`, 1)
	for _, raw := range []string{ephemeral, oversized} {
		if _, err := Decode([]byte(raw)); err == nil {
			t.Fatal("Decode(malformed history response) unexpectedly succeeded")
		}
	}
}
