package protocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

const (
	ProtocolVersion    = 1
	ProtocolVersionV2  = 2
	HubProtocolVersion = ProtocolVersionV2
)

var (
	ErrNoCompatibleVersion = errors.New("no compatible protocol version")
	ErrUnknownFrame        = errors.New("unknown frame")
)

type FrameName string

const (
	FrameHello                        FrameName = "hello"
	FrameHelloAck                     FrameName = "hello.ack"
	FrameEvent                        FrameName = "event"
	FrameEventReceipt                 FrameName = "event.receipt"
	FrameCommand                      FrameName = "command"
	FrameCommandAck                   FrameName = "command.ack"
	FramePing                         FrameName = "ping"
	FramePong                         FrameName = "pong"
	FrameError                        FrameName = "error"
	FrameHistoryPage                  FrameName = "history.page"
	FrameTargetJoinChallenge          FrameName = "target.join.challenge"
	FrameTargetJoin                   FrameName = "target.join"
	FrameTargetJoinCredential         FrameName = "target.join.credential"
	FrameCredentialRotationRequest    FrameName = "credential.rotation.request"
	FrameCredentialRotationCredential FrameName = "credential.rotation.credential"
	FrameCredentialRotationPossession FrameName = "credential.rotation.possession"
	FrameCredentialRotationActivation FrameName = "credential.rotation.activation"
)

const (
	HistoryPageMaxLimit          = 100
	MaxEventPayloadBytes         = 64 * 1024
	MaxAttachGrantBytes          = 64 * 1024
	MinTargetJoinNonceBytes      = 32
	MaxTargetJoinNonceBytes      = 128
	MaxTargetJoinCredentialBytes = 4096
	MaxCredentialRotationIDBytes = 256
)

type Role string

const (
	RoleClient  Role = "client"
	RoleAdapter Role = "adapter"
)

type CommandType string

const (
	CommandSessionSend       CommandType = "session.send"
	CommandPermissionRespond CommandType = "permission.respond"
	CommandSessionInterrupt  CommandType = "session.interrupt"
	CommandSessionStop       CommandType = "session.stop"
	CommandSessionAttach     CommandType = "session.attach"
)

type AckStatus string

const (
	AckAccepted  AckStatus = "accepted"
	AckRejected  AckStatus = "rejected"
	AckDuplicate AckStatus = "duplicate"
)

type Frame interface {
	FrameName() FrameName
}

type Hello struct {
	ProtocolVersion int            `json:"protocol_version"`
	Role            Role           `json:"role"`
	Token           string         `json:"token"`
	Subscriptions   []Subscription `json:"subscriptions,omitempty"`
	SessionID       string         `json:"session_id,omitempty"`
	Provider        string         `json:"provider,omitempty"`
	Resume          bool           `json:"resume,omitempty"`
}

// TargetJoin is the sole client frame permitted on a pending target socket.
// It deliberately has no Session identity or bearer field.
type TargetJoin struct {
	ProtocolVersion int    `json:"protocol_version"`
	JoinNonce       string `json:"join_nonce"`
}

func (*TargetJoin) FrameName() FrameName { return FrameTargetJoin }

// TargetJoinChallenge contains only the opaque target reference and a
// one-time Hub nonce. It is delivered to the current bootstrap Adapter.
type TargetJoinChallenge struct {
	TargetSessionID string `json:"target_session_id"`
	JoinNonce       string `json:"join_nonce"`
	ExpiresAt       int64  `json:"expires_at"`
}

func (*TargetJoinChallenge) FrameName() FrameName { return FrameTargetJoinChallenge }

// TargetJoinCredential is the sole server frame on a pending target socket.
type TargetJoinCredential struct {
	Credential                 string `json:"credential"`
	TargetSessionID            string `json:"target_session_id"`
	TargetCredentialLineageRef string `json:"target_credential_lineage_ref"`
	Generation                 int64  `json:"generation"`
	ExpiresAt                  int64  `json:"expires_at"`
}

func (*TargetJoinCredential) FrameName() FrameName { return FrameTargetJoinCredential }

type CredentialRotationRequest struct {
	RotationID string `json:"rotation_id"`
}

func (*CredentialRotationRequest) FrameName() FrameName { return FrameCredentialRotationRequest }

type CredentialRotationCredential struct {
	SessionID  string `json:"session_id"`
	RotationID string `json:"rotation_id"`
	Generation int64  `json:"generation"`
	Credential string `json:"credential"`
	ExpiresAt  int64  `json:"expires_at"`
}

func (*CredentialRotationCredential) FrameName() FrameName { return FrameCredentialRotationCredential }

type CredentialRotationPossession struct {
	SessionID     string `json:"session_id"`
	RotationID    string `json:"rotation_id"`
	Generation    int64  `json:"generation"`
	AcceptedEpoch int64  `json:"accepted_epoch"`
}

func (*CredentialRotationPossession) FrameName() FrameName { return FrameCredentialRotationPossession }

type CredentialRotationActivation struct {
	RotationID      string `json:"rotation_id"`
	Generation      int64  `json:"generation"`
	ConnectionEpoch int64  `json:"connection_epoch"`
	AcceptedFence   int64  `json:"accepted_fence"`
}

func (*CredentialRotationActivation) FrameName() FrameName { return FrameCredentialRotationActivation }

func (*Hello) FrameName() FrameName { return FrameHello }

type Subscription struct {
	SessionID string `json:"session_id"`
	LastSeq   int64  `json:"last_seq"`
}

type HelloAck struct {
	ProtocolVersion int                `json:"protocol_version"`
	Sessions        []SessionSummary   `json:"sessions"`
	Capabilities    *HelloCapabilities `json:"capabilities,omitempty"`
}

func (*HelloAck) FrameName() FrameName { return FrameHelloAck }

type HelloCapabilities struct {
	HistoryPage *HistoryPageCapability `json:"history_page,omitempty"`
}

type HistoryPageCapability struct {
	MaxLimit int `json:"max_limit"`
}

type SessionSummary struct {
	SessionID  string `json:"session_id"`
	State      string `json:"state"`
	Provider   string `json:"provider"`
	LatestSeq  int64  `json:"latest_seq"`
	ReplayFrom int64  `json:"replay_from"`
}

type Event struct {
	Type       string          `json:"type"`
	SessionID  string          `json:"session_id"`
	Seq        *int64          `json:"seq,omitempty"`
	Time       int64           `json:"time"`
	Payload    json.RawMessage `json:"payload"`
	ProposalID string          `json:"proposal_id,omitempty"`
}

func (*Event) FrameName() FrameName { return FrameEvent }

func (e *Event) Durable() bool {
	return e.Seq != nil
}

type EventReceiptStatus string

const EventReceiptAccepted EventReceiptStatus = "accepted"

// EventReceipt is the reference-only acknowledgement of one v2 Adapter
// durable event proposal. It intentionally does not carry event contents or
// any session, credential, provider, or grant material.
type EventReceipt struct {
	ProposalID string             `json:"proposal_id"`
	Seq        int64              `json:"seq"`
	Status     EventReceiptStatus `json:"status"`
}

func (*EventReceipt) FrameName() FrameName { return FrameEventReceipt }

type Command struct {
	CommandID string          `json:"cmd_id"`
	Type      CommandType     `json:"type"`
	SessionID string          `json:"session_id"`
	Payload   json.RawMessage `json:"payload"`
}

func (*Command) FrameName() FrameName { return FrameCommand }

type CommandAck struct {
	CommandID string    `json:"cmd_id"`
	Status    AckStatus `json:"status"`
	Reason    string    `json:"reason"`
}

func (*CommandAck) FrameName() FrameName { return FrameCommandAck }

type Ping struct {
	Nonce string `json:"nonce,omitempty"`
}

func (*Ping) FrameName() FrameName { return FramePing }

type Pong struct {
	Nonce string `json:"nonce,omitempty"`
}

func (*Pong) FrameName() FrameName { return FramePong }

type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Fatal   bool   `json:"fatal,omitempty"`
}

func (*Error) FrameName() FrameName { return FrameError }

type HistoryPageRequest struct {
	RequestID string `json:"request_id"`
	SessionID string `json:"session_id"`
	BeforeSeq *int64 `json:"before_seq,omitempty"`
	Limit     int    `json:"limit"`
}

func (*HistoryPageRequest) FrameName() FrameName { return FrameHistoryPage }

type HistoryPageEvent struct {
	Frame     FrameName       `json:"frame"`
	Type      string          `json:"type"`
	SessionID string          `json:"session_id"`
	Seq       int64           `json:"seq"`
	Time      int64           `json:"time"`
	Payload   json.RawMessage `json:"payload"`
}

type HistoryPageResponse struct {
	RequestID      string             `json:"request_id"`
	SessionID      string             `json:"session_id"`
	Events         []HistoryPageEvent `json:"events"`
	LatestSeq      int64              `json:"latest_seq"`
	NextBeforeSeq  *int64             `json:"next_before_seq"`
	RetentionState string             `json:"retention_state"`
}

func (*HistoryPageResponse) FrameName() FrameName { return FrameHistoryPage }

func Decode(data []byte) (Frame, error) {
	var env struct {
		Frame FrameName `json:"frame"`
	}
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, fmt.Errorf("decode frame envelope: %w", err)
	}

	switch env.Frame {
	case FrameHello:
		return decodeInto(data, &Hello{})
	case FrameHelloAck:
		return decodeInto(data, &HelloAck{})
	case FrameEvent:
		return decodeEvent(data)
	case FrameEventReceipt:
		return decodeEventReceipt(data)
	case FrameCommand:
		return decodeInto(data, &Command{})
	case FrameCommandAck:
		return decodeInto(data, &CommandAck{})
	case FramePing:
		return decodeInto(data, &Ping{})
	case FramePong:
		return decodeInto(data, &Pong{})
	case FrameError:
		return decodeInto(data, &Error{})
	case FrameHistoryPage:
		return decodeHistoryPage(data)
	case FrameTargetJoin:
		return decodeTargetJoin(data)
	case FrameTargetJoinChallenge:
		return decodeTargetJoinChallenge(data)
	case FrameTargetJoinCredential:
		return decodeTargetJoinCredential(data)
	case FrameCredentialRotationRequest:
		return decodeCredentialRotationRequest(data)
	case FrameCredentialRotationCredential:
		return decodeCredentialRotationCredential(data)
	case FrameCredentialRotationPossession:
		return decodeCredentialRotationPossession(data)
	case FrameCredentialRotationActivation:
		return decodeCredentialRotationActivation(data)
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnknownFrame, env.Frame)
	}
}

func decodeTargetJoin(data []byte) (Frame, error) {
	fields, err := targetJoinFields(data, FrameTargetJoin, "protocol_version", "join_nonce")
	if err != nil {
		return nil, err
	}
	var out TargetJoin
	if json.Unmarshal(fields["protocol_version"], &out.ProtocolVersion) != nil || json.Unmarshal(fields["join_nonce"], &out.JoinNonce) != nil ||
		out.ProtocolVersion != ProtocolVersionV2 || len(out.JoinNonce) < MinTargetJoinNonceBytes || len(out.JoinNonce) > MaxTargetJoinNonceBytes {
		return nil, targetJoinError(FrameTargetJoin)
	}
	return &out, nil
}

func decodeTargetJoinChallenge(data []byte) (Frame, error) {
	fields, err := targetJoinFields(data, FrameTargetJoinChallenge, "target_session_id", "join_nonce", "expires_at")
	if err != nil {
		return nil, err
	}
	var out TargetJoinChallenge
	if json.Unmarshal(fields["target_session_id"], &out.TargetSessionID) != nil || json.Unmarshal(fields["join_nonce"], &out.JoinNonce) != nil || json.Unmarshal(fields["expires_at"], &out.ExpiresAt) != nil ||
		out.TargetSessionID == "" || len(out.JoinNonce) < MinTargetJoinNonceBytes || len(out.JoinNonce) > MaxTargetJoinNonceBytes || out.ExpiresAt <= 0 {
		return nil, targetJoinError(FrameTargetJoinChallenge)
	}
	return &out, nil
}

func decodeTargetJoinCredential(data []byte) (Frame, error) {
	fields, err := targetJoinFields(data, FrameTargetJoinCredential, "credential", "target_session_id", "target_credential_lineage_ref", "generation", "expires_at")
	if err != nil {
		return nil, err
	}
	var out TargetJoinCredential
	if json.Unmarshal(fields["credential"], &out.Credential) != nil || json.Unmarshal(fields["target_session_id"], &out.TargetSessionID) != nil || json.Unmarshal(fields["target_credential_lineage_ref"], &out.TargetCredentialLineageRef) != nil || json.Unmarshal(fields["generation"], &out.Generation) != nil || json.Unmarshal(fields["expires_at"], &out.ExpiresAt) != nil ||
		len(out.Credential) == 0 || len(out.Credential) > MaxTargetJoinCredentialBytes || out.TargetSessionID == "" || out.TargetCredentialLineageRef == "" || out.Generation < 1 || out.ExpiresAt <= 0 {
		return nil, targetJoinError(FrameTargetJoinCredential)
	}
	return &out, nil
}

func decodeCredentialRotationRequest(data []byte) (Frame, error) {
	fields, err := targetJoinFields(data, FrameCredentialRotationRequest, "rotation_id")
	if err != nil {
		return nil, targetJoinError(FrameCredentialRotationRequest)
	}
	var out CredentialRotationRequest
	if json.Unmarshal(fields["rotation_id"], &out.RotationID) != nil || out.RotationID == "" || len(out.RotationID) > MaxCredentialRotationIDBytes {
		return nil, targetJoinError(FrameCredentialRotationRequest)
	}
	return &out, nil
}

func decodeCredentialRotationCredential(data []byte) (Frame, error) {
	fields, err := targetJoinFields(data, FrameCredentialRotationCredential, "session_id", "rotation_id", "generation", "credential", "expires_at")
	if err != nil {
		return nil, targetJoinError(FrameCredentialRotationCredential)
	}
	var out CredentialRotationCredential
	if json.Unmarshal(fields["session_id"], &out.SessionID) != nil || json.Unmarshal(fields["rotation_id"], &out.RotationID) != nil || json.Unmarshal(fields["generation"], &out.Generation) != nil || json.Unmarshal(fields["credential"], &out.Credential) != nil || json.Unmarshal(fields["expires_at"], &out.ExpiresAt) != nil || out.SessionID == "" || out.RotationID == "" || len(out.RotationID) > MaxCredentialRotationIDBytes || out.Generation < 1 || len(out.Credential) == 0 || len(out.Credential) > MaxTargetJoinCredentialBytes || out.ExpiresAt <= 0 {
		return nil, targetJoinError(FrameCredentialRotationCredential)
	}
	return &out, nil
}

func decodeCredentialRotationPossession(data []byte) (Frame, error) {
	fields, err := targetJoinFields(data, FrameCredentialRotationPossession, "session_id", "rotation_id", "generation", "accepted_epoch")
	if err != nil {
		return nil, targetJoinError(FrameCredentialRotationPossession)
	}
	var out CredentialRotationPossession
	if json.Unmarshal(fields["session_id"], &out.SessionID) != nil || json.Unmarshal(fields["rotation_id"], &out.RotationID) != nil || json.Unmarshal(fields["generation"], &out.Generation) != nil || json.Unmarshal(fields["accepted_epoch"], &out.AcceptedEpoch) != nil || out.SessionID == "" || out.RotationID == "" || len(out.RotationID) > MaxCredentialRotationIDBytes || out.Generation < 1 || out.AcceptedEpoch < 1 {
		return nil, targetJoinError(FrameCredentialRotationPossession)
	}
	return &out, nil
}

func decodeCredentialRotationActivation(data []byte) (Frame, error) {
	fields, err := targetJoinFields(data, FrameCredentialRotationActivation, "rotation_id", "generation", "connection_epoch", "accepted_fence")
	if err != nil {
		return nil, targetJoinError(FrameCredentialRotationActivation)
	}
	var out CredentialRotationActivation
	if json.Unmarshal(fields["rotation_id"], &out.RotationID) != nil || json.Unmarshal(fields["generation"], &out.Generation) != nil || json.Unmarshal(fields["connection_epoch"], &out.ConnectionEpoch) != nil || json.Unmarshal(fields["accepted_fence"], &out.AcceptedFence) != nil || out.RotationID == "" || len(out.RotationID) > MaxCredentialRotationIDBytes || out.Generation < 1 || out.ConnectionEpoch < 1 || out.AcceptedFence < 1 {
		return nil, targetJoinError(FrameCredentialRotationActivation)
	}
	return &out, nil
}

func targetJoinFields(data []byte, frame FrameName, keys ...string) (map[string]json.RawMessage, error) {
	fields, err := strictObject(data)
	if err != nil || len(fields) != len(keys)+1 {
		return nil, targetJoinError(frame)
	}
	var actual FrameName
	if json.Unmarshal(fields["frame"], &actual) != nil || actual != frame {
		return nil, targetJoinError(frame)
	}
	for _, key := range keys {
		if fields[key] == nil {
			return nil, targetJoinError(frame)
		}
	}
	return fields, nil
}

func targetJoinError(frame FrameName) error { return fmt.Errorf("decode %s: invalid frame", frame) }

func decodeEvent(data []byte) (Frame, error) {
	fields, err := strictObject(data)
	if err != nil {
		return nil, fmt.Errorf("decode event: %w", err)
	}
	if raw := fields["frame"]; raw == nil {
		return nil, errors.New("decode event: missing frame")
	} else {
		var frame FrameName
		if json.Unmarshal(raw, &frame) != nil || frame != FrameEvent {
			return nil, errors.New("decode event: invalid frame")
		}
	}
	delete(fields, "frame")
	withoutFrame, err := json.Marshal(fields)
	if err != nil {
		return nil, fmt.Errorf("decode event payload: %w", err)
	}
	var event Event
	if err := json.Unmarshal(withoutFrame, &event); err != nil {
		return nil, fmt.Errorf("decode event: %w", err)
	}
	return &event, nil
}

func decodeEventReceipt(data []byte) (Frame, error) {
	fields, err := strictObject(data)
	if err != nil {
		return nil, fmt.Errorf("decode event.receipt: %w", err)
	}
	if len(fields) != 4 {
		return nil, errors.New("decode event.receipt: expected exactly four fields")
	}
	for _, key := range []string{"frame", "proposal_id", "seq", "status"} {
		if fields[key] == nil {
			return nil, fmt.Errorf("decode event.receipt: missing %s", key)
		}
	}
	var frame FrameName
	var receipt EventReceipt
	if json.Unmarshal(fields["frame"], &frame) != nil || frame != FrameEventReceipt ||
		json.Unmarshal(fields["proposal_id"], &receipt.ProposalID) != nil ||
		json.Unmarshal(fields["seq"], &receipt.Seq) != nil ||
		json.Unmarshal(fields["status"], &receipt.Status) != nil ||
		len(receipt.ProposalID) == 0 || len(receipt.ProposalID) > 255 || receipt.Seq < 1 ||
		receipt.Status != EventReceiptAccepted {
		return nil, errors.New("decode event.receipt: invalid receipt")
	}
	return &receipt, nil
}

func decodeHistoryPage(data []byte) (Frame, error) {
	fields, err := strictObject(data)
	if err != nil {
		return nil, fmt.Errorf("decode history.page: %w", err)
	}
	allowed := map[string]bool{
		"frame": true, "request_id": true, "session_id": true, "before_seq": true, "limit": true,
		"events": true, "latest_seq": true, "next_before_seq": true, "retention_state": true,
	}
	for key := range fields {
		if !allowed[key] {
			return nil, fmt.Errorf("decode history.page: unknown field %q", key)
		}
	}
	var frame FrameName
	if err := json.Unmarshal(fields["frame"], &frame); err != nil || frame != FrameHistoryPage {
		return nil, errors.New("decode history.page: invalid frame")
	}
	response := fields["events"] != nil || fields["latest_seq"] != nil ||
		fields["next_before_seq"] != nil || fields["retention_state"] != nil
	if response {
		if fields["before_seq"] != nil || fields["limit"] != nil {
			return nil, errors.New("decode history.page: mixed request and response fields")
		}
		var out HistoryPageResponse
		if err := decodeRequiredHistoryFields(fields, &out.RequestID, &out.SessionID); err != nil {
			return nil, err
		}
		for _, key := range []string{"events", "latest_seq", "next_before_seq", "retention_state"} {
			if fields[key] == nil {
				return nil, fmt.Errorf("decode history.page: missing %s", key)
			}
		}
		if bytes.Equal(bytes.TrimSpace(fields["events"]), []byte("null")) ||
			json.Unmarshal(fields["latest_seq"], &out.LatestSeq) != nil ||
			json.Unmarshal(fields["next_before_seq"], &out.NextBeforeSeq) != nil ||
			json.Unmarshal(fields["retention_state"], &out.RetentionState) != nil {
			return nil, errors.New("decode history.page: invalid response")
		}
		out.Events, err = decodeHistoryPageEvents(fields["events"], out.SessionID)
		if err != nil || !validHistoryPageResponse(&out) {
			return nil, errors.New("decode history.page: invalid response")
		}
		return &out, nil
	}

	var out HistoryPageRequest
	if err := decodeRequiredHistoryFields(fields, &out.RequestID, &out.SessionID); err != nil {
		return nil, err
	}
	if fields["limit"] == nil || json.Unmarshal(fields["limit"], &out.Limit) != nil ||
		out.Limit < 1 || out.Limit > HistoryPageMaxLimit {
		return nil, errors.New("decode history.page: limit must be in 1..100")
	}
	if raw := fields["before_seq"]; raw != nil {
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) || json.Unmarshal(raw, &out.BeforeSeq) != nil ||
			out.BeforeSeq == nil || *out.BeforeSeq < 1 {
			return nil, errors.New("decode history.page: before_seq must be positive")
		}
	}
	return &out, nil
}

func decodeHistoryPageEvents(data []byte, sessionID string) ([]HistoryPageEvent, error) {
	var rawEvents []json.RawMessage
	if err := json.Unmarshal(data, &rawEvents); err != nil || len(rawEvents) > HistoryPageMaxLimit {
		return nil, errors.New("invalid events")
	}
	events := make([]HistoryPageEvent, len(rawEvents))
	for index, raw := range rawEvents {
		fields, err := strictObject(raw)
		if err != nil || len(fields) != 6 {
			return nil, errors.New("invalid event")
		}
		for _, key := range []string{"frame", "type", "session_id", "seq", "time", "payload"} {
			if fields[key] == nil {
				return nil, errors.New("invalid event")
			}
		}
		for key := range fields {
			if key != "frame" && key != "type" && key != "session_id" && key != "seq" && key != "time" && key != "payload" {
				return nil, errors.New("invalid event")
			}
		}
		event := &events[index]
		if json.Unmarshal(fields["frame"], &event.Frame) != nil || event.Frame != FrameEvent ||
			json.Unmarshal(fields["type"], &event.Type) != nil || event.Type == "" ||
			json.Unmarshal(fields["session_id"], &event.SessionID) != nil || event.SessionID != sessionID ||
			json.Unmarshal(fields["seq"], &event.Seq) != nil || event.Seq < 1 ||
			json.Unmarshal(fields["time"], &event.Time) != nil || !json.Valid(fields["payload"]) ||
			len(fields["payload"]) > MaxEventPayloadBytes || !EventTypeAllowed(ProtocolVersionV2, event.Type, true) {
			return nil, errors.New("invalid event")
		}
		event.Payload = append(event.Payload[:0], fields["payload"]...)
	}
	return events, nil
}

func validHistoryPageResponse(response *HistoryPageResponse) bool {
	if response.LatestSeq < 0 || (response.RetentionState != "complete" && response.RetentionState != "retention_gap") {
		return false
	}
	for index, event := range response.Events {
		if event.Seq > response.LatestSeq || index > 0 && response.Events[index-1].Seq >= event.Seq {
			return false
		}
	}
	if response.NextBeforeSeq != nil {
		return *response.NextBeforeSeq > 0 && len(response.Events) > 0 && *response.NextBeforeSeq == response.Events[0].Seq
	}
	return true
}

func decodeRequiredHistoryFields(fields map[string]json.RawMessage, requestID, sessionID *string) error {
	if fields["request_id"] == nil || json.Unmarshal(fields["request_id"], requestID) != nil || *requestID == "" {
		return errors.New("decode history.page: request_id is required")
	}
	if fields["session_id"] == nil || json.Unmarshal(fields["session_id"], sessionID) != nil || *sessionID == "" {
		return errors.New("decode history.page: session_id is required")
	}
	return nil
}

func strictObject(data []byte) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, errors.New("expected object")
	}
	fields := make(map[string]json.RawMessage)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		key, ok := token.(string)
		if !ok {
			return nil, errors.New("expected object key")
		}
		if _, duplicate := fields[key]; duplicate {
			return nil, fmt.Errorf("duplicate field %q", key)
		}
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			return nil, err
		}
		fields[key] = raw
	}
	if _, err := decoder.Token(); err != nil {
		return nil, err
	}
	if token, err := decoder.Token(); err != io.EOF || token != nil {
		return nil, errors.New("trailing JSON value")
	}
	return fields, nil
}

// DecodeAttachGrantPayload accepts the deliberately narrow session.attach
// payload. The raw grant remains at Client-to-Hub ingress and must not be
// retained in a command/event or passed to downstream stores.
func DecodeAttachGrantPayload(payload json.RawMessage) (string, error) {
	if len(payload) == 0 || len(payload) > MaxAttachGrantBytes+32 {
		return "", errors.New("attach grant payload is invalid")
	}
	fields, err := strictObject(payload)
	if err != nil || len(fields) != 1 || fields["grant"] == nil {
		return "", errors.New("attach grant payload is invalid")
	}
	var grant string
	if err := json.Unmarshal(fields["grant"], &grant); err != nil || grant == "" || len(grant) > MaxAttachGrantBytes {
		return "", errors.New("attach grant payload is invalid")
	}
	return grant, nil
}

func Encode(frame Frame) ([]byte, error) {
	if frame == nil {
		return nil, errors.New("encode frame: nil frame")
	}

	body, err := json.Marshal(frame)
	if err != nil {
		return nil, fmt.Errorf("encode frame body: %w", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		return nil, fmt.Errorf("encode frame object: %w", err)
	}
	name, err := json.Marshal(frame.FrameName())
	if err != nil {
		return nil, fmt.Errorf("encode frame name: %w", err)
	}
	fields["frame"] = name

	encoded, err := json.Marshal(fields)
	if err != nil {
		return nil, fmt.Errorf("encode frame envelope: %w", err)
	}
	return encoded, nil
}

func NegotiateVersion(peer []int, supported []int) (int, error) {
	supports := make(map[int]struct{}, len(supported))
	for _, version := range supported {
		supports[version] = struct{}{}
	}

	best := 0
	for _, version := range peer {
		if _, ok := supports[version]; ok && version > best {
			best = version
		}
	}
	if best == 0 {
		return 0, ErrNoCompatibleVersion
	}
	return best, nil
}

func NegotiateHighestVersion(peerHighest, hubHighest int) (int, error) {
	if peerHighest < 1 || hubHighest < 1 {
		return 0, ErrNoCompatibleVersion
	}
	if peerHighest < hubHighest {
		return peerHighest, nil
	}
	return hubHighest, nil
}

func EventTypeAllowed(version int, eventType string, durable bool) bool {
	if durable {
		switch eventType {
		case "presence", "agent.activity", "log.tail", "resource.sample":
			return false
		}
	}
	return version >= ProtocolVersion
}

func PeerEventTypeAllowed(eventType string) bool {
	return eventType != ""
}

func decodeInto(data []byte, out Frame) (Frame, error) {
	var env map[string]json.RawMessage
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, fmt.Errorf("decode frame object: %w", err)
	}
	delete(env, "frame")

	withoutFrame, err := json.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("decode frame payload: %w", err)
	}
	if err := json.Unmarshal(withoutFrame, &out); err != nil {
		return nil, fmt.Errorf("decode %T: %w", out, err)
	}
	return out, nil
}
