package protocol

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
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
	FrameAttentionSubscribe           FrameName = "attention.subscribe"
	FrameAttentionSummary             FrameName = "attention.summary"
	FrameTargetJoinChallenge          FrameName = "target.join.challenge"
	FrameTargetJoin                   FrameName = "target.join"
	FrameTargetJoinCredential         FrameName = "target.join.credential"
	FrameCredentialRotationRequest    FrameName = "credential.rotation.request"
	FrameCredentialRotationCredential FrameName = "credential.rotation.credential"
	FrameCredentialRotationPossession FrameName = "credential.rotation.possession"
	FrameCredentialRotationActivation FrameName = "credential.rotation.activation"
	FrameSettingsDeliveryExecute      FrameName = "settings.delivery.execute"
)

const (
	HistoryPageMaxLimit          = 100
	MaxEventPayloadBytes         = 64 * 1024
	MaxAttachGrantBytes          = 64 * 1024
	MinTargetJoinNonceBytes      = 32
	MaxTargetJoinNonceBytes      = 128
	MaxTargetJoinCredentialBytes = 4096
	MaxCredentialRotationIDBytes = 256
	MaxSettingsIdentifierBytes   = 128
	MaxSettingsCommandIDBytes    = 255
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
	CommandSettingsChange    CommandType = "session.settings.change"
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
	ProtocolVersion     int                         `json:"protocol_version"`
	Sessions            []SessionSummary            `json:"sessions"`
	Capabilities        *HelloCapabilities          `json:"capabilities,omitempty"`
	ConnectionAuthority *ConnectionAuthorityReceipt `json:"connection_authority,omitempty"`
}

func (*HelloAck) FrameName() FrameName { return FrameHelloAck }

// ConnectionAuthorityReceipt is a v2 Adapter-only, non-secret snapshot of the
// Store-proven live connection tuple. It is neither a bearer nor a capability:
// consumers must fail closed when the tuple stops matching trusted lifecycle
// state. It deliberately carries no Provider configuration, path, content, or
// summary data.
type ConnectionAuthorityReceipt struct {
	SessionID            string `json:"session_id"`
	ConnectionEpoch      int64  `json:"connection_epoch"`
	CredentialGeneration int64  `json:"credential_generation"`
	AcceptedFence        int64  `json:"accepted_fence"`
	WriterLeaseID        string `json:"writer_lease_id"`
	ExpiresAt            int64  `json:"expires_at"`
}

type HelloCapabilities struct {
	HistoryPage      *HistoryPageCapability        `json:"history_page,omitempty"`
	AttentionSummary *AttentionSummaryCapability   `json:"attention_summary,omitempty"`
	Settings         *SettingsCapability           `json:"settings,omitempty"`
	RunControl       *RunControlCapability         `json:"run_control,omitempty"`
	FileReferences   *FileReferenceHelloCapability `json:"file_references,omitempty"`
}

type HistoryPageCapability struct {
	MaxLimit int `json:"max_limit"`
}

type AttentionSummaryCapability struct {
	MaxSessions int `json:"max_sessions"`
}

// SettingsCapability advertises fixed v2 protocol limits. It does not claim
// that a particular Adapter control is mutable.
type SettingsCapability struct {
	SchemaVersion                  int `json:"schema_version"`
	MaxPendingChanges              int `json:"max_pending_changes"`
	ProviderResponseTimeoutSeconds int `json:"provider_response_timeout_seconds"`
}

// RunControlCapability advertises the fixed v2 durable run-control contract.
// Per-Adapter support remains in the durable session.run.capabilities event.
type RunControlCapability struct {
	SchemaVersion            int `json:"schema_version"`
	MaxPending               int `json:"max_pending"`
	CompletionTimeoutSeconds int `json:"completion_timeout_seconds"`
}

// FileReferenceHelloCapability advertises the fixed v2 grammar. Per-Adapter
// support remains in the durable capability event.
type FileReferenceHelloCapability struct {
	SchemaVersion    int `json:"schema_version"`
	MaxReferences    int `json:"max_references"`
	MaxMetadataBytes int `json:"max_metadata_bytes"`
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

// SettingsChange is the bounded Client request interpreted by the Hub. It
// deliberately contains only opaque identifiers, never Provider objects.
type SettingsChange struct {
	CapabilityFingerprint     string
	RequestedModelID          *string
	RequestedPermissionModeID *string
}

type SettingsCapabilityChoice struct {
	ID    string `json:"id"`
	Label string `json:"label"`
}

type SettingsCapabilityPayload struct {
	SchemaVersion             int                        `json:"schema_version"`
	Fingerprint               string                     `json:"fingerprint"`
	Models                    []SettingsCapabilityChoice `json:"models"`
	PermissionModes           []SettingsCapabilityChoice `json:"permission_modes"`
	EffectiveModelID          string                     `json:"effective_model_id"`
	EffectivePermissionModeID string                     `json:"effective_permission_mode_id"`
	ModelChange               string                     `json:"model_change"`
	PermissionChange          string                     `json:"permission_change"`
	ModelReadOnlyReason       *string                    `json:"model_read_only_reason"`
	PermissionReadOnlyReason  *string                    `json:"permission_read_only_reason"`
}

// SettingsEffectivePayload is the Adapter's bounded result proposal. The Hub
// resolves it to durable capability metadata before it can finalize a command.
type SettingsEffectivePayload struct {
	CommandID                 string
	RequestFingerprint        string
	EffectiveFingerprint      string
	Outcome                   string
	EffectiveModelID          string
	EffectivePermissionModeID string
	ReasonCode                *string
}

type SettingsDeliveryExecute struct {
	SessionID          string `json:"session_id"`
	CommandID          string `json:"cmd_id"`
	ReservationVersion int64  `json:"reservation_version"`
	OperationTimeoutMS int64  `json:"operation_timeout_ms"`
}

func (*SettingsDeliveryExecute) FrameName() FrameName { return FrameSettingsDeliveryExecute }

// FileReferenceCapabilityPayload is the Adapter-owned, bounded capability
// proposal. It contains neither Provider objects nor workspace metadata.
type FileReferenceCapabilityPayload struct {
	SchemaVersion int                                `json:"schema_version"`
	Fingerprint   string                             `json:"fingerprint"`
	MaxReferences int                                `json:"max_references"`
	MaxTotalBytes int64                              `json:"max_total_bytes"`
	File          FileReferenceDispositionCapability `json:"file"`
	Image         FileReferenceImageCapability       `json:"image"`
}

type FileReferenceDispositionCapability struct {
	Mode     string  `json:"mode"`
	MaxBytes *int64  `json:"max_bytes"`
	Reason   *string `json:"reason"`
}

type FileReferenceImageCapability struct {
	Mode       string   `json:"mode"`
	MaxBytes   *int64   `json:"max_bytes"`
	MediaTypes []string `json:"media_types"`
	Reason     *string  `json:"reason"`
}

// FileReferenceSendPayload is the bounded part of a v2 session.send that
// contains references. RequestFingerprint is opaque Store metadata only.
type FileReferenceSendPayload struct {
	CapabilityFingerprint string
	RequestFingerprint    string
	ReferenceCount        int
	HasReferences         bool
	References            []FileReferencePart
}

type FileReferencePart struct {
	Disposition string
	Bytes       int64
	MediaType   *string
}

// FileReferenceOutcomePayload is the Adapter's bounded, terminal delivery
// proposal. The Hub checks it against the ledger before it is persisted.
type FileReferenceOutcomePayload struct {
	MessageID      string
	CommandID      string
	Outcome        string
	ReferenceIndex *int
	Reason         *string
}

// RunControlCapabilityPayload is the exact Adapter-owned capability proposal.
// It deliberately contains no provider object or connection metadata.
type RunControlCapabilityPayload struct {
	SchemaVersion      int  `json:"schema_version"`
	InterruptSupported bool `json:"interrupt_supported"`
	StopSupported      bool `json:"stop_supported"`
}

// RunControlOutcomePayload is the exact Adapter completion proposal. The Hub
// resolves it through the Store before broadcasting the Store-derived event.
type RunControlOutcomePayload struct {
	CommandID       string
	Operation       string
	Outcome         string
	CompletionState *string
	ReasonCode      *string
}

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

// AttentionSubscribe carries no Session ID or cursor. Membership is derived
// exclusively from the current Auth-owned attention grant.
type AttentionSubscribe struct {
	RequestID string `json:"request_id"`
}

func (*AttentionSubscribe) FrameName() FrameName { return FrameAttentionSubscribe }

type AttentionSummary struct {
	SessionID       string               `json:"session_id"`
	LatestSeq       int64                `json:"latest_seq"`
	State           string               `json:"state"`
	Permission      *AttentionPermission `json:"permission,omitempty"`
	TerminalOutcome *string              `json:"terminal_outcome,omitempty"`
	LatestChangeSeq *int64               `json:"latest_change_seq,omitempty"`
	Blocker         *AttentionBlocker    `json:"blocker,omitempty"`
	SummaryVersion  int64                `json:"summary_version"`
	SummaryState    string               `json:"summary_state"`
}

type AttentionPermission struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

type AttentionBlocker struct {
	Kind              string  `json:"kind"`
	Reason            *string `json:"reason,omitempty"`
	ExpiresAt         *int64  `json:"expires_at,omitempty"`
	BlockingSessionID *string `json:"blocking_session_id,omitempty"`
	Operation         *string `json:"operation,omitempty"`
}

type AttentionSummaryFrame struct {
	RequestID         string             `json:"request_id"`
	Kind              string             `json:"kind"`
	SubscriptionState string             `json:"subscription_state"`
	Summaries         []AttentionSummary `json:"summaries"`
}

func (*AttentionSummaryFrame) FrameName() FrameName { return FrameAttentionSummary }

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
		return decodeCommand(data)
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
	case FrameAttentionSubscribe:
		return decodeAttentionSubscribe(data)
	case FrameAttentionSummary:
		return decodeAttentionSummary(data)
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
	case FrameSettingsDeliveryExecute:
		return decodeSettingsDeliveryExecute(data)
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnknownFrame, env.Frame)
	}
}

func decodeAttentionSubscribe(data []byte) (Frame, error) {
	fields, err := targetJoinFields(data, FrameAttentionSubscribe, "request_id")
	if err != nil {
		return nil, fmt.Errorf("decode attention.subscribe: invalid frame")
	}
	var out AttentionSubscribe
	if json.Unmarshal(fields["request_id"], &out.RequestID) != nil || !validSettingsCommandID(out.RequestID) {
		return nil, fmt.Errorf("decode attention.subscribe: invalid frame")
	}
	return &out, nil
}

func decodeAttentionSummary(data []byte) (Frame, error) {
	fields, err := targetJoinFields(data, FrameAttentionSummary, "request_id", "kind", "subscription_state", "summaries")
	if err != nil {
		return nil, errors.New("decode attention.summary: invalid frame")
	}
	var out AttentionSummaryFrame
	if json.Unmarshal(fields["request_id"], &out.RequestID) != nil || json.Unmarshal(fields["kind"], &out.Kind) != nil ||
		json.Unmarshal(fields["subscription_state"], &out.SubscriptionState) != nil ||
		!validSettingsCommandID(out.RequestID) || (out.Kind != "snapshot" && out.Kind != "update") ||
		(out.SubscriptionState != "complete" && out.SubscriptionState != "incomplete") {
		return nil, errors.New("decode attention.summary: invalid frame")
	}
	var errSummaries error
	out.Summaries, errSummaries = decodeAttentionSummaries(fields["summaries"])
	if errSummaries != nil {
		return nil, errors.New("decode attention.summary: invalid frame")
	}
	seen := make(map[string]struct{}, len(out.Summaries))
	for _, summary := range out.Summaries {
		if !validAttentionSummary(summary) {
			return nil, errors.New("decode attention.summary: invalid frame")
		}
		if _, duplicate := seen[summary.SessionID]; duplicate {
			return nil, errors.New("decode attention.summary: invalid frame")
		}
		seen[summary.SessionID] = struct{}{}
	}
	return &out, nil
}

func decodeAttentionSummaries(raw json.RawMessage) ([]AttentionSummary, error) {
	var entries []json.RawMessage
	if json.Unmarshal(raw, &entries) != nil || len(entries) > 64 {
		return nil, errors.New("invalid summaries")
	}
	result := make([]AttentionSummary, 0, len(entries))
	for _, entry := range entries {
		fields, err := strictObject(entry)
		if err != nil {
			return nil, err
		}
		allowed := map[string]bool{"session_id": true, "latest_seq": true, "state": true, "permission": true, "terminal_outcome": true, "latest_change_seq": true, "blocker": true, "summary_version": true, "summary_state": true}
		for key := range fields {
			if !allowed[key] {
				return nil, errors.New("unknown summary field")
			}
		}
		var summary AttentionSummary
		if fields["session_id"] == nil || fields["latest_seq"] == nil || fields["state"] == nil || fields["summary_version"] == nil || fields["summary_state"] == nil ||
			json.Unmarshal(fields["session_id"], &summary.SessionID) != nil || json.Unmarshal(fields["latest_seq"], &summary.LatestSeq) != nil ||
			json.Unmarshal(fields["state"], &summary.State) != nil || json.Unmarshal(fields["summary_version"], &summary.SummaryVersion) != nil || json.Unmarshal(fields["summary_state"], &summary.SummaryState) != nil ||
			decodeOptionalAttentionPermission(fields["permission"], &summary.Permission) != nil || decodeOptionalString(fields["terminal_outcome"], &summary.TerminalOutcome) != nil ||
			decodeOptionalInt64(fields["latest_change_seq"], &summary.LatestChangeSeq) != nil || decodeOptionalAttentionBlocker(fields["blocker"], &summary.Blocker) != nil || !validAttentionSummary(summary) {
			return nil, errors.New("invalid summary")
		}
		result = append(result, summary)
	}
	return result, nil
}

func decodeOptionalAttentionPermission(raw json.RawMessage, out **AttentionPermission) error {
	if raw == nil || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil
	}
	fields, err := strictObject(raw)
	if err != nil || len(fields) != 2 || fields["id"] == nil || fields["status"] == nil {
		return errors.New("invalid permission")
	}
	value := &AttentionPermission{}
	if json.Unmarshal(fields["id"], &value.ID) != nil || json.Unmarshal(fields["status"], &value.Status) != nil {
		return errors.New("invalid permission")
	}
	*out = value
	return nil
}

func decodeOptionalAttentionBlocker(raw json.RawMessage, out **AttentionBlocker) error {
	if raw == nil || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil
	}
	fields, err := strictObject(raw)
	if err != nil || fields["kind"] == nil {
		return errors.New("invalid blocker")
	}
	allowed := map[string]bool{"kind": true, "reason": true, "expires_at": true, "blocking_session_id": true, "operation": true}
	for key := range fields {
		if !allowed[key] {
			return errors.New("invalid blocker")
		}
	}
	value := &AttentionBlocker{}
	if json.Unmarshal(fields["kind"], &value.Kind) != nil || decodeOptionalString(fields["reason"], &value.Reason) != nil ||
		decodeOptionalInt64(fields["expires_at"], &value.ExpiresAt) != nil || decodeOptionalString(fields["blocking_session_id"], &value.BlockingSessionID) != nil ||
		decodeOptionalString(fields["operation"], &value.Operation) != nil {
		return errors.New("invalid blocker")
	}
	*out = value
	return nil
}

func decodeOptionalString(raw json.RawMessage, out **string) error {
	if raw == nil || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil
	}
	var value string
	if json.Unmarshal(raw, &value) != nil {
		return errors.New("invalid optional string")
	}
	*out = &value
	return nil
}

func decodeOptionalInt64(raw json.RawMessage, out **int64) error {
	if raw == nil || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil
	}
	var value int64
	if json.Unmarshal(raw, &value) != nil {
		return errors.New("invalid optional int")
	}
	*out = &value
	return nil
}

func validAttentionSummary(summary AttentionSummary) bool {
	if !validSettingsCommandID(summary.SessionID) || summary.LatestSeq < 0 || summary.SummaryVersion < 0 ||
		(summary.SummaryState != "complete" && summary.SummaryState != "incomplete") || summary.State == "" {
		return false
	}
	if summary.Permission != nil && (!validSettingsCommandID(summary.Permission.ID) || summary.Permission.Status != "pending") {
		return false
	}
	if summary.Blocker == nil {
		return true
	}
	blocker := summary.Blocker
	if blocker.Kind != "queued" && blocker.Kind != "reauthorization_required" && blocker.Kind != "new_run_required" && blocker.Kind != "outcome_unknown" {
		return false
	}
	if blocker.Kind != "queued" && (blocker.Reason != nil || blocker.ExpiresAt != nil || blocker.BlockingSessionID != nil) {
		return false
	}
	if blocker.Kind != "outcome_unknown" && blocker.Operation != nil {
		return false
	}
	return (blocker.Reason == nil || validSettingsIdentifier(*blocker.Reason)) &&
		(blocker.BlockingSessionID == nil || validSettingsCommandID(*blocker.BlockingSessionID)) &&
		(blocker.Operation == nil || validSettingsIdentifier(*blocker.Operation))
}

func decodeCommand(data []byte) (Frame, error) {
	decoded, err := decodeInto(data, &Command{})
	if err != nil {
		return nil, err
	}
	command := decoded.(*Command)
	if command.Type != CommandSettingsChange {
		return command, nil
	}
	fields, err := targetJoinFields(data, FrameCommand, "cmd_id", "type", "session_id", "payload")
	if err != nil || !validSettingsCommandID(command.CommandID) || !validSettingsCommandID(command.SessionID) {
		return nil, errors.New("decode settings command: invalid command")
	}
	if _, err := DecodeSettingsChangePayload(fields["payload"]); err != nil {
		return nil, fmt.Errorf("decode settings command: %w", err)
	}
	return command, nil
}

// DecodeSettingsChangePayload validates the exact, provider-neutral command
// grammar before Hub routing can touch the durable Settings ledger.
func DecodeSettingsChangePayload(payload json.RawMessage) (SettingsChange, error) {
	fields, err := strictObject(payload)
	if err != nil || len(fields) < 2 || len(fields) > 3 {
		return SettingsChange{}, errors.New("invalid settings change payload")
	}
	for key := range fields {
		if key != "capability_fingerprint" && key != "model_id" && key != "permission_mode_id" {
			return SettingsChange{}, errors.New("invalid settings change payload")
		}
	}
	var change SettingsChange
	if fields["capability_fingerprint"] == nil || json.Unmarshal(fields["capability_fingerprint"], &change.CapabilityFingerprint) != nil || !validSettingsFingerprint(change.CapabilityFingerprint) {
		return SettingsChange{}, errors.New("invalid settings capability fingerprint")
	}
	if raw := fields["model_id"]; raw != nil {
		var value string
		if json.Unmarshal(raw, &value) != nil || !validSettingsIdentifier(value) {
			return SettingsChange{}, errors.New("invalid settings model id")
		}
		change.RequestedModelID = &value
	}
	if raw := fields["permission_mode_id"]; raw != nil {
		var value string
		if json.Unmarshal(raw, &value) != nil || !validSettingsIdentifier(value) {
			return SettingsChange{}, errors.New("invalid settings permission mode id")
		}
		change.RequestedPermissionModeID = &value
	}
	if change.RequestedModelID == nil && change.RequestedPermissionModeID == nil {
		return SettingsChange{}, errors.New("settings change requires a requested value")
	}
	return change, nil
}

// DecodeSettingsCapabilityPayload validates the exact durable capability
// event grammar and recomputes its canonical fingerprint.
func DecodeSettingsCapabilityPayload(payload json.RawMessage) (SettingsCapabilityPayload, error) {
	fields, err := strictObject(payload)
	if err != nil || len(fields) != 10 {
		return SettingsCapabilityPayload{}, errors.New("invalid settings capability payload")
	}
	allowed := map[string]bool{"schema_version": true, "fingerprint": true, "models": true, "permission_modes": true, "effective_model_id": true, "effective_permission_mode_id": true, "model_change": true, "permission_change": true, "model_read_only_reason": true, "permission_read_only_reason": true}
	for key := range fields {
		if !allowed[key] {
			return SettingsCapabilityPayload{}, errors.New("invalid settings capability payload")
		}
	}
	models, err := decodeSettingsCapabilityChoices(fields["models"])
	if err != nil {
		return SettingsCapabilityPayload{}, errors.New("invalid settings capability payload")
	}
	permissionModes, err := decodeSettingsCapabilityChoices(fields["permission_modes"])
	if err != nil {
		return SettingsCapabilityPayload{}, errors.New("invalid settings capability payload")
	}
	var out SettingsCapabilityPayload
	out.Models = models
	out.PermissionModes = permissionModes
	if json.Unmarshal(fields["schema_version"], &out.SchemaVersion) != nil || out.SchemaVersion != 1 ||
		json.Unmarshal(fields["fingerprint"], &out.Fingerprint) != nil || !validSettingsFingerprint(out.Fingerprint) ||
		json.Unmarshal(fields["effective_model_id"], &out.EffectiveModelID) != nil || json.Unmarshal(fields["effective_permission_mode_id"], &out.EffectivePermissionModeID) != nil ||
		json.Unmarshal(fields["model_change"], &out.ModelChange) != nil || json.Unmarshal(fields["permission_change"], &out.PermissionChange) != nil ||
		json.Unmarshal(fields["model_read_only_reason"], &out.ModelReadOnlyReason) != nil || json.Unmarshal(fields["permission_read_only_reason"], &out.PermissionReadOnlyReason) != nil {
		return SettingsCapabilityPayload{}, errors.New("invalid settings capability payload")
	}
	if !validSettingsChoices(out.Models, 1, 32) || !validSettingsChoices(out.PermissionModes, 1, 16) ||
		!validSettingsIdentifier(out.EffectiveModelID) || !validSettingsIdentifier(out.EffectivePermissionModeID) ||
		!settingsChoiceContains(out.Models, out.EffectiveModelID) || !settingsChoiceContains(out.PermissionModes, out.EffectivePermissionModeID) ||
		!validSettingsChangeMode(out.ModelChange, out.ModelReadOnlyReason, len(out.Models)) || !validSettingsChangeMode(out.PermissionChange, out.PermissionReadOnlyReason, len(out.PermissionModes)) {
		return SettingsCapabilityPayload{}, errors.New("invalid settings capability payload")
	}
	if out.Fingerprint != settingsCapabilityFingerprint(out) {
		return SettingsCapabilityPayload{}, errors.New("settings capability fingerprint mismatch")
	}
	return out, nil
}

func decodeSettingsCapabilityChoices(data json.RawMessage) ([]SettingsCapabilityChoice, error) {
	var entries []json.RawMessage
	if json.Unmarshal(data, &entries) != nil {
		return nil, errors.New("invalid settings capability choices")
	}
	choices := make([]SettingsCapabilityChoice, len(entries))
	for index, entry := range entries {
		fields, err := strictObject(entry)
		if err != nil || len(fields) != 2 || fields["id"] == nil || fields["label"] == nil ||
			json.Unmarshal(fields["id"], &choices[index].ID) != nil || json.Unmarshal(fields["label"], &choices[index].Label) != nil {
			return nil, errors.New("invalid settings capability choice")
		}
		for key := range fields {
			if key != "id" && key != "label" {
				return nil, errors.New("invalid settings capability choice")
			}
		}
	}
	return choices, nil
}

// DecodeSettingsEffectivePayload validates the exact terminal result grammar
// without allowing the Adapter to select durable outcome metadata itself.
func DecodeSettingsEffectivePayload(payload json.RawMessage) (SettingsEffectivePayload, error) {
	fields, err := strictObject(payload)
	if err != nil || len(fields) != 7 {
		return SettingsEffectivePayload{}, errors.New("invalid settings effective payload")
	}
	allowed := map[string]bool{"cmd_id": true, "request_fingerprint": true, "effective_fingerprint": true, "outcome": true, "effective_model_id": true, "effective_permission_mode_id": true, "reason_code": true}
	for key := range fields {
		if !allowed[key] {
			return SettingsEffectivePayload{}, errors.New("invalid settings effective payload")
		}
	}
	var out SettingsEffectivePayload
	if json.Unmarshal(fields["cmd_id"], &out.CommandID) != nil || !validSettingsCommandID(out.CommandID) ||
		json.Unmarshal(fields["request_fingerprint"], &out.RequestFingerprint) != nil || !validSettingsFingerprint(out.RequestFingerprint) ||
		json.Unmarshal(fields["effective_fingerprint"], &out.EffectiveFingerprint) != nil || !validSettingsFingerprint(out.EffectiveFingerprint) ||
		json.Unmarshal(fields["outcome"], &out.Outcome) != nil || !validSettingsOutcome(out.Outcome) ||
		json.Unmarshal(fields["effective_model_id"], &out.EffectiveModelID) != nil || !validSettingsIdentifier(out.EffectiveModelID) ||
		json.Unmarshal(fields["effective_permission_mode_id"], &out.EffectivePermissionModeID) != nil || !validSettingsIdentifier(out.EffectivePermissionModeID) ||
		json.Unmarshal(fields["reason_code"], &out.ReasonCode) != nil {
		return SettingsEffectivePayload{}, errors.New("invalid settings effective payload")
	}
	if (out.Outcome == "applied" && out.ReasonCode != nil) || (out.Outcome != "applied" && !validSettingsReasonPointer(out.ReasonCode)) {
		return SettingsEffectivePayload{}, errors.New("invalid settings effective payload")
	}
	return out, nil
}

// DecodeRunControlCapabilityPayload accepts only the exact v2 durable
// capability grammar before a Hub can update the run-control ledger.
func DecodeRunControlCapabilityPayload(payload json.RawMessage) (RunControlCapabilityPayload, error) {
	if len(payload) == 0 || len(payload) > MaxEventPayloadBytes {
		return RunControlCapabilityPayload{}, errors.New("invalid run-control capability payload")
	}
	fields, err := strictObject(payload)
	if err != nil || len(fields) != 3 {
		return RunControlCapabilityPayload{}, errors.New("invalid run-control capability payload")
	}
	for key := range fields {
		if key != "schema_version" && key != "interrupt_supported" && key != "stop_supported" {
			return RunControlCapabilityPayload{}, errors.New("invalid run-control capability payload")
		}
	}
	var out RunControlCapabilityPayload
	if fields["schema_version"] == nil || fields["interrupt_supported"] == nil || fields["stop_supported"] == nil ||
		json.Unmarshal(fields["schema_version"], &out.SchemaVersion) != nil ||
		json.Unmarshal(fields["interrupt_supported"], &out.InterruptSupported) != nil ||
		json.Unmarshal(fields["stop_supported"], &out.StopSupported) != nil || out.SchemaVersion != 1 {
		return RunControlCapabilityPayload{}, errors.New("invalid run-control capability payload")
	}
	if !validRunControlBoolean(fields["interrupt_supported"]) || !validRunControlBoolean(fields["stop_supported"]) {
		return RunControlCapabilityPayload{}, errors.New("invalid run-control capability payload")
	}
	return out, nil
}

func validRunControlBoolean(value json.RawMessage) bool {
	value = bytes.TrimSpace(value)
	return bytes.Equal(value, []byte("true")) || bytes.Equal(value, []byte("false"))
}

// DecodeRunControlOutcomePayload accepts the exact public completion proposal.
// Reservation and writer fencing stay in Store metadata rather than this frame.
func DecodeRunControlOutcomePayload(payload json.RawMessage) (RunControlOutcomePayload, error) {
	if len(payload) == 0 || len(payload) > MaxEventPayloadBytes {
		return RunControlOutcomePayload{}, errors.New("invalid run-control outcome payload")
	}
	fields, err := strictObject(payload)
	if err != nil || len(fields) != 5 {
		return RunControlOutcomePayload{}, errors.New("invalid run-control outcome payload")
	}
	for key := range fields {
		if key != "cmd_id" && key != "operation" && key != "outcome" && key != "completion_state" && key != "reason_code" {
			return RunControlOutcomePayload{}, errors.New("invalid run-control outcome payload")
		}
	}
	var out RunControlOutcomePayload
	if fields["cmd_id"] == nil || fields["operation"] == nil || fields["outcome"] == nil || fields["completion_state"] == nil || fields["reason_code"] == nil ||
		json.Unmarshal(fields["cmd_id"], &out.CommandID) != nil || !validSettingsCommandID(out.CommandID) ||
		json.Unmarshal(fields["operation"], &out.Operation) != nil || (out.Operation != "interrupt" && out.Operation != "stop") ||
		json.Unmarshal(fields["outcome"], &out.Outcome) != nil || !validRunControlOutcome(out.Outcome) ||
		json.Unmarshal(fields["completion_state"], &out.CompletionState) != nil ||
		json.Unmarshal(fields["reason_code"], &out.ReasonCode) != nil {
		return RunControlOutcomePayload{}, errors.New("invalid run-control outcome payload")
	}
	if out.Outcome == "completed" {
		want := "ready"
		if out.Operation == "stop" {
			want = "ended"
		}
		if out.CompletionState == nil || *out.CompletionState != want || out.ReasonCode != nil {
			return RunControlOutcomePayload{}, errors.New("invalid run-control outcome payload")
		}
		return out, nil
	}
	if out.CompletionState != nil || !validSettingsReasonPointer(out.ReasonCode) {
		return RunControlOutcomePayload{}, errors.New("invalid run-control outcome payload")
	}
	return out, nil
}

func validRunControlOutcome(value string) bool {
	return value == "completed" || value == "rejected" || value == "timeout" || value == "unsupported" || value == "outcome_unknown"
}

func validSettingsChoices(choices []SettingsCapabilityChoice, min, max int) bool {
	if len(choices) < min || len(choices) > max {
		return false
	}
	last := ""
	for _, choice := range choices {
		if !validSettingsIdentifier(choice.ID) || !validSettingsLabel(choice.Label) || (last != "" && choice.ID <= last) {
			return false
		}
		last = choice.ID
	}
	return true
}

func validSettingsLabel(value string) bool {
	if len(value) < 1 || len(value) > 128 || !utf8.ValidString(value) || norm.NFC.String(value) != value {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func settingsChoiceContains(choices []SettingsCapabilityChoice, id string) bool {
	for _, choice := range choices {
		if choice.ID == id {
			return true
		}
	}
	return false
}

func validSettingsChangeMode(mode string, reason *string, choices int) bool {
	if mode == "allowed" {
		return choices >= 2 && reason == nil
	}
	return mode == "read_only" && reason != nil && len(*reason) >= 1 && len(*reason) <= 64 && validSettingsReason(*reason)
}

func validSettingsReason(value string) bool {
	if len(value) == 0 || len(value) > 64 || value[0] < 'a' || value[0] > 'z' {
		return false
	}
	for _, ch := range value[1:] {
		if !((ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '_') {
			return false
		}
	}
	return true
}

func validSettingsReasonPointer(value *string) bool {
	return value != nil && validSettingsReason(*value)
}

// DecodeFileReferenceCapabilityPayload verifies the exact, bounded v2
// capability proposal before the Hub commits it to its durable ledger.
func DecodeFileReferenceCapabilityPayload(payload json.RawMessage) (FileReferenceCapabilityPayload, error) {
	fields, err := strictObject(payload)
	if err != nil || len(fields) != 6 {
		return FileReferenceCapabilityPayload{}, errors.New("invalid file-reference capability")
	}
	for key := range fields {
		if key != "schema_version" && key != "fingerprint" && key != "max_references" && key != "max_total_bytes" && key != "file" && key != "image" {
			return FileReferenceCapabilityPayload{}, errors.New("invalid file-reference capability")
		}
	}
	var capability FileReferenceCapabilityPayload
	if fields["schema_version"] == nil || fields["fingerprint"] == nil || fields["max_references"] == nil || fields["max_total_bytes"] == nil || fields["file"] == nil || fields["image"] == nil ||
		json.Unmarshal(fields["schema_version"], &capability.SchemaVersion) != nil || json.Unmarshal(fields["fingerprint"], &capability.Fingerprint) != nil ||
		json.Unmarshal(fields["max_references"], &capability.MaxReferences) != nil || json.Unmarshal(fields["max_total_bytes"], &capability.MaxTotalBytes) != nil ||
		decodeFileReferenceDisposition(fields["file"], &capability.File) != nil || decodeFileReferenceImage(fields["image"], &capability.Image) != nil ||
		!validFileReferenceCapability(capability) || capability.Fingerprint != FileReferenceCapabilityFingerprint(capability) {
		return FileReferenceCapabilityPayload{}, errors.New("invalid file-reference capability")
	}
	return capability, nil
}

// FileReferenceCapabilityFingerprint returns the versioned canonical digest
// specified for a valid capability. Invalid values return an empty string.
func FileReferenceCapabilityFingerprint(capability FileReferenceCapabilityPayload) string {
	if capability.SchemaVersion != 1 || capability.MaxReferences < 1 || capability.MaxReferences > 8 || capability.MaxTotalBytes < 1 || capability.MaxTotalBytes > 10485760 ||
		!validFileReferenceDisposition(capability.File, capability.MaxTotalBytes) || !validFileReferenceImage(capability.Image, capability.MaxTotalBytes) {
		return ""
	}
	data := make([]byte, 0, 256)
	data = append(data, []byte("agentwharf.file-reference-capability.v1")...)
	data = append(data, 0, 1, byte(capability.MaxReferences))
	var number [8]byte
	binary.BigEndian.PutUint64(number[:], uint64(capability.MaxTotalBytes))
	data = append(data, number[:]...)
	appendDisposition := func(value FileReferenceDispositionCapability) {
		if value.Mode == "allowed" {
			data = append(data, 1)
			binary.BigEndian.PutUint64(number[:], uint64(*value.MaxBytes))
			data = append(data, number[:]...)
			appendFileReferenceString(&data, "")
			return
		}
		data = append(data, 0)
		binary.BigEndian.PutUint64(number[:], 0)
		data = append(data, number[:]...)
		appendFileReferenceString(&data, *value.Reason)
	}
	appendDisposition(capability.File)
	if capability.Image.Mode == "allowed" {
		data = append(data, 1)
		binary.BigEndian.PutUint64(number[:], uint64(*capability.Image.MaxBytes))
		data = append(data, number[:]...)
	} else {
		data = append(data, 0)
		binary.BigEndian.PutUint64(number[:], 0)
		data = append(data, number[:]...)
	}
	data = append(data, byte(len(capability.Image.MediaTypes)))
	for _, mediaType := range capability.Image.MediaTypes {
		appendFileReferenceString(&data, mediaType)
	}
	if capability.Image.Mode == "allowed" {
		appendFileReferenceString(&data, "")
	} else {
		appendFileReferenceString(&data, *capability.Image.Reason)
	}
	digest := sha256.Sum256(data)
	return fmt.Sprintf("sha256:%x", digest[:])
}

func appendFileReferenceString(data *[]byte, value string) {
	var length [2]byte
	binary.BigEndian.PutUint16(length[:], uint16(len(value)))
	*data = append(*data, length[:]...)
	*data = append(*data, value...)
}

func validFileReferenceCapability(capability FileReferenceCapabilityPayload) bool {
	return capability.SchemaVersion == 1 && validFileReferenceFingerprint(capability.Fingerprint) &&
		FileReferenceCapabilityFingerprint(capability) != ""
}

func decodeFileReferenceDisposition(raw json.RawMessage, value *FileReferenceDispositionCapability) error {
	fields, err := strictObject(raw)
	if err != nil || len(fields) != 3 || fields["mode"] == nil || fields["max_bytes"] == nil || fields["reason"] == nil ||
		json.Unmarshal(fields["mode"], &value.Mode) != nil || json.Unmarshal(fields["max_bytes"], &value.MaxBytes) != nil || json.Unmarshal(fields["reason"], &value.Reason) != nil {
		return errors.New("invalid file-reference disposition")
	}
	for key := range fields {
		if key != "mode" && key != "max_bytes" && key != "reason" {
			return errors.New("invalid file-reference disposition")
		}
	}
	return nil
}

func decodeFileReferenceImage(raw json.RawMessage, value *FileReferenceImageCapability) error {
	fields, err := strictObject(raw)
	if err != nil || len(fields) != 4 || fields["mode"] == nil || fields["max_bytes"] == nil || fields["media_types"] == nil || fields["reason"] == nil ||
		json.Unmarshal(fields["mode"], &value.Mode) != nil || json.Unmarshal(fields["max_bytes"], &value.MaxBytes) != nil || json.Unmarshal(fields["media_types"], &value.MediaTypes) != nil || json.Unmarshal(fields["reason"], &value.Reason) != nil {
		return errors.New("invalid file-reference image")
	}
	for key := range fields {
		if key != "mode" && key != "max_bytes" && key != "media_types" && key != "reason" {
			return errors.New("invalid file-reference image")
		}
	}
	return nil
}

func validFileReferenceDisposition(value FileReferenceDispositionCapability, total int64) bool {
	if value.Mode == "allowed" {
		return value.Reason == nil && value.MaxBytes != nil && *value.MaxBytes >= 1 && *value.MaxBytes <= total
	}
	return value.Mode == "unsupported" && value.MaxBytes == nil && validFileReferenceReason(value.Reason)
}

func validFileReferenceImage(value FileReferenceImageCapability, total int64) bool {
	if value.Mode == "allowed" {
		if value.Reason != nil || value.MaxBytes == nil || *value.MaxBytes < 1 || *value.MaxBytes > total || len(value.MediaTypes) < 1 || len(value.MediaTypes) > 4 {
			return false
		}
		last := ""
		for _, mediaType := range value.MediaTypes {
			if (mediaType != "image/png" && mediaType != "image/jpeg" && mediaType != "image/webp" && mediaType != "image/gif") || (last != "" && last >= mediaType) {
				return false
			}
			last = mediaType
		}
		return true
	}
	return value.Mode == "unsupported" && value.MaxBytes == nil && len(value.MediaTypes) == 0 && validFileReferenceReason(value.Reason)
}

func validFileReferenceReason(value *string) bool {
	return value != nil && validSettingsReason(*value)
}

// DecodeFileReferenceSendPayload validates only the new v2 reference shape.
// Text-only session.send payloads retain their established permissive shape.
func DecodeFileReferenceSendPayload(payload json.RawMessage) (FileReferenceSendPayload, error) {
	fields, err := strictObject(payload)
	if err != nil {
		return FileReferenceSendPayload{}, errors.New("invalid session.send payload")
	}
	content, found := fields["content"]
	if !found {
		return FileReferenceSendPayload{}, nil
	}
	var parts []json.RawMessage
	if json.Unmarshal(content, &parts) != nil {
		return FileReferenceSendPayload{}, nil
	}
	result := FileReferenceSendPayload{}
	nonReferenceParts := make([]map[string]json.RawMessage, 0, len(parts))
	canonicalParts := make([]json.RawMessage, 0, len(parts))
	for _, part := range parts {
		partFields, err := strictObject(part)
		if err != nil || partFields["kind"] == nil {
			return FileReferenceSendPayload{}, errors.New("invalid session.send content")
		}
		var kind string
		if json.Unmarshal(partFields["kind"], &kind) != nil {
			return FileReferenceSendPayload{}, errors.New("invalid session.send content")
		}
		canonicalPart, err := json.Marshal(partFields)
		if err != nil {
			return FileReferenceSendPayload{}, errors.New("invalid session.send content")
		}
		canonicalParts = append(canonicalParts, canonicalPart)
		if kind != "file_reference" {
			nonReferenceParts = append(nonReferenceParts, partFields)
			continue
		}
		result.HasReferences = true
		reference, err := decodeFileReferencePart(partFields)
		if err != nil {
			return FileReferenceSendPayload{}, err
		}
		result.ReferenceCount++
		result.References = append(result.References, reference)
	}
	if !result.HasReferences {
		return result, nil
	}
	for _, part := range nonReferenceParts {
		var text string
		if len(part) != 2 || part["kind"] == nil || part["text"] == nil || json.Unmarshal(part["text"], &text) != nil {
			return FileReferenceSendPayload{}, errors.New("invalid file-reference session.send")
		}
		var kind string
		if json.Unmarshal(part["kind"], &kind) != nil || kind != "text" {
			return FileReferenceSendPayload{}, errors.New("invalid file-reference session.send")
		}
	}
	if len(fields) != 2 || fields["capability_fingerprint"] == nil || json.Unmarshal(fields["capability_fingerprint"], &result.CapabilityFingerprint) != nil || !validFileReferenceFingerprint(result.CapabilityFingerprint) || result.ReferenceCount > 8 {
		return FileReferenceSendPayload{}, errors.New("invalid file-reference session.send")
	}
	for key := range fields {
		if key != "content" && key != "capability_fingerprint" {
			return FileReferenceSendPayload{}, errors.New("invalid file-reference session.send")
		}
	}
	canonical, err := json.Marshal(struct {
		Content               []json.RawMessage `json:"content"`
		CapabilityFingerprint string            `json:"capability_fingerprint"`
	}{Content: canonicalParts, CapabilityFingerprint: result.CapabilityFingerprint})
	if err != nil || len(canonical) > 8192 {
		return FileReferenceSendPayload{}, errors.New("invalid file-reference session.send")
	}
	digest := sha256.Sum256(canonical)
	result.RequestFingerprint = fmt.Sprintf("sha256:%x", digest[:])
	return result, nil
}

func decodeFileReferencePart(fields map[string]json.RawMessage) (FileReferencePart, error) {
	if len(fields) != 7 {
		return FileReferencePart{}, errors.New("invalid file-reference part")
	}
	for key := range fields {
		if key != "kind" && key != "disposition" && key != "path" && key != "version" && key != "content_digest" && key != "bytes" && key != "media_type" {
			return FileReferencePart{}, errors.New("invalid file-reference part")
		}
	}
	var disposition, path, version, digest string
	var bytes int64
	var mediaType *string
	if json.Unmarshal(fields["disposition"], &disposition) != nil || json.Unmarshal(fields["path"], &path) != nil || json.Unmarshal(fields["version"], &version) != nil ||
		json.Unmarshal(fields["content_digest"], &digest) != nil || json.Unmarshal(fields["bytes"], &bytes) != nil || json.Unmarshal(fields["media_type"], &mediaType) != nil ||
		(disposition != "file" && disposition != "image") || !validFileReferencePath(path) || !validFileReferenceOpaqueVersion(version) || !validFileReferenceFingerprint(digest) || bytes < 0 || bytes > 10485760 || !validFileReferenceMediaType(mediaType) {
		return FileReferencePart{}, errors.New("invalid file-reference part")
	}
	return FileReferencePart{Disposition: disposition, Bytes: bytes, MediaType: mediaType}, nil
}

func validFileReferencePath(value string) bool {
	if len(value) < 1 || len(value) > 1024 || !utf8.ValidString(value) || strings.HasPrefix(value, "/") || strings.Contains(value, "\\") || strings.ContainsRune(value, 0) {
		return false
	}
	parts := strings.Split(value, "/")
	if len(parts) > 32 {
		return false
	}
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}
	return true
}

func validFileReferenceOpaqueVersion(value string) bool {
	return len(value) >= 1 && len(value) <= 256 && utf8.ValidString(value) && !strings.ContainsRune(value, 0)
}

func validFileReferenceFingerprint(value string) bool {
	if len(value) != len("sha256:")+64 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	for _, ch := range value[len("sha256:"):] {
		if !(ch >= '0' && ch <= '9') && !(ch >= 'a' && ch <= 'f') {
			return false
		}
	}
	return true
}

func validFileReferenceMediaType(value *string) bool {
	if value == nil {
		return true
	}
	if len(*value) < 1 || len(*value) > 127 {
		return false
	}
	for _, ch := range *value {
		if ch < 0x20 || ch > 0x7e {
			return false
		}
	}
	return true
}

func DecodeFileReferenceOutcomePayload(payload json.RawMessage) (FileReferenceOutcomePayload, error) {
	fields, err := strictObject(payload)
	if err != nil || len(fields) != 5 {
		return FileReferenceOutcomePayload{}, errors.New("invalid file-reference outcome")
	}
	for key := range fields {
		if key != "message_id" && key != "cmd_id" && key != "outcome" && key != "reference_index" && key != "reason" {
			return FileReferenceOutcomePayload{}, errors.New("invalid file-reference outcome")
		}
	}
	var result FileReferenceOutcomePayload
	if fields["message_id"] == nil || fields["cmd_id"] == nil || fields["outcome"] == nil || fields["reference_index"] == nil || fields["reason"] == nil ||
		json.Unmarshal(fields["message_id"], &result.MessageID) != nil || json.Unmarshal(fields["cmd_id"], &result.CommandID) != nil || json.Unmarshal(fields["outcome"], &result.Outcome) != nil ||
		json.Unmarshal(fields["reference_index"], &result.ReferenceIndex) != nil || json.Unmarshal(fields["reason"], &result.Reason) != nil ||
		!validSettingsCommandID(result.MessageID) || !validSettingsCommandID(result.CommandID) ||
		(result.Outcome != "delivered" && result.Outcome != "rejected" && result.Outcome != "outcome_unknown") {
		return FileReferenceOutcomePayload{}, errors.New("invalid file-reference outcome")
	}
	if result.Outcome == "delivered" && (result.ReferenceIndex != nil || result.Reason != nil) {
		return FileReferenceOutcomePayload{}, errors.New("invalid file-reference outcome")
	}
	if result.Outcome == "outcome_unknown" && (result.ReferenceIndex != nil || result.Reason == nil || (*result.Reason != "delivery_unconfirmed" && *result.Reason != "writer_lost" && *result.Reason != "adapter_deadline")) {
		return FileReferenceOutcomePayload{}, errors.New("invalid file-reference outcome")
	}
	if result.Outcome == "rejected" && (result.ReferenceIndex == nil || *result.ReferenceIndex < 0 || result.Reason == nil || !validFileReferenceRejectionReason(*result.Reason)) {
		return FileReferenceOutcomePayload{}, errors.New("invalid file-reference outcome")
	}
	return result, nil
}

func validFileReferenceRejectionReason(reason string) bool {
	switch reason {
	case "missing", "removed", "stale_reference", "size_changed", "media_type_changed", "image_unsupported", "provider_unsupported", "unsafe_path", "quarantined", "access_denied":
		return true
	default:
		return false
	}
}

func validSettingsOutcome(value string) bool {
	switch value {
	case "applied", "rejected", "timeout", "unsupported", "stale_capability", "outcome_unknown", "mismatched_effective":
		return true
	default:
		return false
	}
}

func settingsCapabilityFingerprint(capability SettingsCapabilityPayload) string {
	data := make([]byte, 0, 512)
	appendString := func(value string) {
		var length [2]byte
		binary.BigEndian.PutUint16(length[:], uint16(len(value)))
		data = append(data, length[:]...)
		data = append(data, value...)
	}
	data = append(data, []byte("agentwharf.settings-capability.v1")...)
	data = append(data, 0, 1, byte(len(capability.Models)))
	for _, choice := range capability.Models {
		appendString(choice.ID)
		appendString(choice.Label)
	}
	data = append(data, byte(len(capability.PermissionModes)))
	for _, choice := range capability.PermissionModes {
		appendString(choice.ID)
		appendString(choice.Label)
	}
	appendString(capability.EffectiveModelID)
	appendString(capability.EffectivePermissionModeID)
	if capability.ModelChange == "allowed" {
		data = append(data, 1)
	} else {
		data = append(data, 0)
	}
	if capability.PermissionChange == "allowed" {
		data = append(data, 1)
	} else {
		data = append(data, 0)
	}
	if capability.ModelReadOnlyReason == nil {
		appendString("")
	} else {
		appendString(*capability.ModelReadOnlyReason)
	}
	if capability.PermissionReadOnlyReason == nil {
		appendString("")
	} else {
		appendString(*capability.PermissionReadOnlyReason)
	}
	digest := sha256.Sum256(data)
	return fmt.Sprintf("sha256:%x", digest[:])
}

func decodeSettingsDeliveryExecute(data []byte) (Frame, error) {
	fields, err := targetJoinFields(data, FrameSettingsDeliveryExecute, "session_id", "cmd_id", "reservation_version", "operation_timeout_ms")
	if err != nil {
		return nil, errors.New("decode settings delivery: invalid frame")
	}
	var out SettingsDeliveryExecute
	if json.Unmarshal(fields["session_id"], &out.SessionID) != nil || json.Unmarshal(fields["cmd_id"], &out.CommandID) != nil ||
		json.Unmarshal(fields["reservation_version"], &out.ReservationVersion) != nil || json.Unmarshal(fields["operation_timeout_ms"], &out.OperationTimeoutMS) != nil ||
		!validSettingsCommandID(out.SessionID) || !validSettingsCommandID(out.CommandID) || out.ReservationVersion < 1 || out.OperationTimeoutMS != 30000 {
		return nil, errors.New("decode settings delivery: invalid frame")
	}
	return &out, nil
}

func validSettingsCommandID(value string) bool {
	return len(value) > 0 && len(value) <= MaxSettingsCommandIDBytes
}

func validSettingsIdentifier(value string) bool {
	if len(value) == 0 || len(value) > MaxSettingsIdentifierBytes {
		return false
	}
	for index := 0; index < len(value); index += 1 {
		ch := value[index]
		if index == 0 {
			if !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9')) {
				return false
			}
			continue
		}
		if !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || strings.ContainsRune("._:/-", rune(ch))) {
			return false
		}
	}
	return true
}

func validSettingsFingerprint(value string) bool {
	if len(value) != len("sha256:")+64 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	for _, ch := range value[len("sha256:"):] {
		if !((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f')) {
			return false
		}
	}
	return true
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
