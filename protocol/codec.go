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
	FrameHello       FrameName = "hello"
	FrameHelloAck    FrameName = "hello.ack"
	FrameEvent       FrameName = "event"
	FrameCommand     FrameName = "command"
	FrameCommandAck  FrameName = "command.ack"
	FramePing        FrameName = "ping"
	FramePong        FrameName = "pong"
	FrameError       FrameName = "error"
	FrameHistoryPage FrameName = "history.page"
)

const (
	HistoryPageMaxLimit  = 100
	MaxEventPayloadBytes = 64 * 1024
	MaxAttachGrantBytes  = 64 * 1024
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
	Type      string          `json:"type"`
	SessionID string          `json:"session_id"`
	Seq       *int64          `json:"seq,omitempty"`
	Time      int64           `json:"time"`
	Payload   json.RawMessage `json:"payload"`
}

func (*Event) FrameName() FrameName { return FrameEvent }

func (e *Event) Durable() bool {
	return e.Seq != nil
}

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
		return decodeInto(data, &Event{})
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
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnknownFrame, env.Frame)
	}
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
		case "presence", "agent.activity", "log.tail", "resource.sample", "session.idle_warning", "x.vm.idle_warning":
			return false
		}
	}
	switch eventType {
	case "session.idle_warning":
		return version == ProtocolVersion && !durable
	case "x.vm.idle_warning":
		return false
	default:
		return true
	}
}

func PeerEventTypeAllowed(eventType string) bool {
	return eventType != "session.idle_warning" && eventType != "x.vm.idle_warning"
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
