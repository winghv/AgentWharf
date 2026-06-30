package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
)

const ProtocolVersion = 1

var (
	ErrNoCompatibleVersion = errors.New("no compatible protocol version")
	ErrUnknownFrame        = errors.New("unknown frame")
)

type FrameName string

const (
	FrameHello      FrameName = "hello"
	FrameHelloAck   FrameName = "hello.ack"
	FrameEvent      FrameName = "event"
	FrameCommand    FrameName = "command"
	FrameCommandAck FrameName = "command.ack"
	FramePing       FrameName = "ping"
	FramePong       FrameName = "pong"
	FrameError      FrameName = "error"
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
	ProtocolVersion int              `json:"protocol_version"`
	Sessions        []SessionSummary `json:"sessions"`
}

func (*HelloAck) FrameName() FrameName { return FrameHelloAck }

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
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnknownFrame, env.Frame)
	}
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
