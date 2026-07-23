package core

import (
	"errors"
	"fmt"
	"sync"

	"github.com/winghv/agentwharf/protocol"
)

var (
	ErrInvalidAdapterConnectionConfig = errors.New("invalid adapter connection config")
	ErrInvalidHelloAck                = errors.New("invalid hello ack")
)

type AdapterConnectionConfig struct {
	SessionID       string
	Provider        string
	Token           string
	ProtocolVersion int
}

type AdapterConnectionState struct {
	cfg AdapterConnectionConfig

	mu       sync.Mutex
	accepted bool
}

func NewAdapterConnectionState(cfg AdapterConnectionConfig) (*AdapterConnectionState, error) {
	if cfg.SessionID == "" {
		return nil, fmt.Errorf("%w: session_id is required", ErrInvalidAdapterConnectionConfig)
	}
	if cfg.Provider == "" {
		return nil, fmt.Errorf("%w: provider is required", ErrInvalidAdapterConnectionConfig)
	}
	if cfg.Token == "" {
		return nil, fmt.Errorf("%w: token is required", ErrInvalidAdapterConnectionConfig)
	}
	if cfg.ProtocolVersion == 0 {
		cfg.ProtocolVersion = protocol.ProtocolVersion
	}
	if cfg.ProtocolVersion != protocol.ProtocolVersion && cfg.ProtocolVersion != protocol.ProtocolVersionV2 {
		return nil, fmt.Errorf("%w: unsupported protocol version", ErrInvalidAdapterConnectionConfig)
	}
	return &AdapterConnectionState{cfg: cfg}, nil
}

func (s *AdapterConnectionState) Hello() protocol.Hello {
	s.mu.Lock()
	resume := s.accepted
	s.mu.Unlock()

	return protocol.Hello{
		ProtocolVersion: s.cfg.ProtocolVersion,
		Role:            protocol.RoleAdapter,
		Token:           s.cfg.Token,
		SessionID:       s.cfg.SessionID,
		Provider:        s.cfg.Provider,
		Resume:          resume,
	}
}

func (s *AdapterConnectionState) MarkAccepted(ack protocol.HelloAck) (protocol.SessionSummary, error) {
	if ack.ProtocolVersion != s.cfg.ProtocolVersion {
		return protocol.SessionSummary{}, fmt.Errorf("%w: protocol version %d", ErrInvalidHelloAck, ack.ProtocolVersion)
	}
	if s.cfg.ProtocolVersion == protocol.ProtocolVersion && (ack.Capabilities != nil || ack.ConnectionAuthority != nil) {
		return protocol.SessionSummary{}, fmt.Errorf("%w: v1 acknowledgement includes capabilities", ErrInvalidHelloAck)
	}
	if s.cfg.ProtocolVersion == protocol.ProtocolVersionV2 && !validConnectionAuthorityReceipt(ack.ConnectionAuthority, s.cfg.SessionID) {
		return protocol.SessionSummary{}, fmt.Errorf("%w: v2 acknowledgement omits or corrupts connection authority", ErrInvalidHelloAck)
	}
	if len(ack.Sessions) != 1 {
		return protocol.SessionSummary{}, fmt.Errorf("%w: expected one session summary", ErrInvalidHelloAck)
	}

	summary := ack.Sessions[0]
	if summary.SessionID != s.cfg.SessionID {
		return protocol.SessionSummary{}, fmt.Errorf("%w: session_id %q", ErrInvalidHelloAck, summary.SessionID)
	}
	if summary.Provider != "" && summary.Provider != s.cfg.Provider {
		return protocol.SessionSummary{}, fmt.Errorf("%w: provider %q", ErrInvalidHelloAck, summary.Provider)
	}

	s.mu.Lock()
	s.accepted = true
	s.mu.Unlock()
	return summary, nil
}

func validConnectionAuthorityReceipt(receipt *protocol.ConnectionAuthorityReceipt, sessionID string) bool {
	return receipt != nil && receipt.SessionID == sessionID && receipt.ConnectionEpoch > 0 && receipt.CredentialGeneration > 0 &&
		receipt.AcceptedFence > 0 && receipt.WriterLeaseID != "" && receipt.ExpiresAt > 0
}
