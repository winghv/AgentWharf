package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/winghv/agentwharf/adapter/core"
	"github.com/winghv/agentwharf/protocol"
	"nhooyr.io/websocket"
)

const (
	hubReconnectMinDelay = 250 * time.Millisecond
	hubReconnectMaxDelay = 15 * time.Second
)

type adapterCredentialSet struct {
	mu         sync.Mutex
	active     string
	activeGen  int64
	prior      string
	pending    string
	pendingGen int64
}

func newAdapterCredentialSet(token string, authority *protocol.ConnectionAuthorityReceipt) *adapterCredentialSet {
	generation := int64(1)
	if authority != nil && authority.CredentialGeneration > 0 {
		generation = authority.CredentialGeneration
	}
	return &adapterCredentialSet{active: token, activeGen: generation}
}

func (s *adapterCredentialSet) recordPending(token string, generation int64) {
	if s == nil || token == "" || generation < 1 {
		return
	}
	s.mu.Lock()
	s.pending, s.pendingGen = token, generation
	s.mu.Unlock()
}

func (s *adapterCredentialSet) activate(generation int64) {
	if s == nil || generation < 1 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pending == "" || s.pendingGen != generation {
		return
	}
	s.prior = s.active
	s.active, s.activeGen = s.pending, s.pendingGen
	s.pending, s.pendingGen = "", 0
}

func (s *adapterCredentialSet) candidates() []string {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]string, 0, 3)
	for _, token := range []string{s.pending, s.active, s.prior} {
		if token == "" {
			continue
		}
		duplicate := false
		for _, existing := range result {
			if existing == token {
				duplicate = true
				break
			}
		}
		if !duplicate {
			result = append(result, token)
		}
	}
	return result
}

func (s *adapterCredentialSet) converge(token string, generation int64) {
	if s == nil || token == "" || generation < 1 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case token == s.pending && generation == s.pendingGen:
		s.prior = s.active
		s.active, s.activeGen = s.pending, s.pendingGen
		s.pending, s.pendingGen = "", 0
	case token == s.active && generation == s.activeGen:
		// The server did not activate the pending credential. Drop the abandoned
		// candidate so the rotation manager can request a fresh lineage step.
		s.pending, s.pendingGen = "", 0
	case token == s.prior:
		s.active, s.activeGen = s.prior, generation
		s.prior, s.pending, s.pendingGen = "", "", 0
	default:
		return
	}
}

func (s *adapterCredentialSet) pendingGeneration() int64 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pendingGen
}

// hubConnection owns a replaceable Hub transport. Provider pipes and process
// supervision live outside it, so a temporary network failure cannot terminate
// the Provider or create a new Session.
type hubConnection struct {
	cfg         wrapConfig
	credentials *adapterCredentialSet

	connMu sync.RWMutex
	conn   *websocket.Conn

	reconnectMu  sync.Mutex
	writeMu      sync.Mutex
	pendingMu    sync.Mutex
	pending      map[string]*protocol.Event
	pendingOrder []string

	authorityMu sync.RWMutex
	authority   *protocol.ConnectionAuthorityReceipt

	reconnectProposalMu        sync.RWMutex
	reconnectProposalFactories map[string]func() (*protocol.Event, error)
}

func newHubConnection(cfg wrapConfig, conn *websocket.Conn, authority *protocol.ConnectionAuthorityReceipt) *hubConnection {
	return &hubConnection{
		cfg: cfg, conn: conn, authority: cloneConnectionAuthority(authority),
		credentials:                newAdapterCredentialSet(cfg.AdapterToken, authority),
		pending:                    make(map[string]*protocol.Event),
		reconnectProposalFactories: make(map[string]func() (*protocol.Event, error)),
	}
}

func (c *hubConnection) setReconnectProposalFactory(key string, factory func() (*protocol.Event, error)) func() {
	if c == nil || key == "" || factory == nil {
		return func() {}
	}
	c.reconnectProposalMu.Lock()
	c.reconnectProposalFactories[key] = factory
	c.reconnectProposalMu.Unlock()
	return func() {
		c.reconnectProposalMu.Lock()
		delete(c.reconnectProposalFactories, key)
		c.reconnectProposalMu.Unlock()
	}
}

func (c *hubConnection) freshReconnectProposals() ([]*protocol.Event, error) {
	c.reconnectProposalMu.RLock()
	factories := make([]func() (*protocol.Event, error), 0, len(c.reconnectProposalFactories))
	for _, factory := range c.reconnectProposalFactories {
		factories = append(factories, factory)
	}
	c.reconnectProposalMu.RUnlock()
	result := make([]*protocol.Event, 0, len(factories))
	for _, factory := range factories {
		event, err := factory()
		if err != nil {
			return nil, err
		}
		if event != nil {
			result = append(result, event)
		}
	}
	return result, nil
}

func cloneConnectionAuthority(authority *protocol.ConnectionAuthorityReceipt) *protocol.ConnectionAuthorityReceipt {
	if authority == nil {
		return nil
	}
	copy := *authority
	return &copy
}

func (c *hubConnection) currentAuthority() *protocol.ConnectionAuthorityReceipt {
	c.authorityMu.RLock()
	defer c.authorityMu.RUnlock()
	return cloneConnectionAuthority(c.authority)
}

func (c *hubConnection) setAuthority(authority *protocol.ConnectionAuthorityReceipt) {
	c.authorityMu.Lock()
	c.authority = cloneConnectionAuthority(authority)
	c.authorityMu.Unlock()
}

func (c *hubConnection) close() {
	c.connMu.Lock()
	conn := c.conn
	c.conn = nil
	c.connMu.Unlock()
	if conn != nil {
		_ = conn.Close(websocket.StatusNormalClosure, "")
	}
}

func (c *hubConnection) current() *websocket.Conn {
	c.connMu.RLock()
	defer c.connMu.RUnlock()
	return c.conn
}

func (c *hubConnection) write(ctx context.Context, frame protocol.Frame) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if event, ok := frame.(*protocol.Event); ok {
		if c.cfg.ProtocolVersion == protocol.ProtocolVersionV2 && !isCLIEventEphemeral(event.Type) && event.ProposalID == "" {
			proposalID, err := randomToken()
			if err != nil {
				return fmt.Errorf("generate event proposal id: %w", err)
			}
			event.ProposalID = proposalID
		}
		if err := validateHubEventFrame(event); err != nil {
			return err
		}
		if c.cfg.ProtocolVersion == protocol.ProtocolVersionV2 && !isCLIEventEphemeral(event.Type) {
			c.trackProposal(event)
		}
	}
	for {
		conn := c.current()
		if conn == nil {
			if err := c.reconnect(ctx, nil); err != nil {
				return err
			}
			continue
		}
		if err := writeCLIProtocolFrame(ctx, conn, frame); err == nil {
			return nil
		}
		if err := c.reconnect(ctx, conn); err != nil {
			return err
		}
	}
}

func validateHubEventFrame(event *protocol.Event) error {
	if event == nil {
		return errors.New("event is nil")
	}
	encoded, err := protocol.Encode(event)
	if err != nil {
		return fmt.Errorf("encode event: %w", err)
	}
	if len(encoded) > protocol.MaxWebSocketFrameBytes {
		return fmt.Errorf("event frame exceeds %d byte limit: %d bytes", protocol.MaxWebSocketFrameBytes, len(encoded))
	}
	return nil
}

func (c *hubConnection) read(ctx context.Context) (protocol.Frame, error) {
	for {
		conn := c.current()
		if conn == nil {
			if err := c.reconnect(ctx, nil); err != nil {
				return nil, err
			}
			continue
		}
		frame, err := readCLIProtocolFrame(ctx, conn)
		if err != nil {
			if err := c.reconnect(ctx, conn); err != nil {
				return nil, err
			}
			continue
		}
		if receipt, ok := frame.(*protocol.EventReceipt); ok {
			c.ackProposal(receipt.ProposalID)
		}
		return frame, nil
	}
}

func (c *hubConnection) trackProposal(event *protocol.Event) {
	copy := *event
	copy.Payload = append([]byte(nil), event.Payload...)
	c.pendingMu.Lock()
	if _, exists := c.pending[event.ProposalID]; !exists {
		c.pendingOrder = append(c.pendingOrder, event.ProposalID)
	}
	c.pending[event.ProposalID] = &copy
	c.pendingMu.Unlock()
}

func (c *hubConnection) ackProposal(proposalID string) {
	c.pendingMu.Lock()
	delete(c.pending, proposalID)
	c.pendingMu.Unlock()
}

func (c *hubConnection) proposals() []*protocol.Event {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	result := make([]*protocol.Event, 0, len(c.pending))
	order := c.pendingOrder[:0]
	for _, id := range c.pendingOrder {
		frame, ok := c.pending[id]
		if !ok {
			continue
		}
		order = append(order, id)
		copy := *frame
		copy.Payload = append([]byte(nil), frame.Payload...)
		result = append(result, &copy)
	}
	c.pendingOrder = order
	return result
}

func (c *hubConnection) reconnect(ctx context.Context, failed *websocket.Conn) error {
	c.reconnectMu.Lock()
	defer c.reconnectMu.Unlock()
	if current := c.current(); current != nil && current != failed {
		return nil
	}
	if failed != nil {
		_ = failed.Close(websocket.StatusGoingAway, "reconnecting")
	}

	delay := hubReconnectMinDelay
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var authFailures int
		candidates := c.credentials.candidates()
		for _, token := range candidates {
			conn, authority, authRejected, err := c.dialAndResume(ctx, token)
			if err != nil {
				if authRejected {
					authFailures++
				}
				continue
			}
			c.credentials.converge(token, authority.CredentialGeneration)
			c.setAuthority(authority)
			c.connMu.Lock()
			old := c.conn
			c.conn = conn
			c.connMu.Unlock()
			if old != nil && old != conn {
				_ = old.Close(websocket.StatusGoingAway, "replaced")
			}
			for _, proposal := range c.proposals() {
				if err := writeCLIProtocolFrame(ctx, conn, proposal); err != nil {
					_ = conn.Close(websocket.StatusGoingAway, "proposal replay failed")
					c.connMu.Lock()
					if c.conn == conn {
						c.conn = nil
					}
					c.connMu.Unlock()
					break
				}
			}
			if c.current() == conn {
				proposals, err := c.freshReconnectProposals()
				if err != nil {
					_ = conn.Close(websocket.StatusGoingAway, "reconnect proposal creation failed")
					c.connMu.Lock()
					if c.conn == conn {
						c.conn = nil
					}
					c.connMu.Unlock()
					continue
				}
				for _, proposal := range proposals {
					c.trackProposal(proposal)
					if err := writeCLIProtocolFrame(ctx, conn, proposal); err != nil {
						_ = conn.Close(websocket.StatusGoingAway, "reconnect proposal publish failed")
						c.connMu.Lock()
						if c.conn == conn {
							c.conn = nil
						}
						c.connMu.Unlock()
						break
					}
				}
			}
			if c.current() == conn {
				return nil
			}
		}
		if len(candidates) > 0 && authFailures == len(candidates) {
			return errClaimAuthRejection
		}
		jitter := time.Duration(rand.Int63n(int64(delay/2 + 1)))
		timer := time.NewTimer(delay + jitter)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		if delay < hubReconnectMaxDelay {
			delay *= 2
			if delay > hubReconnectMaxDelay {
				delay = hubReconnectMaxDelay
			}
		}
	}
}

func (c *hubConnection) dialAndResume(ctx context.Context, token string) (*websocket.Conn, *protocol.ConnectionAuthorityReceipt, bool, error) {
	conn, _, err := websocket.Dial(ctx, c.cfg.HubURL, nil)
	if err != nil {
		return nil, nil, false, err
	}
	fail := func(err error, auth bool) (*websocket.Conn, *protocol.ConnectionAuthorityReceipt, bool, error) {
		_ = conn.Close(websocket.StatusPolicyViolation, "resume rejected")
		return nil, nil, auth, err
	}
	hello := protocol.Hello{ProtocolVersion: c.cfg.ProtocolVersion, Role: protocol.RoleAdapter, Token: token, SessionID: c.cfg.SessionID, Provider: c.cfg.Provider, Resume: true}
	if err := writeCLIProtocolFrame(ctx, conn, &hello); err != nil {
		return fail(err, false)
	}
	frame, err := readCLIProtocolFrame(ctx, conn)
	if err != nil {
		return fail(err, false)
	}
	if protocolErr, ok := frame.(*protocol.Error); ok {
		return fail(fmt.Errorf("hub error %s: %s", protocolErr.Code, protocolErr.Message), claimProtocolErrorRequiresReclaim(protocolErr))
	}
	ack, ok := frame.(*protocol.HelloAck)
	if !ok {
		return fail(fmt.Errorf("read resume hello ack: got %T", frame), false)
	}
	state, err := core.NewAdapterConnectionState(core.AdapterConnectionConfig{SessionID: c.cfg.SessionID, Provider: c.cfg.Provider, Token: token, ProtocolVersion: c.cfg.ProtocolVersion})
	if err != nil {
		return fail(err, false)
	}
	if _, err := state.MarkAccepted(*ack); err != nil {
		return fail(err, errors.Is(err, core.ErrInvalidHelloAck))
	}
	return conn, cloneConnectionAuthority(ack.ConnectionAuthority), false, nil
}

func isCLIEventEphemeral(eventType string) bool {
	switch eventType {
	case "presence", "agent.activity", "log.tail", "resource.sample":
		return true
	default:
		return false
	}
}
