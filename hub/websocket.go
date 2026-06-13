package hub

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/winghv/agentwharf/auth"
	"github.com/winghv/agentwharf/protocol"
	"nhooyr.io/websocket"
)

const defaultHandshakeTimeout = 10 * time.Second

type WebSocketConfig struct {
	Handshake        *Handshake
	HandshakeTimeout time.Duration
}

func NewWebSocketHandler(cfg WebSocketConfig) http.Handler {
	timeout := cfg.HandshakeTimeout
	if timeout <= 0 {
		timeout = defaultHandshakeTimeout
	}
	return &webSocketHandler{
		handshake:        cfg.Handshake,
		handshakeTimeout: timeout,
	}
}

type webSocketHandler struct {
	handshake        *Handshake
	handshakeTimeout time.Duration
}

func (h *webSocketHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		return
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	ctx := r.Context()
	if err := h.acceptPeer(ctx, conn); err != nil {
		return
	}
	h.readLoop(ctx, conn)
}

func (h *webSocketHandler) acceptPeer(ctx context.Context, conn *websocket.Conn) error {
	frame, err := h.readHelloFrame(ctx, conn)
	if err != nil {
		_ = writeProtocolError(context.Background(), conn, "timeout", "waiting for hello", true)
		_ = conn.Close(websocket.StatusPolicyViolation, "hello timeout")
		return err
	}
	hello, ok := frame.(*protocol.Hello)
	if !ok {
		_ = writeProtocolError(ctx, conn, "invalid_hello", "first frame must be hello", true)
		_ = conn.Close(websocket.StatusPolicyViolation, "invalid hello")
		return ErrInvalidHello
	}
	if h.handshake == nil {
		err := errors.New("websocket handshake is not configured")
		_ = writeProtocolError(ctx, conn, "internal_error", err.Error(), true)
		_ = conn.Close(websocket.StatusInternalError, "handshake not configured")
		return err
	}
	ack, _, err := h.handshake.HandleHello(ctx, hello)
	if err != nil {
		code := protocolErrorCode(err)
		_ = writeProtocolError(ctx, conn, code, err.Error(), true)
		_ = conn.Close(websocket.StatusPolicyViolation, code)
		return err
	}
	return writeProtocolFrame(ctx, conn, &ack)
}

func (h *webSocketHandler) readHelloFrame(ctx context.Context, conn *websocket.Conn) (protocol.Frame, error) {
	type result struct {
		frame protocol.Frame
		err   error
	}
	resultCh := make(chan result, 1)
	go func() {
		frame, err := readProtocolFrame(ctx, conn)
		resultCh <- result{frame: frame, err: err}
	}()

	timer := time.NewTimer(h.handshakeTimeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-resultCh:
		return result.frame, result.err
	case <-timer.C:
		return nil, context.DeadlineExceeded
	}
}

func (h *webSocketHandler) readLoop(ctx context.Context, conn *websocket.Conn) {
	for {
		frame, err := readProtocolFrame(ctx, conn)
		if err != nil {
			return
		}
		switch typed := frame.(type) {
		case *protocol.Ping:
			if err := writeProtocolFrame(ctx, conn, &protocol.Pong{Nonce: typed.Nonce}); err != nil {
				return
			}
		case *protocol.Pong:
			continue
		default:
			_ = writeProtocolError(ctx, conn, "unsupported_frame", fmt.Sprintf("unsupported frame %s", typed.FrameName()), false)
		}
	}
}

func readProtocolFrame(ctx context.Context, conn *websocket.Conn) (protocol.Frame, error) {
	messageType, data, err := conn.Read(ctx)
	if err != nil {
		return nil, err
	}
	if messageType != websocket.MessageText {
		return nil, fmt.Errorf("expected text websocket message, got %v", messageType)
	}
	frame, err := protocol.Decode(data)
	if err != nil {
		return nil, err
	}
	return frame, nil
}

func writeProtocolFrame(ctx context.Context, conn *websocket.Conn, frame protocol.Frame) error {
	data, err := protocol.Encode(frame)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, data)
}

func writeProtocolError(ctx context.Context, conn *websocket.Conn, code string, message string, fatal bool) error {
	return writeProtocolFrame(ctx, conn, &protocol.Error{
		Code:    code,
		Message: message,
		Fatal:   fatal,
	})
}

func protocolErrorCode(err error) string {
	switch {
	case errors.Is(err, ErrInvalidHello), errors.Is(err, ErrVersionUnsupported):
		return "invalid_hello"
	case errors.Is(err, auth.ErrInvalidToken), errors.Is(err, auth.ErrUnauthorized):
		return "unauthorized"
	case errors.Is(err, ErrSessionNotFound):
		return "session_not_found"
	default:
		return "internal_error"
	}
}
