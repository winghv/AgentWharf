package hub

import (
	"context"
	"net/http"
	"time"

	gws "github.com/gorilla/websocket"
	"nhooyr.io/websocket"
)

// managedConn keeps Hub's context-aware surface while exposing a synchronous
// Pong callback to its only reader.
type managedConn struct{ raw *gws.Conn }

const hubWebSocketReadLimit = 64 << 10

func acceptManagedConn(w http.ResponseWriter, r *http.Request) (*managedConn, error) {
	raw, err := (&gws.Upgrader{EnableCompression: false}).Upgrade(w, r, nil)
	if err != nil {
		return nil, err
	}
	raw.SetReadLimit(hubWebSocketReadLimit)
	return &managedConn{raw: raw}, nil
}
func (c *managedConn) Read(ctx context.Context) (int, []byte, error) {
	if deadline, ok := ctx.Deadline(); ok {
		if err := c.raw.SetReadDeadline(deadline); err != nil {
			return 0, nil, err
		}
	}
	return c.raw.ReadMessage()
}
func (c *managedConn) Write(ctx context.Context, kind websocket.MessageType, data []byte) error {
	if deadline, ok := ctx.Deadline(); ok {
		if err := c.raw.SetWriteDeadline(deadline); err != nil {
			return err
		}
	}
	return c.raw.WriteMessage(int(kind), data)
}
func (c *managedConn) Close(code websocket.StatusCode, reason string) error {
	_ = c.raw.WriteControl(gws.CloseMessage, gws.FormatCloseMessage(int(code), reason), time.Now().Add(time.Second))
	return c.raw.Close()
}
func (c *managedConn) CloseNow() error { return c.raw.Close() }
func (c *managedConn) writePing(ctx context.Context, nonce string) error {
	deadline := time.Now().Add(time.Second)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	return c.raw.WriteControl(gws.PingMessage, []byte(nonce), deadline)
}
func (c *managedConn) setPongHandler(handler func(string) error) { c.raw.SetPongHandler(handler) }
