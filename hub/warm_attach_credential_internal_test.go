package hub

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	gws "github.com/gorilla/websocket"
	"github.com/winghv/agentwharf/auth"
	"github.com/winghv/agentwharf/protocol"
	"github.com/winghv/agentwharf/store"
	"nhooyr.io/websocket"
)

func TestBeginPendingTargetJoinDoesNotScheduleRejectedAdmission(t *testing.T) {
	expiresAt := time.Now().Add(time.Minute)
	entry := func(attachID string) *pendingTargetJoin {
		return &pendingTargetJoin{attachID: attachID, expiresAt: expiresAt}
	}
	full := make(map[string]*pendingTargetJoin, maxPendingTargetJoins)
	for index := 0; index < maxPendingTargetJoins; index++ {
		id := strconv.Itoa(index)
		full["nonce"+id] = entry("other" + id)
	}
	for _, test := range []struct {
		name     string
		joins    map[string]*pendingTargetJoin
		byAttach map[string]*pendingTargetJoin
		attachID string
	}{
		{name: "duplicate", joins: map[string]*pendingTargetJoin{"nonce": entry("duplicate")}, byAttach: map[string]*pendingTargetJoin{"duplicate": entry("duplicate")}, attachID: "duplicate"},
		{name: "capacity", joins: full, byAttach: map[string]*pendingTargetJoin{}, attachID: "new"},
	} {
		t.Run(test.name, func(t *testing.T) {
			scheduled := 0
			handler := &webSocketHandler{
				pendingTargetJoins: test.joins, pendingTargetJoinByAttach: test.byAttach,
				pendingTargetJoinTimer: func(time.Duration, func()) *time.Timer {
					scheduled++
					return nil
				},
			}
			authorization := auth.AttachAuthorization{Grant: auth.AttachGrant{AttachID: test.attachID}}
			if err := handler.beginPendingTargetJoin(context.Background(), authorization, store.WarmAttachTargetActivation{ExpiresAt: expiresAt}); err == nil {
				t.Fatal("beginPendingTargetJoin() unexpectedly accepted unavailable admission")
			}
			if scheduled != 0 {
				t.Fatalf("rejected admission scheduled %d expiry timer(s), want 0", scheduled)
			}
		})
	}
}

func TestPendingTargetJoinPingFenceRejectsDecodedInputBeforeObserverLock(t *testing.T) {
	observed := make(chan struct{})
	releaseObserver := make(chan struct{})
	entry := &pendingTargetJoin{
		attachID: "attach-ping-fence", expiresAt: time.Now().Add(time.Minute),
		readerReady: make(chan struct{}), finished: make(chan struct{}),
		observeHook: func() {
			close(observed)
			<-releaseObserver
		},
	}
	handler := &webSocketHandler{
		pendingTargetJoins:        map[string]*pendingTargetJoin{"nonce": entry},
		pendingTargetJoinByAttach: map[string]*pendingTargetJoin{entry.attachID: entry},
	}
	ready := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := acceptManagedConn(w, r)
		if err != nil {
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "")
		entry.conn = conn
		handler.startPendingTargetJoinReader(r.Context(), entry)
		close(ready)
		<-entry.finished
	}))
	defer server.Close()

	client, _, err := websocket.Dial(context.Background(), "ws"+server.URL[len("http"):], nil)
	if err != nil {
		t.Fatalf("dial pending target: %v", err)
	}
	defer client.Close(websocket.StatusNormalClosure, "")
	<-ready
	clientReadCtx, cancelClientRead := context.WithCancel(context.Background())
	defer cancelClientRead()
	go func() { _, _, _ = client.Read(clientReadCtx) }()
	payload, err := protocol.Encode(&protocol.Ping{Nonce: "forbidden"})
	if err != nil {
		t.Fatalf("encode forbidden pending input: %v", err)
	}
	if err := client.Write(context.Background(), websocket.MessageText, payload); err != nil {
		t.Fatalf("write forbidden pending input: %v", err)
	}
	select {
	case <-observed:
	case <-time.After(time.Second):
		t.Fatal("pending reader did not decode forbidden input")
	}

	claimCtx, cancelClaim := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancelClaim()
	if err := handler.claimPendingTargetJoinDelivery(claimCtx, entry, &protocol.TargetJoinCredential{}); !errors.Is(err, ErrWarmAttachCredentialNotAccepted) {
		t.Fatalf("claimPendingTargetJoinDelivery() error = %v, want %v", err, ErrWarmAttachCredentialNotAccepted)
	}
	if entry.deliveryClaimed {
		t.Fatal("decoded forbidden input reached delivery claim")
	}
	close(releaseObserver)
}

func TestPendingTargetJoinMatchingPongRejectsObservedInputBeforeCredentialWrite(t *testing.T) {
	entry := &pendingTargetJoin{
		attachID: "attach-matching-pong", expiresAt: time.Now().Add(time.Minute),
		finished: make(chan struct{}), inputObserved: true,
		deliveryNonce: "matching-nonce", deliveryFrame: &protocol.TargetJoinCredential{Credential: "must-not-write"},
		deliveryResult: make(chan error, 1),
	}
	handler := &webSocketHandler{pendingTargetJoinByAttach: map[string]*pendingTargetJoin{entry.attachID: entry}}
	if err := handler.completePendingTargetJoinDelivery(entry, entry.deliveryNonce); err != nil {
		t.Fatalf("completePendingTargetJoinDelivery() error = %v", err)
	}
	select {
	case err := <-entry.deliveryResult:
		t.Fatalf("matching Pong published result %v after observed input", err)
	default:
	}
	if entry.deliveryClaimed {
		t.Fatal("matching Pong claimed credential delivery after observed input")
	}
}

func TestManagedConnRejectsOversizedIngressBeforeDecode(t *testing.T) {
	readResult := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := acceptManagedConn(w, r)
		if err != nil {
			readResult <- err
			return
		}
		_, _, err = conn.Read(r.Context())
		readResult <- err
		_ = conn.CloseNow()
	}))
	defer server.Close()
	client, _, err := websocket.Dial(context.Background(), "ws"+server.URL[len("http"):], nil)
	if err != nil {
		t.Fatalf("dial managed connection: %v", err)
	}
	defer client.Close(websocket.StatusNormalClosure, "")
	_ = client.Write(context.Background(), websocket.MessageText, make([]byte, hubWebSocketReadLimit+1))
	select {
	case err := <-readResult:
		if err == nil {
			t.Fatal("oversized ingress reached protocol decode")
		}
	case <-time.After(time.Second):
		t.Fatal("managed connection did not reject oversized ingress")
	}
}

func TestManagedConnSerializesConcurrentWrites(t *testing.T) {
	const writes = 8
	ready := make(chan struct{})
	start := make(chan struct{})
	writeResult := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := acceptManagedConn(w, r)
		if err != nil {
			writeResult <- err
			return
		}
		defer conn.CloseNow()
		close(ready)
		<-start

		results := make(chan error, writes)
		var group sync.WaitGroup
		for range writes {
			group.Add(1)
			go func() {
				defer group.Done()
				results <- conn.Write(context.Background(), websocket.MessageText, []byte(`{"frame":"ping","nonce":"serialized"}`))
			}()
		}
		group.Wait()
		close(results)
		for err := range results {
			if err != nil {
				writeResult <- err
				return
			}
		}
		writeResult <- nil
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client, _, err := websocket.Dial(ctx, "ws"+server.URL[len("http"):], nil)
	if err != nil {
		t.Fatalf("dial managed connection: %v", err)
	}
	defer client.Close(websocket.StatusNormalClosure, "")
	<-ready
	close(start)
	for range writes {
		if _, _, err := client.Read(ctx); err != nil {
			t.Fatalf("read serialized write: %v", err)
		}
	}
	if err := <-writeResult; err != nil {
		t.Fatalf("concurrent write: %v", err)
	}
}

func TestPendingTargetJoinMatchingPongLinearizesCredentialBeforeNextInput(t *testing.T) {
	entry := &pendingTargetJoin{attachID: "attach-pong-order", expiresAt: time.Now().Add(time.Minute), readerReady: make(chan struct{}), finished: make(chan struct{})}
	handler := &webSocketHandler{pendingTargetJoins: map[string]*pendingTargetJoin{"nonce": entry}, pendingTargetJoinByAttach: map[string]*pendingTargetJoin{entry.attachID: entry}}
	ready := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := acceptManagedConn(w, r)
		if err != nil {
			return
		}
		entry.conn = conn
		handler.startPendingTargetJoinReader(r.Context(), entry)
		close(ready)
		<-entry.finished
	}))
	defer server.Close()
	client, _, err := gws.DefaultDialer.Dial("ws"+server.URL[len("http"):], nil)
	if err != nil {
		t.Fatalf("dial target: %v", err)
	}
	defer client.Close()
	client.SetPingHandler(func(payload string) error {
		if err := client.WriteControl(gws.PongMessage, []byte(payload), time.Now().Add(time.Second)); err != nil {
			return err
		}
		return client.WriteMessage(gws.TextMessage, []byte(`{"frame":"ping","nonce":"forbidden-after-pong"}`))
	})
	<-ready
	result := make(chan error, 1)
	go func() {
		result <- handler.claimPendingTargetJoinDelivery(context.Background(), entry, &protocol.TargetJoinCredential{Credential: "credential", TargetSessionID: "target", TargetCredentialLineageRef: "lineage", Generation: 1, ExpiresAt: time.Now().Add(time.Minute).UnixMilli()})
	}()
	_, payload, err := client.ReadMessage()
	if err != nil {
		t.Fatalf("read credential: %v", err)
	}
	frame, err := protocol.Decode(payload)
	if err != nil {
		t.Fatalf("decode credential: %v", err)
	}
	if _, ok := frame.(*protocol.TargetJoinCredential); !ok {
		t.Fatalf("frame = %T, want credential", frame)
	}
	if err := <-result; err != nil {
		t.Fatalf("delivery error = %v", err)
	}
}
