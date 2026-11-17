package observatory

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/transport/internet/tagged"
)

// Replace only the outbound transport boundary: exercise the real probe HTTP
// client/transport against a real local TCP server without credentials or TUN.
func localProbeObserver(t *testing.T, url string) *Observer {
	t.Helper()
	original := tagged.Dialer
	tagged.Dialer = func(ctx context.Context, _ routing.Dispatcher, dest xnet.Destination, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", dest.NetAddr())
	}
	t.Cleanup(func() { tagged.Dialer = original })
	observer := &Observer{ctx: context.Background(), config: &Config{ProbeUrl: url}}
	if err := observer.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = observer.Close() })
	return observer
}

func TestCompletedProbesReleaseHTTPConnections(t *testing.T) {
	var mutex sync.Mutex
	connections := map[net.Conn]bool{}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	server.Config.ConnState = func(conn net.Conn, state http.ConnState) {
		mutex.Lock()
		defer mutex.Unlock()
		if state == http.StateNew {
			connections[conn] = true
		}
		if state == http.StateClosed {
			delete(connections, conn)
		}
	}
	server.Start()
	t.Cleanup(server.Close)
	observer := localProbeObserver(t, server.URL)
	for i := 0; i < 20; i++ {
		if result := observer.probe("local"); !result.Alive {
			t.Fatalf("probe %d failed", i)
		}
	}
	deadline := time.Now().Add(time.Second)
	for {
		mutex.Lock()
		remaining := len(connections)
		mutex.Unlock()
		if remaining == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("completed probes retained %d HTTP connections; want zero", remaining)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestObserverCloseCancelsPendingHTTPRequest(t *testing.T) {
	requested := make(chan struct{})
	disconnected := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		close(requested)
		<-request.Context().Done()
		close(disconnected)
	}))
	t.Cleanup(server.Close)
	observer := localProbeObserver(t, server.URL)
	completed := make(chan struct{})
	go func() { observer.probe("local"); close(completed) }()
	select {
	case <-requested:
	case <-time.After(time.Second):
		t.Fatal("probe never reached local server")
	}
	if err := observer.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-completed:
	case <-time.After(time.Second):
		t.Error("Close did not cancel pending probe")
	}
	select {
	case <-disconnected:
	case <-time.After(time.Second):
		t.Error("Close left HTTP connection open")
	}
	// Drain before restoring the global dial seam, including on RED.
	select {
	case <-completed:
	case <-time.After(6 * time.Second):
		t.Fatal("probe exceeded client timeout")
	}
}

func TestObserverCloseCancelsPendingOutboundDial(t *testing.T) {
	original := tagged.Dialer
	entered := make(chan struct{})
	cancelled := make(chan struct{})
	release := make(chan struct{})
	dialDone := make(chan struct{})
	tagged.Dialer = func(ctx context.Context, _ routing.Dispatcher, _ xnet.Destination, _ string) (net.Conn, error) {
		defer close(dialDone)
		close(entered)
		select {
		case <-ctx.Done():
			close(cancelled)
			return nil, ctx.Err()
		case <-release:
			return nil, context.Canceled
		}
	}
	defer func() {
		close(release)
		select {
		case <-dialDone:
		case <-time.After(time.Second):
			t.Error("outbound dial did not drain")
		}
		tagged.Dialer = original
	}()
	observer := &Observer{ctx: context.Background(), config: &Config{ProbeUrl: "http://127.0.0.1/"}}
	if err := observer.Start(); err != nil {
		t.Fatal(err)
	}
	defer observer.Close()
	completed := make(chan struct{})
	go func() { observer.probe("local"); close(completed) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("probe did not start outbound dial")
	}
	if err := observer.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Error("Close did not cancel tagged outbound dial context")
	}
	select {
	case <-completed:
	case <-time.After(time.Second):
		t.Error("pending probe did not return after Close")
	}
}

type probeTrackedConnection struct {
	net.Conn
	closed chan struct{}
	once   sync.Once
}

func (c *probeTrackedConnection) Close() error {
	c.once.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

// A dial may successfully return just after cancellation. HTTP Transport must
// receive that connection so it can close it when no request needs it anymore.
func TestObserverClosesConnectionReturnedAfterCancellation(t *testing.T) {
	original := tagged.Dialer
	client, server := net.Pipe()
	tracked := &probeTrackedConnection{Conn: client, closed: make(chan struct{})}
	defer server.Close()
	defer client.Close()
	entered := make(chan struct{})
	returned := make(chan struct{})
	tagged.Dialer = func(ctx context.Context, _ routing.Dispatcher, _ xnet.Destination, _ string) (net.Conn, error) {
		close(entered)
		<-ctx.Done()
		// Model a lower-level dial completing after cancellation propagation,
		// not a cooperative dial that immediately returns ctx.Err().
		time.Sleep(50 * time.Millisecond)
		close(returned)
		return tracked, nil
	}
	defer func() {
		select {
		case <-returned:
		case <-time.After(time.Second):
			t.Error("late dial did not drain")
		}
		tagged.Dialer = original
	}()
	observer := &Observer{ctx: context.Background(), config: &Config{ProbeUrl: "http://127.0.0.1/"}}
	if err := observer.Start(); err != nil {
		t.Fatal(err)
	}
	defer observer.Close()
	completed := make(chan struct{})
	go func() { observer.probe("local"); close(completed) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("probe did not enter tagged dial")
	}
	if err := observer.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-completed:
	case <-time.After(time.Second):
		t.Fatal("cancelled probe did not return")
	}
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("late dial did not return")
	}
	select {
	case <-tracked.closed:
	case <-time.After(time.Second):
		t.Error("connection returned after cancellation was orphaned")
	}
}

func TestProbeTimeoutCancelsPendingOutboundDialWithoutClosingObserver(t *testing.T) {
	original := tagged.Dialer
	entered := make(chan struct{})
	cancelled := make(chan struct{})
	release := make(chan struct{})
	drained := make(chan struct{})
	tagged.Dialer = func(ctx context.Context, _ routing.Dispatcher, _ xnet.Destination, _ string) (net.Conn, error) {
		defer close(drained)
		close(entered)
		select {
		case <-ctx.Done():
			close(cancelled)
			return nil, ctx.Err()
		case <-release:
			return nil, context.Canceled
		}
	}
	defer func() {
		close(release)
		select {
		case <-drained:
		case <-time.After(time.Second):
			t.Error("test dial did not drain")
		}
		tagged.Dialer = original
	}()
	observer := &Observer{ctx: context.Background(), config: &Config{ProbeUrl: "http://127.0.0.1/"}}
	if err := observer.Start(); err != nil {
		t.Fatal(err)
	}
	// Deliberately do not Close until AFTER observing timeout cancellation.
	defer observer.Close()
	completed := make(chan ProbeResult, 1)
	go func() { completed <- observer.probe("local") }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("probe did not enter dial")
	}
	select {
	case result := <-completed:
		if result.Alive {
			t.Error("timed-out dial reported alive")
		}
	case <-time.After(6 * time.Second):
		t.Fatal("probe exceeded existing five-second timeout")
	}
	select {
	case <-cancelled:
	case <-time.After(200 * time.Millisecond):
		t.Error("probe timeout left tagged outbound dial context alive")
	}
}
