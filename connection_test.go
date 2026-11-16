package mapepire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// mockServer is a minimal Mapepire server used to test the connection layer
// without a live IBM i. It answers each request in its own goroutine, so
// responses can arrive out of order like they can from the real server.
//
// SQL statements control its behavior:
//   - "SLOW <ms>"  responds after the given delay
//   - "NEVER"      never responds
//   - "DROP"       closes the websocket without responding
//   - anything else responds immediately, echoing the SQL back in the data
type mockServer struct {
	*httptest.Server
	mu         sync.Mutex
	conns      []*websocket.Conn
	connectErr string // if set, connect requests fail with this error
}

func newMockServer(t *testing.T) *mockServer {
	t.Helper()
	m := &mockServer{}
	upgrader := websocket.Upgrader{}

	m.Server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		m.mu.Lock()
		m.conns = append(m.conns, ws)
		m.mu.Unlock()

		var writeMu sync.Mutex
		reply := func(v any) {
			b, _ := json.Marshal(v)
			writeMu.Lock()
			defer writeMu.Unlock()
			ws.WriteMessage(websocket.TextMessage, b)
		}

		for {
			_, msg, err := ws.ReadMessage()
			if err != nil {
				return
			}
			var req struct {
				ID    string `json:"id"`
				Type  string `json:"type"`
				SQL   string `json:"sql"`
				Terse bool   `json:"terse"`
			}
			if err := json.Unmarshal(msg, &req); err != nil {
				continue
			}

			switch req.Type {
			case "connect":
				m.mu.Lock()
				connectErr := m.connectErr
				m.mu.Unlock()
				if connectErr != "" {
					reply(map[string]any{"id": req.ID, "success": false, "error": connectErr})
				} else {
					reply(map[string]any{"id": req.ID, "success": true})
				}
			case "getdbjob":
				reply(map[string]any{"id": req.ID, "success": true, "job": "123456/QUSER/QZDASOINIT"})
			case "getversion":
				reply(map[string]any{"id": req.ID, "success": true, "version": "2.1.0"})
			case "sqlclose":
				reply(map[string]any{"id": req.ID, "success": true})
			case "exit":
				ws.Close()
				return
			case "sql":
				go func() {
					switch {
					case req.SQL == "NEVER":
						return
					case req.SQL == "DROP":
						ws.Close()
						return
					case strings.HasPrefix(req.SQL, "SLOW "):
						var ms int
						fmt.Sscanf(req.SQL, "SLOW %d", &ms)
						time.Sleep(time.Duration(ms) * time.Millisecond)
					}
					var data any = []map[string]any{{"SQL": req.SQL}}
					if req.Terse {
						data = [][]any{{req.SQL}}
					}
					reply(map[string]any{
						"id": req.ID, "success": true, "is_done": true,
						"has_results": true, "data": data,
					})
				}()
			}
		}
	}))
	t.Cleanup(m.Close)
	return m
}

func (m *mockServer) daemon(t *testing.T) DaemonServer {
	t.Helper()
	u, err := url.Parse(m.URL)
	if err != nil {
		t.Fatal(err)
	}
	return DaemonServer{Host: u.Hostname(), Port: u.Port(), User: "u", Password: "p", IgnoreUnauthorized: true}
}

func connectMock(t *testing.T, m *mockServer, timeout time.Duration) *SQLJob {
	t.Helper()
	daemon := m.daemon(t)
	daemon.Timeout = timeout
	job := NewSQLJob("mock")
	if err := job.Connect(daemon); err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { job.Close() })
	return job
}

func execute(t *testing.T, job *SQLJob, sql string) (*ServerResponse, error) {
	t.Helper()
	query, err := job.Query(sql)
	if err != nil {
		t.Fatal(err)
	}
	return query.Execute()
}

func sqlOf(resp *ServerResponse) string {
	if resp == nil || len(resp.Data) == 0 {
		return ""
	}
	s, _ := resp.Data[0]["SQL"].(string)
	return s
}

func TestMockConnect(t *testing.T) {
	m := newMockServer(t)
	job := connectMock(t, m, 0)

	if job.GetStatus() != JOBSTATUS_READY {
		t.Errorf("status = %v, want READY", job.GetStatus())
	}
	if job.Jobname != "123456/QUSER/QZDASOINIT" {
		t.Errorf("jobname = %q", job.Jobname)
	}
}

func TestMockConnectFailureCleansUp(t *testing.T) {
	m := newMockServer(t)
	m.mu.Lock()
	m.connectErr = "bad credentials"
	m.mu.Unlock()

	job := NewSQLJob("mock")
	if err := job.Connect(m.daemon(t)); err == nil {
		t.Fatal("expected error")
	}
	if job.GetStatus() != JOBSTATUS_ERROR {
		t.Errorf("status = %v, want ERROR", job.GetStatus())
	}
	if job.isConnected() {
		t.Error("job should not keep a connection after a failed connect")
	}
}

// A slow response must not be delivered to a later request.
func TestMockOutOfOrderResponses(t *testing.T) {
	m := newMockServer(t)
	job := connectMock(t, m, 0)

	slowDone := make(chan *ServerResponse)
	go func() {
		resp, err := execute(t, job, "SLOW 300")
		if err != nil {
			t.Error(err)
		}
		slowDone <- resp
	}()

	time.Sleep(50 * time.Millisecond) // make sure the slow request is sent first
	start := time.Now()
	fast, err := execute(t, job, "FAST")
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Errorf("fast query waited %v behind the slow one", elapsed)
	}
	if got := sqlOf(fast); got != "FAST" {
		t.Errorf("fast query got response for %q", got)
	}

	slow := <-slowDone
	if got := sqlOf(slow); got != "SLOW 300" {
		t.Errorf("slow query got response for %q", got)
	}
}

// After a timeout, the late response must be dropped, not handed to the next request.
func TestMockTimeoutDoesNotDesync(t *testing.T) {
	m := newMockServer(t)
	job := connectMock(t, m, 100*time.Millisecond)

	_, err := execute(t, job, "SLOW 300")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}

	time.Sleep(300 * time.Millisecond) // let the late response arrive

	resp, err := execute(t, job, "NEXT")
	if err != nil {
		t.Fatal(err)
	}
	if got := sqlOf(resp); got != "NEXT" {
		t.Errorf("got response for %q, want NEXT", got)
	}
	if !job.isConnected() {
		t.Error("a timeout should not drop the connection")
	}
}

func TestMockContextCancel(t *testing.T) {
	m := newMockServer(t)
	job := connectMock(t, m, 0)

	query, _ := job.Query("NEVER")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := query.ExecuteContext(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}

	// The statement can be retried after a failed attempt
	if query.state.Load() != STATE_NOT_YET_RUN {
		t.Errorf("state = %v, want STATE_NOT_YET_RUN", query.state.Load())
	}
}

func TestMockConcurrentRequests(t *testing.T) {
	m := newMockServer(t)
	job := connectMock(t, m, 5*time.Second)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			sql := fmt.Sprintf("SLOW %d", i%7*10)
			resp, err := execute(t, job, sql)
			if err != nil {
				t.Error(err)
				return
			}
			if got := sqlOf(resp); got != sql {
				t.Errorf("query %q got response for %q", sql, got)
			}
			job.GetStatus()
		}(i)
		go func() {
			defer wg.Done()
			version, err := job.GetVersion()
			if err != nil || version != "2.1.0" {
				t.Errorf("GetVersion() = %q, %v", version, err)
			}
		}()
	}
	wg.Wait()
}

func TestMockConcurrentExecuteSameQuery(t *testing.T) {
	m := newMockServer(t)
	job := connectMock(t, m, 5*time.Second)
	query, _ := job.Query("SLOW 100")

	var wg sync.WaitGroup
	var mu sync.Mutex
	succeeded := 0
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := query.Execute(); err == nil {
				mu.Lock()
				succeeded++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if succeeded != 1 {
		t.Errorf("query executed %d times, want 1", succeeded)
	}
}

// Terse handling must follow the query being sent, not the last query created.
func TestMockTerseFollowsQuery(t *testing.T) {
	m := newMockServer(t)
	job := connectMock(t, m, 0)

	terse, _ := job.QueryWithOptions("TERSE", QueryOptions{TerseResult: true})
	normal, _ := job.Query("NORMAL")

	resp, err := terse.Execute()
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.TerseData) != 1 || resp.TerseData[0][0] != "TERSE" {
		t.Errorf("terse data = %v", resp.TerseData)
	}

	resp, err = normal.Execute()
	if err != nil {
		t.Fatal(err)
	}
	if got := sqlOf(resp); got != "NORMAL" {
		t.Errorf("normal data = %v", resp.Data)
	}
}

// A dropped connection fails pending and future requests instead of hanging.
func TestMockConnectionDrop(t *testing.T) {
	m := newMockServer(t)
	job := connectMock(t, m, 0)

	pending := make(chan error)
	go func() {
		_, err := execute(t, job, "NEVER")
		pending <- err
	}()
	time.Sleep(50 * time.Millisecond)

	_, err := execute(t, job, "DROP")
	var wsErr *WebsocketError
	if !errors.As(err, &wsErr) {
		t.Fatalf("err = %v, want WebsocketError", err)
	}

	select {
	case err := <-pending:
		if !errors.As(err, &wsErr) {
			t.Errorf("pending err = %v, want WebsocketError", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pending request hung after the connection dropped")
	}

	if job.isConnected() {
		t.Error("job should report as disconnected")
	}
	if _, err := job.GetVersion(); !errors.As(err, &wsErr) {
		t.Errorf("GetVersion() err = %v, want WebsocketError", err)
	}
}

func TestMockCloseFailsPendingRequests(t *testing.T) {
	m := newMockServer(t)
	daemon := m.daemon(t)
	job := NewSQLJob("mock")
	if err := job.Connect(daemon); err != nil {
		t.Fatal(err)
	}

	pending := make(chan error)
	go func() {
		_, err := execute(t, job, "NEVER")
		pending <- err
	}()
	time.Sleep(50 * time.Millisecond)

	if err := job.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}
	if job.GetStatus() != JOBSTATUS_ENDED {
		t.Errorf("status = %v, want ENDED", job.GetStatus())
	}

	select {
	case err := <-pending:
		var wsErr *WebsocketError
		if !errors.As(err, &wsErr) {
			t.Errorf("pending err = %v, want WebsocketError", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pending request hung after Close()")
	}

	if err := job.Close(); err == nil {
		t.Error("second Close() should fail")
	}
}

func TestMockPoolReconnectsDroppedJob(t *testing.T) {
	m := newMockServer(t)
	pool, err := NewPool(PoolOptions{Creds: m.daemon(t), MaxSize: 1, StartingSize: 1, MaxWaitTime: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	if _, err := pool.ExecuteSQL("DROP"); err == nil {
		t.Fatal("expected error")
	}
	resp, err := pool.ExecuteSQL("AFTER")
	if err != nil {
		t.Fatal(err)
	}
	if got := sqlOf(resp); got != "AFTER" {
		t.Errorf("got response for %q", got)
	}
}
