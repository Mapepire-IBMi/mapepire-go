package mapepire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// errConnClosed is the reason recorded when the connection was closed locally.
var errConnClosed = errors.New("connection closed")

// closeWriteTimeout bounds how long Close() waits to deliver the exit/close frames.
const closeWriteTimeout = 5 * time.Second

// wsConn multiplexes request/response pairs over a single websocket.
//
// Mapepire tags every response with the id of the request it answers, so a
// single reader goroutine reads every incoming message and hands it to the
// caller waiting on that id. This means:
//   - several requests can be in flight on one connection at once,
//   - a response that arrives after its caller gave up (timeout/cancel) is
//     dropped instead of being handed to the next request,
//   - a broken connection fails every pending request immediately.
type wsConn struct {
	ws      *websocket.Conn
	writeMu sync.Mutex // serializes writes (gorilla allows one concurrent writer)

	mu      sync.Mutex             // guards pending, closed, err
	pending map[string]chan []byte // in-flight requests keyed by request id
	closed  bool
	err     error // why the connection stopped; valid once closed is true

	done      chan struct{} // closed when the connection is shut down
	closeOnce sync.Once
}

func newWSConn(ws *websocket.Conn) *wsConn {
	c := &wsConn{
		ws:      ws,
		pending: make(map[string]chan []byte),
		done:    make(chan struct{}),
	}
	go c.readLoop()
	return c
}

// readLoop is the only reader of the websocket. It routes each message to the
// request waiting on its id and drops messages nobody is waiting for.
func (c *wsConn) readLoop() {
	for {
		_, msg, err := c.ws.ReadMessage()
		if err != nil {
			c.shutdown(err)
			return
		}

		var envelope struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(msg, &envelope); err != nil {
			continue
		}

		c.mu.Lock()
		ch, ok := c.pending[envelope.ID]
		if ok {
			delete(c.pending, envelope.ID)
		}
		c.mu.Unlock()

		if ok {
			ch <- msg // buffered, never blocks
		}
	}
}

// shutdown marks the connection dead, fails all pending requests and closes the socket.
func (c *wsConn) shutdown(reason error) {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.err = reason
		c.pending = nil
		c.mu.Unlock()

		close(c.done)
		c.ws.Close()
	})
}

// isClosed reports whether the connection has been shut down.
func (c *wsConn) isClosed() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

// closedError returns the error reported to callers once the connection is dead.
func (c *wsConn) closedError(method string) error {
	c.mu.Lock()
	reason := c.err
	c.mu.Unlock()
	if reason == nil {
		reason = errConnClosed
	}
	return &WebsocketError{Method: method, Message: "connection is closed: " + reason.Error()}
}

func (c *wsConn) forget(id string) {
	c.mu.Lock()
	if c.pending != nil {
		delete(c.pending, id)
	}
	c.mu.Unlock()
}

// write sends one text message, honoring the context deadline. A failed write
// leaves a gorilla connection unusable, so it also shuts the connection down.
func (c *wsConn) write(ctx context.Context, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("request not sent: %w", err)
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if c.isClosed() {
		return c.closedError("write()")
	}

	deadline, _ := ctx.Deadline() // zero value means no deadline
	c.ws.SetWriteDeadline(deadline)

	if err := c.ws.WriteMessage(websocket.TextMessage, payload); err != nil {
		c.shutdown(err)
		return &WebsocketError{Method: "write()", Message: "WriteMessage(): " + err.Error()}
	}
	return nil
}

// roundTrip sends a request and waits for the response carrying the same id.
func (c *wsConn) roundTrip(ctx context.Context, id string, payload []byte) ([]byte, error) {
	ch := make(chan []byte, 1)

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, c.closedError("send()")
	}
	if _, inFlight := c.pending[id]; inFlight {
		c.mu.Unlock()
		return nil, fmt.Errorf("a request with ID %q is already in flight on this job", id)
	}
	c.pending[id] = ch
	c.mu.Unlock()

	if err := c.write(ctx, payload); err != nil {
		c.forget(id)
		return nil, err
	}

	select {
	case msg := <-ch:
		return msg, nil
	case <-ctx.Done():
		c.forget(id)
		return nil, fmt.Errorf("no response for request %q: %w", id, ctx.Err())
	case <-c.done:
		// The response may have been delivered just before the connection died.
		select {
		case msg := <-ch:
			return msg, nil
		default:
		}
		return nil, c.closedError("send()")
	}
}

// close sends the optional exit payload and a close frame, then shuts the connection down.
func (c *wsConn) close(exitPayload []byte) error {
	var writeErr error
	if exitPayload != nil {
		ctx, cancel := context.WithTimeout(context.Background(), closeWriteTimeout)
		writeErr = c.write(ctx, exitPayload)
		cancel()
	}

	if !c.isClosed() {
		msg := websocket.FormatCloseMessage(websocket.CloseNormalClosure, "")
		c.ws.WriteControl(websocket.CloseMessage, msg, time.Now().Add(closeWriteTimeout))
	}

	c.shutdown(errConnClosed)
	return writeErr
}
