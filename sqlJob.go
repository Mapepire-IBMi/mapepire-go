package mapepire

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// Represents a DB2 server daemon with connection details.
type DaemonServer struct {
	Host               string        // Hostname or IP
	User               string        // Username for authentication
	Password           string        // Password for authentication
	Port               string        // Default port is 8076
	IgnoreUnauthorized bool          // Ignore unauthorized certificate
	Technique          string        // CLI or TCP
	Properties         string        // A semicolon-delimited list of JDBC connection properties
	Timeout            time.Duration // Per-request timeout, also used for the websocket handshake (0 = no timeout)
}

// Represents a SQL job that manages connections and queries to a database.
//
// A SQLJob is safe for concurrent use: multiple goroutines may run queries on
// the same job at the same time. Read the job status with GetStatus() rather
// than the Status field when other goroutines may be using the job.
type SQLJob struct {
	ID         string        // Unique identifier
	Jobname    string        // Name of the Job
	Status     string        // Status of the Job
	Options    *TraceOptions // Trace configuration options
	daemon     DaemonServer  // Server daemon with connection details
	queryList  *queryList    // List of all open queries
	conn       *wsConn       // Websocket connection
	counter    atomic.Uint32 // Counter for query IDs
	reqCounter atomic.Uint64 // Counter for internal request IDs
	mu         sync.Mutex    // Guards Jobname, Status, Options, daemon and conn
}

const (
	JOBSTATUS_BUSY        = "BUSY"
	JOBSTATUS_ERROR       = "ERROR"
	JOBSTATUS_ENDED       = "ENDED"
	JOBSTATUS_READY       = "READY"
	JOBSTATUS_CONNECTING  = "CONNECTING"
	JOBSTATUS_NOT_STARTED = "NOT_STARTED"
)
const (
	DEFAULT_FETCH_SIZE = 100
	MAX_FETCH_SIZE     = 1000
)

// Receive a new SQL job with the given ID
func NewSQLJob(ID string) *SQLJob {
	list := newQueryList()
	return &SQLJob{ID: ID, Status: JOBSTATUS_NOT_STARTED, queryList: list}
}

// Creates a websocket connection and connects to the server.
func (s *SQLJob) Connect(server DaemonServer) error {
	return s.ConnectContext(context.Background(), server)
}

// Creates a websocket connection and connects to the server.
// The context bounds the handshake and the initial requests.
func (s *SQLJob) ConnectContext(ctx context.Context, server DaemonServer) error {
	s.setJobStatus(JOBSTATUS_CONNECTING)
	if server.Port == "" {
		server.Port = "8076"
	}

	url := fmt.Sprintf("wss://%s:%s/db/", server.Host, server.Port)
	dialer := *websocket.DefaultDialer

	if server.IgnoreUnauthorized {
		dialer.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	if server.Timeout > 0 {
		dialer.HandshakeTimeout = server.Timeout
	}

	header := http.Header{}
	header.Add("authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(server.User+":"+server.Password)))

	ws, _, err := dialer.DialContext(ctx, url, header)
	if err != nil {
		s.setJobStatus(JOBSTATUS_ERROR)
		return &WebsocketError{Method: "Connect()", Message: err.Error()}
	}
	conn := newWSConn(ws)

	s.mu.Lock()
	old := s.conn
	s.conn = conn
	s.daemon = server
	s.mu.Unlock()
	if old != nil {
		old.close(nil)
	}

	fail := func(err error) error {
		s.mu.Lock()
		if s.conn == conn {
			s.conn = nil
		}
		s.mu.Unlock()
		conn.close(nil)
		s.setJobStatus(JOBSTATUS_ERROR)
		return err
	}

	if _, err := s.request(ctx, connectRequest(s.newRequestID("connect"), server), false); err != nil {
		return fail(err)
	}
	if _, err := s.getDBJobContext(ctx); err != nil {
		return fail(err)
	}

	s.setJobStatus(JOBSTATUS_READY)
	return nil
}

func connectRequest(id string, server DaemonServer) serverRequest {
	var jsonreq string
	if server.Technique != "" {
		jsonreq =
			fmt.Sprintf(`{"id":"%v","type":"connect","techinque":"%v","props":"%v"}`, id, server.Technique, server.Properties)
	} else {
		jsonreq =
			fmt.Sprintf(`{"id":"%v","type":"connect","props":"%v"}`, id, server.Properties)
	}
	return serverRequest{id: id, jsonreq: jsonreq}
}

// sends requests to the server and decodes the response
func (s *SQLJob) send(ctx context.Context, req serverRequest) (*ServerResponse, error) {
	response := &ServerResponse{
		ID: req.id,
	}

	resp, err := s.request(ctx, req, true)
	if err != nil {
		var serverErr *ServerError
		if errors.As(err, &serverErr) {
			response.SqlRC, response.SqlState = serverErr.sqlRC, serverErr.sqlState
		}
		response.Error = err
		return response, err
	}

	// Only works if data is received in terse format
	if req.terse {
		resp = []byte(strings.Replace(string(resp), `"data":[[`, `"terse_data":[[`, 1))
	}

	if err := json.Unmarshal(resp, response); err != nil {
		unmarshalErr := fmt.Errorf("unmarshal error in send() method: ")
		err = errors.Join(unmarshalErr, err)
		return response, err
	}

	if jobname := s.getJobname(); jobname != "" {
		response.Job = jobname
	}

	return response, nil
}

// request sends a request, waits for the matching response and checks it for
// server errors. When allowReconnect is set and the server reports that the
// job is no longer connected, one reconnect attempt is made.
func (s *SQLJob) request(ctx context.Context, req serverRequest, allowReconnect bool) ([]byte, error) {
	conn := s.getConn()
	if conn == nil {
		return nil, &WebsocketError{Method: "send()", Message: "need a connection"}
	}

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	resp, err := conn.roundTrip(ctx, req.id, []byte(req.jsonreq))
	if err != nil {
		return nil, err
	}

	if err := checkJsonErr(resp); err != nil {
		if err.Message == "Not connected" && allowReconnect {
			err.Message += fmt.Sprintf(" -> reconnected? %v", s.reconnect(ctx))
		}
		err.Message = "json.Unmarshal(): " + err.Message
		return resp, err
	}
	return resp, nil
}

// checks JSON for errors
func checkJsonErr(jsonres []byte) *ServerError {
	var checkError struct {
		Error    string
		SqlRC    int    `json:"sql_rc"`
		SqlState string `json:"sql_state"`
	}

	json.Unmarshal(jsonres, &checkError)
	if checkError.Error != "" || checkError.SqlState != "" {
		return &ServerError{
			Method:   "checkJsonErr()",
			Message:  checkError.Error,
			sqlRC:    checkError.SqlRC,
			sqlState: checkError.SqlState,
		}
	}
	return nil
}

// re-sends the connect request on the existing websocket
func (s *SQLJob) reconnect(ctx context.Context) bool {
	_, err := s.request(ctx, connectRequest(s.newRequestID("connect"), s.getDaemon()), false)
	return err == nil
}

// applies the configured per-request timeout to the context
func (s *SQLJob) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if timeout := s.getDaemon().Timeout; timeout > 0 {
		return context.WithTimeout(ctx, timeout)
	}
	return ctx, func() {}
}

// Creates a query with the SQL
func (s *SQLJob) Query(sql string) (*Query, error) {
	return s.QueryWithOptions(sql, QueryOptions{Rows: DEFAULT_FETCH_SIZE})
}

// Creates a query with the CL command
func (s *SQLJob) ClCommand(command string) (*Query, error) {
	return s.QueryWithOptions(command, QueryOptions{IsCLcommand: true})
}

// Creates a query with the given options
func (s *SQLJob) QueryWithOptions(command string, options QueryOptions) (*Query, error) {

	if command == "" {
		return nil, fmt.Errorf("SQL or CL command required")
	}

	jsonParams, err := func() (string, error) {
		if len(options.Parameters) == 1 {
			param, err := json.Marshal(options.Parameters[0])
			return string(param), err
		} else {
			params, err := json.Marshal(options.Parameters)
			return string(params), err
		}
	}()
	if err != nil {
		return nil, fmt.Errorf("error marshalling to JSON: %v", err)
	}

	ID := s.getNewUniqueID()

	var rows string
	if options.Rows > 0 {
		rows = fmt.Sprint(options.Rows)
	}
	if options.Rows > MAX_FETCH_SIZE {
		rows = fmt.Sprint(MAX_FETCH_SIZE)
	}

	query := &Query{
		ID:          ID,
		parameters:  jsonParams,
		rowsToFetch: rows,
		terse:       options.TerseResult,
		job:         s,
	}
	query.state.Store(STATE_NOT_YET_RUN)

	if options.Parameters != nil {
		query.prepared = true
	}

	if options.IsCLcommand {
		query.clCommand = command
	} else {
		query.sqlQuery = command
	}

	s.queryList.addQuery(query)
	return query, nil
}

// Set trace configuration options
func (s *SQLJob) SetTraceConfig(ops TraceOptions) error {
	return s.SetTraceConfigContext(context.Background(), ops)
}

// Set trace configuration options
func (s *SQLJob) SetTraceConfigContext(ctx context.Context, ops TraceOptions) error {
	var jsonreq string
	id := s.newRequestID("setconfig")

	var allFields = ops.Tracelevel != "" && ops.Tracedest != "" && ops.Jtopentracedest != "" && ops.Jtopentracelevel != ""
	var jtFields = ops.Jtopentracelevel != "" && ops.Jtopentracedest != ""
	var traceFields = ops.Tracelevel != "" && ops.Tracedest != ""

	if s.getConn() == nil {
		return &WebsocketError{Method: "SetTraceConfig()", Message: "need a connection"}
	}

	if allFields {
		jsonreq =
			fmt.Sprintf(`{"id":"%s","type":"setconfig","jtopentracelevel":"%s","jtopentracedest":"%s","tracelevel":"%s","tracedest":"%s"}`, id, ops.Jtopentracelevel, ops.Jtopentracedest, ops.Tracelevel, ops.Tracedest)

	} else if traceFields {
		jsonreq =
			fmt.Sprintf(`{"id":"%s","type":"setconfig","tracelevel":"%s","tracedest":"%s"}`, id, ops.Tracelevel, ops.Tracedest)

	} else if jtFields {
		jsonreq =
			fmt.Sprintf(`{"id":"%s","type":"setconfig","tracelevel":"%s","tracedest":"%s"}`, id, ops.Jtopentracelevel, ops.Jtopentracedest)
	} else {
		return fmt.Errorf("need atleast 2 fields; level and dest of the same tracer")
	}

	resp, err := s.request(ctx, serverRequest{id: id, jsonreq: jsonreq}, true)
	if err != nil {
		return err
	}

	trace := &TraceOptions{}
	err = json.Unmarshal(resp, trace)
	if err != nil {
		unmarshalErr := fmt.Errorf("unmarshal error in SetTraceConfig() method: ")
		err = errors.Join(unmarshalErr, err)
		return err
	}

	ops.tracing = true
	s.mu.Lock()
	s.Options = &ops
	s.mu.Unlock()
	return nil
}

// Receive trace data (after setting config)
func (s *SQLJob) GetTraceData() error {
	return s.GetTraceDataContext(context.Background())
}

// Receive trace data (after setting config)
func (s *SQLJob) GetTraceDataContext(ctx context.Context) error {
	s.mu.Lock()
	var options TraceOptions
	if s.Options != nil {
		options = *s.Options
	}
	s.mu.Unlock()

	if !options.tracing {
		return fmt.Errorf("need to set the trace config")
	}

	if s.getConn() == nil {
		return &WebsocketError{Method: "GetTraceData()", Message: "need a connection"}
	}

	id := s.newRequestID("gettracedata")
	jsonreq := fmt.Sprintf(`{"id":"%v","type":"gettracedata"}`, id)

	resp, err := s.request(ctx, serverRequest{id: id, jsonreq: jsonreq}, true)
	if err != nil {
		return err
	}

	var data traceData
	data.Tracedest = options.Tracedest
	data.Jtopentracedest = options.Jtopentracedest

	err = json.Unmarshal(resp, &data)
	if err != nil {
		unmarshalErr := fmt.Errorf("unmarshal error in GetTraceData() method: ")
		err = errors.Join(unmarshalErr, err)
		return err
	}

	err = createTraceFile(data)
	if err != nil {
		return err
	}

	return nil
}

// Creates a file for the trace data
func createTraceFile(t traceData) error {
	if t.Tracedest == "file" || t.Tracedest == "FILE" {
		filename := "trace-" + time.Now().Format("2002-01-02") + ".html"
		file, err := os.Create(filename)
		if err != nil {
			return fmt.Errorf("error creating file: %v", err)
		}

		_, err = file.Write([]byte(t.Tracedata))
		if err != nil {
			return fmt.Errorf("error writing to file: %v", err)
		}
	}
	if t.Jtopentracedest == "file" || t.Jtopentracedest == "FILE" {
		filename := "jtopentrace-" + time.Now().Format("2002-01-02") + ".txt"
		file, err := os.Create(filename)
		if err != nil {
			return fmt.Errorf("error creating file: %v", err)
		}

		_, err = file.Write([]byte(t.Jtopentracedata))
		if err != nil {
			return fmt.Errorf("error writing to file: %v", err)
		}
	}

	return nil
}

// Closes the SQL job and websocket connection.
// Requests still in flight on the job fail with a WebsocketError.
func (s *SQLJob) Close() error {
	s.mu.Lock()
	conn := s.conn
	if conn == nil {
		s.mu.Unlock()
		return &WebsocketError{Method: "Close()", Message: "need a connection"}
	}
	s.conn = nil
	s.Options = nil
	s.Status = JOBSTATUS_ENDED
	s.mu.Unlock()

	exit := fmt.Sprintf(`{"id":"%s","type":"exit"}`, s.newRequestID("exit"))
	if err := conn.close([]byte(exit)); err != nil {
		var wsErr *WebsocketError
		if errors.As(err, &wsErr) {
			wsErr.Method = "Close()"
		}
		return err
	}
	return nil
}

// Receive the current job status
func (s *SQLJob) GetStatus() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Status
}

// Receive the current version info
func (s *SQLJob) GetVersion() (string, error) {
	return s.GetVersionContext(context.Background())
}

// Receive the current version info
func (s *SQLJob) GetVersionContext(ctx context.Context) (string, error) {
	if s.getConn() == nil {
		return "", &WebsocketError{Method: "GetVersion()", Message: "need a connection"}
	}
	id := s.newRequestID("getversion")
	jsonreq := fmt.Sprintf(`{"id":"%s","type":"getversion"}`, id)

	resp, err := s.request(ctx, serverRequest{id: id, jsonreq: jsonreq}, true)
	if err != nil {
		return "", err
	}

	var response struct{ Version string }
	err = json.Unmarshal(resp, &response)
	if err != nil {
		unmarshalErr := fmt.Errorf("unmarshal error in GetVersion() method: ")
		err = errors.Join(unmarshalErr, err)
		return "", err
	}

	return response.Version, nil
}

// Receive the name of the Job
func (s *SQLJob) getDBJob() (string, error) {
	return s.getDBJobContext(context.Background())
}

func (s *SQLJob) getDBJobContext(ctx context.Context) (string, error) {
	if s.ID == "" {
		return "", fmt.Errorf("need a job ID")
	}

	id := s.newRequestID("getdbjob")
	jsonreq := fmt.Sprintf(`{"id":"%v","type":"getdbjob"}`, id)

	resp, err := s.send(ctx, serverRequest{id: id, jsonreq: jsonreq})
	if err != nil {
		return "", err
	}

	s.mu.Lock()
	s.Jobname = resp.Job
	s.mu.Unlock()

	resp.IsDone = true
	return resp.Job, nil
}

// Receive a new unique ID for a query
func (s *SQLJob) getNewUniqueID() string {
	count := s.counter.Add(1)
	ID := strconv.Itoa(int(count))
	return ID
}

// Receive a new unique ID for an internal request. The prefix keeps these
// IDs from colliding with the numeric query IDs.
func (s *SQLJob) newRequestID(kind string) string {
	return kind + "-" + strconv.FormatUint(s.reqCounter.Add(1), 10)
}

// Set the current job status. Once a job has ended, only a new connection
// attempt can change its status, so requests failing after Close() don't
// overwrite it.
func (s *SQLJob) setJobStatus(status string) {
	s.mu.Lock()
	if s.Status != JOBSTATUS_ENDED || status == JOBSTATUS_CONNECTING {
		s.Status = status
	}
	s.mu.Unlock()
}

func (s *SQLJob) getJobname() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Jobname
}

func (s *SQLJob) getDaemon() DaemonServer {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.daemon
}

func (s *SQLJob) getConn() *wsConn {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn
}

// Reports whether the job has a live connection
func (s *SQLJob) isConnected() bool {
	conn := s.getConn()
	return conn != nil && !conn.isClosed()
}

// Drops the connection, e.g. after a websocket error
func (s *SQLJob) dropConnection() {
	s.mu.Lock()
	conn := s.conn
	s.conn = nil
	s.mu.Unlock()
	if conn != nil {
		conn.close(nil)
	}
}
