package mapepire

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// Represents options for query execution
type QueryOptions struct {
	Rows        int     // The amount of rows to fetch
	Parameters  [][]any // Parameters, if any
	TerseResult bool    // Whether the result returns in terse format
	IsCLcommand bool    // Whether the command is a CL command
}

// Represents a SQL Query that can be executed and managed within a SQL job
type Query struct {
	ID          string       // The unique identifier
	clCommand   string       // CL command
	sqlQuery    string       // SQL query
	parameters  string       // Parameters, if any
	terse       bool         // Whether the result returns in terse format
	rowsToFetch string       // The amount of rows to fetch
	prepared    bool         // Whether the query has been prepared
	job         *SQLJob      // Pointer to the SQL Job
	state       atomic.Int32 // The current state of the query
}

// Represents a query list managed by the job
type queryList struct {
	list []*Query   // List of all open queries
	lock sync.Mutex // Mutex
}

const (
	STATE_RUN_DONE = iota
	STATE_RUN_MORE_DATA
	STATE_NOT_YET_RUN
	STATE_RUNNING
)

// Receive a new query list
func newQueryList() *queryList {
	return &queryList{list: []*Query{}}
}

// Add a new Query to the list
func (ql *queryList) addQuery(query *Query) {
	ql.lock.Lock()
	ql.list = append(ql.list, query)
	ql.lock.Unlock()
}

// validates the cont_id
func (ql *queryList) validateID(ID string) bool {
	ql.lock.Lock()
	defer ql.lock.Unlock()

	for _, query := range ql.list {
		if query.ID == ID && query.state.Load() != STATE_RUN_DONE {
			return true
		}
	}
	return false
}

// cleans up the query list
func (ql *queryList) cleanup(ctx context.Context) {
	ql.lock.Lock()
	defer ql.lock.Unlock()

	newList := make([]*Query, 0, len(ql.list))
	for _, query := range ql.list {
		if query.state.Load() != STATE_RUN_DONE {
			newList = append(newList, query)
		} else {
			query.sqlCloseUnsafe(ctx, query.ID)
		}
	}
	ql.list = newList
}

// Executes the query/command and returns the results
func (q *Query) Execute() (*ServerResponse, error) {
	return q.ExecuteContext(context.Background())
}

// Executes the query/command and returns the results.
// The context bounds how long to wait for the server's response.
func (q *Query) ExecuteContext(ctx context.Context) (*ServerResponse, error) {
	q.job.setJobStatus(JOBSTATUS_BUSY)

	if !q.state.CompareAndSwap(STATE_NOT_YET_RUN, STATE_RUNNING) {
		q.job.setJobStatus(JOBSTATUS_ERROR)
		err := fmt.Errorf("statement has already been run")
		if q.state.Load() == STATE_RUNNING {
			err = fmt.Errorf("statement is already running")
		}
		return &ServerResponse{ID: q.ID}, err
	}

	jsonreq := func() string {
		if q.clCommand != "" {
			return fmt.Sprintf(`{"id":"%s","type":"cl","cmd":"%s","terse":%t}`, q.ID, q.clCommand, q.terse)
		}
		if q.prepared {
			return fmt.Sprintf(`{"id":"%s","type":"prepare_sql_execute","sql":"%s","parameters":%s,"rows":"%s","terse":%t}`, q.ID, q.sqlQuery, q.parameters, q.rowsToFetch, q.terse)
		}
		return fmt.Sprintf(`{"id":"%s","type":"sql","sql":"%s","rows":"%s","terse":%t}`, q.ID, q.sqlQuery, q.rowsToFetch, q.terse)
	}()

	request := &serverRequest{
		id:      q.ID,
		jsonreq: jsonreq,
		terse:   q.terse,
	}

	resp, err := q.sendRequest(ctx, request)
	if err != nil {
		// Allow the statement to be retried
		q.state.CompareAndSwap(STATE_RUNNING, STATE_NOT_YET_RUN)
	}
	return resp, err
}

// Fetch more rows from a previous request with the ID
func (q *Query) FetchMore(contID string, rows string) (*ServerResponse, error) {
	return q.FetchMoreContext(context.Background(), contID, rows)
}

// Fetch more rows from a previous request with the ID
func (q *Query) FetchMoreContext(ctx context.Context, contID string, rows string) (*ServerResponse, error) {
	q.job.setJobStatus(JOBSTATUS_BUSY)

	resp := &ServerResponse{
		ID: q.ID,
	}

	valid := q.job.queryList.validateID(contID)
	if !valid {
		q.job.setJobStatus(JOBSTATUS_ERROR)
		err := fmt.Errorf("need ID from previous SQL")
		return resp, err
	} else if state := q.state.Load(); state == STATE_NOT_YET_RUN || state == STATE_RUNNING {
		q.job.setJobStatus(JOBSTATUS_ERROR)
		err := fmt.Errorf("statement has not yet been run")
		return resp, err
	}

	jsonreq :=
		fmt.Sprintf(`{"id":"%s","type":"sqlmore","cont_id":"%s","rows":"%s"}`, q.ID, contID, rows)

	request := &serverRequest{
		id:      q.ID,
		jsonreq: jsonreq,
		terse:   q.terse,
	}

	response, err := q.sendRequest(ctx, request)
	if err != nil {
		return resp, err
	}

	if response.Success {
		response.HasResults = true
	}
	return response, nil
}

// Close cursor from a previous request.
// Select querys are automatically closed after fetching all data.
func (q *Query) SQLClose(contID string) error {
	return q.SQLCloseContext(context.Background(), contID)
}

// Close cursor from a previous request.
func (q *Query) SQLCloseContext(ctx context.Context, contID string) error {

	valid := q.job.queryList.validateID(contID)
	if !valid {
		q.job.setJobStatus(JOBSTATUS_ERROR)
		return fmt.Errorf("need ID from previous query")
	}

	return q.sqlCloseUnsafe(ctx, contID)
}

// Close cursor from a previous request without validating existence of contID
func (q *Query) sqlCloseUnsafe(ctx context.Context, contID string) error {
	q.job.setJobStatus(JOBSTATUS_BUSY)
	jsonreq :=
		fmt.Sprintf(`{"id":"%v","type":"sqlclose","cont_id":"%v"}`, q.ID, contID)

	request := &serverRequest{
		id:      q.ID,
		jsonreq: jsonreq,
	}

	_, err := q.job.send(ctx, *request)
	if err != nil {
		q.job.setJobStatus(JOBSTATUS_ERROR)
		return err
	}

	return nil
}

// sends the request and sets the query state
func (q *Query) sendRequest(ctx context.Context, request *serverRequest) (*ServerResponse, error) {
	resp, err := q.job.send(ctx, *request)
	if err != nil {
		q.job.setJobStatus(JOBSTATUS_ERROR)
		return resp, err
	}

	if resp.IsDone && resp.Success {
		q.state.Store(STATE_RUN_DONE)
	} else if resp.Success && !resp.IsDone {
		q.state.Store(STATE_RUN_MORE_DATA)
	}
	q.job.queryList.cleanup(ctx)

	q.job.setJobStatus(JOBSTATUS_READY)
	return resp, nil
}
