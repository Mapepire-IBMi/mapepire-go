package mapepire

import (
	"log"
	"os"
	"testing"

	"github.com/joho/godotenv"
)

func getServer() DaemonServer {
	godotenv.Load(".env")
	return DaemonServer{
		Host:               os.Getenv("VITE_SERVER"),
		User:               os.Getenv("VITE_DB_USER"),
		Password:           os.Getenv("VITE_DB_PASS"),
		Port:               os.Getenv("VITE_PORT"),
		Properties:         "prompt=false;translate binary=true;naming=system",
		IgnoreUnauthorized: true,
	}
}

var server = getServer()

func TestConnect(t *testing.T) {

	job := NewSQLJob("test")
	have := job.Connect(server)

	if have != nil {
		t.Errorf("should not throw error")
	}
}

// Connect to invalid Host
func TestConnectInvalidHost(t *testing.T) {
	daemon := DaemonServer{
		Host:               "invalidHost.de",
		User:               "invalid",
		Password:           "pw",
		Technique:          "TCP",
		IgnoreUnauthorized: false,
	}

	job := NewSQLJob("test")
	have := job.Connect(daemon)

	if have == nil {
		t.Errorf("should throw error")
	}
}

func TestConnectNoProps(t *testing.T) {

	daemon := DaemonServer{
		Host:               server.Host,
		User:               server.User,
		Password:           server.Password,
		Port:               server.Port,
		Properties:         "",
		IgnoreUnauthorized: true,
	}

	job := NewSQLJob("test")
	have := job.Connect(daemon)

	if have != nil {
		t.Errorf("should not throw error")
	}
}

// Connect to DB with technique
func TestConnectTechnique(t *testing.T) {
	daemon := DaemonServer{
		Host:               server.Host,
		User:               server.User,
		Password:           server.Password,
		Technique:          "TCP",
		IgnoreUnauthorized: true,
	}

	job := NewSQLJob("test")
	have := job.Connect(daemon)

	if have != nil {
		t.Errorf("should not throw error")
	}
}

// Connect to database with invalid properties
func TestConnectInvalid(t *testing.T) {
	serveri := DaemonServer{
		Host:               server.Host,
		User:               "fakeuser",
		Password:           "fakepw",
		Port:               "8076",
		IgnoreUnauthorized: false,
	}

	job := NewSQLJob("test")
	have := job.Connect(serveri)

	if have == nil {
		t.Errorf("should throw error")
	}
}

// Implicit disconnection on new connect request
func TestConnect2(t *testing.T) {

	// first connection
	job := NewSQLJob("test")
	job.Connect(server)
	name1, _ := job.getDBJob()

	// second connection
	job2 := NewSQLJob("test2")
	job2.Connect(server)
	name2, _ := job2.getDBJob()

	if name1 == name2 {
		t.Errorf("Should not be the same Jobs")
	}
}

// valid Query
func TestQuery(t *testing.T) {

	queryops := QueryOptions{
		Rows: 5,
	}
	_, have := initSQLTable("SELECT * FROM TEMPTEST", queryops)

	want := Query{
		ID: "3",
	}
	want.state.Store(STATE_NOT_YET_RUN)

	if have.ID != want.ID {
		t.Errorf("got %v, want %v", have.ID, want.ID)
	}
	if have.state != want.state {
		t.Errorf("got %v, want %v", have.state.Load(), want.state.Load())
	}
}

// Query invalid
func TestQueryInvalid2(t *testing.T) {
	job := NewSQLJob("test")
	job.Connect(server)

	queryops := QueryOptions{
		TerseResult: true,
		Rows:        5,
	}
	_, err := job.QueryWithOptions("", queryops)

	if err == nil {
		t.Errorf("should throw error")
	}
}

// Query with 1 param
func TestQueryParams(t *testing.T) {
	job := NewSQLJob("test")
	job.Connect(server)

	query, _ := job.Query("DECLARE GLOBAL TEMPORARY TABLE TEMPTEST (ID CHAR(8) NOT NULL, DESCRIPTION VARCHAR(60) NOT NULL, SERIALNO CHAR(12) NOT NULL)")
	resp, _ := query.Execute()
	if resp.Error != nil {
		t.Errorf("should not throw error")
	}

	insertquery, _ := job.Query(`INSERT INTO TEMPTEST VALUES (1, 'Lorem ipsum', 121212),
			(2, 'dolor sit amet', 232323),
			(3, 'consetetur sadipscing elitr', 343434),
			(4, 'sed diam nonumy', 454545),
			(5, 'eirmod tempor', 565656)`)
	insertquery.Execute()

	queryops := QueryOptions{
		Rows:       5,
		Parameters: [][]any{{3}},
	}
	_, err := job.QueryWithOptions("SELECT * FROM TEMPTEST WHERE ID = ?", queryops)

	if err != nil {
		t.Errorf("should not throw error")
	}
}

// Query with 2 params
func TestQueryMoreParams(t *testing.T) {
	job := NewSQLJob("test")
	job.Connect(server)

	query, _ := job.Query("DECLARE GLOBAL TEMPORARY TABLE TEMPTEST (ID CHAR(8) NOT NULL, DESCRIPTION VARCHAR(60) NOT NULL, SERIALNO CHAR(12) NOT NULL)")
	resp, _ := query.Execute()
	if resp.Error != nil {
		t.Errorf("should not throw error")
	}

	insertquery, _ := job.Query(`INSERT INTO TEMPTEST VALUES (1, 'Lorem ipsum', 121212),
			(2, 'dolor sit amet', 232323),
			(3, 'consetetur sadipscing elitr', 343434),
			(4, 'sed diam nonumy', 454545),
			(5, 'eirmod tempor', 565656)`)
	insertquery.Execute()

	queryops := QueryOptions{
		Rows:       5,
		Parameters: [][]any{{3, "343434"}},
	}
	_, err := job.QueryWithOptions("SELECT * FROM TEMPTEST WHERE ID = ? AND SERIALNO = ?", queryops)

	if err != nil {
		t.Errorf("should not throw error")
	}
}

// Set trace config
func TestSetTraceConfig1(t *testing.T) {
	job := NewSQLJob("test")
	job.Connect(server)

	traceops := TraceOptions{
		Tracelevel: "ERRORS",
		Tracedest:  "file",
	}

	err := job.SetTraceConfig(traceops)
	log.Println(err)
	if err != nil {
		t.Errorf("should not throw error")
	}
}

// Set trace config (jtopentrace)
func TestSetTraceConfig2(t *testing.T) {
	job := NewSQLJob("test")
	job.Connect(server)

	traceops := TraceOptions{
		Jtopentracelevel: "ERRORS",
		Jtopentracedest:  "file",
	}

	err := job.SetTraceConfig(traceops)
	if err != nil {
		t.Errorf("should not throw error")
	}
}

// Set trace config (all options)
func TestSetTraceConfig3(t *testing.T) {
	job := NewSQLJob("test")
	job.Connect(server)

	traceops := TraceOptions{
		Jtopentracelevel: "ERRORS",
		Jtopentracedest:  "file",
		Tracelevel:       "ERRORS",
		Tracedest:        "file",
	}

	err := job.SetTraceConfig(traceops)
	if err != nil {
		t.Errorf("should not throw error")
	}
}

// Set trace config invalid
func TestSetTraceConfigInvalid1(t *testing.T) {
	job := NewSQLJob("test")
	job.Connect(server)

	traceops := TraceOptions{
		Jtopentracelevel: "ERRORS",
	}

	err := job.SetTraceConfig(traceops)
	if err == nil {
		t.Errorf("should throw error")
	}
}

// Set trace config invalid 2
func TestSetTraceConfigInvalid2(t *testing.T) {
	job := NewSQLJob("test")
	job.Connect(server)

	traceops := TraceOptions{
		Tracelevel: "ERRORS",
	}

	err := job.SetTraceConfig(traceops)
	if err == nil {
		t.Errorf("should throw error")
	}
}

// Set trace config invalid 3
func TestSetTraceConfigInvalid3(t *testing.T) {

	job := NewSQLJob("test")
	job.Connect(server)

	traceops := TraceOptions{
		Tracelevel: "something",
		Tracedest:  "else",
	}

	err := job.SetTraceConfig(traceops)
	if err == nil {
		t.Errorf("should throw error")
	}
}

// Get trace data
func TestGetTraceData(t *testing.T) {
	job := NewSQLJob("test")
	job.Connect(server)

	traceops := TraceOptions{
		Tracelevel: "ERRORS",
		Tracedest:  "file",
	}

	err := job.SetTraceConfig(traceops)
	if err != nil {
		t.Errorf("should not throw error")
	}

	err = job.GetTraceData()
	if err != nil {
		t.Errorf("should not throw error")
	}
}

// Get trace data jtopentrace
func TestGetTraceData2(t *testing.T) {
	job := NewSQLJob("test")
	job.Connect(server)

	traceops := TraceOptions{
		Jtopentracelevel: "ERRORS",
		Jtopentracedest:  "file",
	}

	err := job.SetTraceConfig(traceops)
	if err != nil {
		t.Errorf("should not throw error")
	}

	err = job.GetTraceData()
	if err != nil {
		t.Errorf("should not throw error")
	}
}

// Get trace data invalid
func TestGetTraceDataInvalid(t *testing.T) {
	job := NewSQLJob("test")
	job.Connect(server)

	err := job.GetTraceData()
	if err == nil {
		t.Errorf("should throw error")
	}
}

// Close connection
func TestClose(t *testing.T) {
	job := NewSQLJob("test")
	job.Connect(server)
	err := job.Close()
	if err != nil {
		t.Errorf("should not throw error")
	}
}

// Close connection invalid
func TestCloseInvalid(t *testing.T) {
	job := NewSQLJob("test")
	err := job.Close()
	if err == nil {
		t.Errorf("should throw error")
	}
}

// Get version
func TestGetVersion(t *testing.T) {
	job := NewSQLJob("test")
	job.Connect(server)

	version, err := job.GetVersion()
	if err != nil {
		t.Errorf("should not throw error")
	}
	if version == "" {
		t.Errorf("should not throw error")
	}
}

// Get status
func TestGetStatus(t *testing.T) {
	job := NewSQLJob("test")
	job.Connect(server)
	status := job.GetStatus()

	if job.Status != status {
		t.Errorf("have %v, want %v", job.Status, status)
	}
	if job.Status != "READY" {
		t.Errorf("wrong status")
	}
}
