package mapepire

import (
	"bytes"
	"fmt"
	"log"
	"testing"
)

func initSQLTable(command string, queryops2 QueryOptions) (*SQLJob, *Query) {
	job := NewSQLJob("test")
	err := job.Connect(server)
	if err != nil {
		log.Fatal(err)
	}

	query, _ := job.Query("CREATE TABLE qtemp.TEMPTEST (ID decimal(8) NOT NULL, DESCRIPTION VARCHAR(60) NOT NULL, SERIALNO CHAR(12) NOT NULL)")
	resp, _ := query.Execute()
	if resp.Error != nil {
		log.Fatal(resp.Error)
	}

	insertquery, _ := job.Query(`INSERT INTO TEMPTEST VALUES (1, 'Lorem ipsum', 121212),
	(2, 'dolor sit amet', 232323),
	(3, 'consetetur sadipscing elitr', 343434),
	(4, 'sed diam nonumy', 454545),
	(5, 'eirmod tempor', 565656)`)
	resp2, _ := insertquery.Execute()
	log.Println(resp2)

	query2, err := job.QueryWithOptions(command, queryops2)
	if err != nil {
		log.Fatal(err)
	}
	return job, query2
}

// Execute SQL
func TestNewExecute(t *testing.T) {
	queryops := QueryOptions{
		Rows: 5,
	}
	_, query := initSQLTable("SELECT * FROM TEMPTEST", queryops)
	have, _ := query.Execute()

	want := &ServerResponse{
		ID:         "3",
		Success:    true,
		IsDone:     false,
		Error:      nil,
		HasResults: true,
	}

	if have.ID != want.ID {
		t.Errorf("got %s, want %s", have.ID, want.ID)
	}
	if have.Success != want.Success {
		t.Errorf("got %t, want %t", have.Success, want.Success)
	}
	if have.Error != want.Error {
		t.Errorf("got %s, want %s", have.Error, want.Error)
	}
	if have.IsDone != want.IsDone {
		t.Errorf("got %t, want %t", have.IsDone, want.IsDone)
	}
	if have.HasResults != want.HasResults {
		t.Errorf("got %v, want %v", have.HasResults, want.HasResults)
	}
	if have.Data == nil {
		t.Errorf("should have data")
	}
	if have.Metadata == nil {
		t.Errorf("should have metadata")
	}
}

// Execute SQL, response in terse
func TestNewExecuteTerse(t *testing.T) {
	queryops := QueryOptions{
		Rows:        5,
		TerseResult: true,
	}
	_, query := initSQLTable("SELECT * FROM TEMPTEST", queryops)
	have, _ := query.Execute()

	want := &ServerResponse{
		ID:         "3",
		Success:    true,
		IsDone:     false,
		Error:      nil,
		HasResults: true,
	}

	if have.ID != want.ID {
		t.Errorf("got %s, want %s", have.ID, want.ID)
	}
	if have.Success != want.Success {
		t.Errorf("got %t, want %t", have.Success, want.Success)
	}
	if have.Error != want.Error {
		t.Errorf("got %s, want %s", have.Error, want.Error)
	}
	if have.IsDone != want.IsDone {
		t.Errorf("got %t, want %t", have.IsDone, want.IsDone)
	}
	if have.HasResults != want.HasResults {
		t.Errorf("got %v, want %v", have.HasResults, want.HasResults)
	}
	if have.Data != nil {
		t.Errorf("should be in Terse_data")
	}
	if have.TerseData == nil {
		t.Errorf("should have Terse_data")
	}
	if have.Metadata == nil {
		t.Errorf("should have metadata")
	}
}

// Execute SQL invalid
func TestNewExecuteInvalid(t *testing.T) {
	queryops := QueryOptions{
		Rows: 5,
	}
	_, query := initSQLTable("SELECT * FROM INVALIDTABLE", queryops)
	have, _ := query.Execute()

	want := &ServerResponse{
		ID:         "3",
		Success:    false,
		IsDone:     false,
		HasResults: false,
	}

	if have.ID != want.ID {
		t.Errorf("got %s, want %s", have.ID, want.ID)
	}
	if have.Success != want.Success {
		t.Errorf("got %t, want %t", have.Success, want.Success)
	}
	if have.Error == nil {
		t.Errorf("should throw Error")
	}
	if have.IsDone != want.IsDone {
		t.Errorf("got %t, want %t", have.IsDone, want.IsDone)
	}
	if have.HasResults != want.HasResults {
		t.Errorf("got %v, want %v", have.HasResults, want.HasResults)
	}
	if have.Data != nil {
		t.Errorf("should not have data")
	}
	if have.Metadata != nil {
		t.Errorf("should not have metadata")
	}
}

// Execute SQL insert
func TestNewExecuteInsert(t *testing.T) {
	job := NewSQLJob("test")
	job.Connect(server)

	query, _ := job.Query("DECLARE GLOBAL TEMPORARY TABLE TEMPTEST (ID CHAR(8) NOT NULL, DESCRIPTION VARCHAR(60) NOT NULL, SERIALNO CHAR(12) NOT NULL)")
	resp, _ := query.Execute()
	if resp.Error != nil {
		log.Fatal(resp.Error)
	}

	insertquery, _ := job.Query("INSERT INTO TEMPTEST VALUES (1, 'testtest', 121212)")
	have, _ := insertquery.Execute()

	want := ServerResponse{
		ID:          "2",
		Success:     true,
		HasResults:  false,
		UpdateCount: 1,
	}

	if have.ID != want.ID {
		t.Errorf("got %s, want %s", have.ID, want.ID)
	}
	if have.Success != want.Success {
		t.Errorf("got %t, want %t", have.Success, want.Success)
	}
	if have.Error != nil {
		t.Errorf("should not throw Error")
	}
	if have.UpdateCount != want.UpdateCount {
		t.Errorf("have %v, want %v", have.UpdateCount, want.UpdateCount)
	}
	if have.HasResults != want.HasResults {
		t.Errorf("have %v, want %v", have.HasResults, want.HasResults)
	}
}

func TestBlobInsertAndSelect(t *testing.T) {
	job := NewSQLJob("test")
	if err := job.Connect(server); err != nil {
		t.Fatal(err)
	}

	createQuery, err := job.Query("CREATE TABLE qtemp.TEMPBLOBTEST (ID INT NOT NULL, DATA BLOB(100) NOT NULL)")
	if err != nil {
		t.Fatal(err)
	}
	if response, err := createQuery.Execute(); err != nil || response.Error != nil {
		t.Fatalf("create blob table failed: response=%v, error=%v", response, err)
	}

	want := []byte("blob test data")
	insertQuery, err := job.QueryWithOptions(
		"INSERT INTO qtemp.TEMPBLOBTEST (ID, DATA) VALUES (?, ?)",
		QueryOptions{Parameters: [][]any{{1, NewBlob(want)}}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if response, err := insertQuery.Execute(); err != nil || response.Error != nil {
		t.Fatalf("insert blob failed: response=%v, error=%v", response, err)
	}

	selectQuery, err := job.Query("SELECT DATA FROM qtemp.TEMPBLOBTEST WHERE ID = 1")
	if err != nil {
		t.Fatal(err)
	}
	response, err := selectQuery.Execute()
	if err != nil {
		t.Fatal(err)
	}
	if response.Error != nil {
		t.Fatalf("select blob failed: %v", response.Error)
	}
	if len(response.Data) != 1 {
		t.Fatalf("got %d result rows, want 1", len(response.Data))
	}

	dataColumn := response.Data[0]["DATA"]
	blobRef := dataColumn.ToBlobRef()
	if blobRef == (BlobRef{}) {
		t.Fatal("select did not return a blob reference")
	}

	got, err := job.FetchBlob(blobRef)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("fetched blob = %q, want %q", got, want)
	}
}

// Execute SQL update
func TestNewExecuteUpdate(t *testing.T) {
	queryops := QueryOptions{}

	job, query := initSQLTable("UPDATE TEMPTEST SET ID = 5, DESCRIPTION = 'test', SERIALNO = 545454 WHERE ID = 1", queryops)
	update, _ := query.Execute()
	if update.Error != nil {
		t.Errorf("should not throw error")
	}

	query2, _ := job.Query("SELECT * FROM TEMPTEST WHERE ID = 5")
	have, _ := query2.Execute()
	want := ServerResponse{
		ID:          "4",
		Success:     true,
		HasResults:  true,
		IsDone:      true,
		UpdateCount: 1,
	}

	if have.ID != want.ID {
		t.Errorf("got %s, want %s", have.ID, want.ID)
	}
	if have.Success != want.Success {
		t.Errorf("got %t, want %t", have.Success, want.Success)
	}
	if have.Error != nil {
		t.Errorf("should not throw Error")
	}
	if have.IsDone != want.IsDone {
		t.Errorf("got %t, want %t", have.IsDone, want.IsDone)
	}
	if have.HasResults != want.HasResults {
		t.Errorf("got %v, want %v", have.HasResults, want.HasResults)
	}
	if have.Data == nil {
		t.Errorf("should have data")
	}
	if have.Metadata == nil {
		t.Errorf("should have metadata")
	}
	if update.UpdateCount != want.UpdateCount {
		t.Errorf("have %v, want %v", have.UpdateCount, want.UpdateCount)
	}
}

// Execute SQL delete
func TestNewExecuteDelete(t *testing.T) {
	queryops := QueryOptions{}

	_, query := initSQLTable("DELETE FROM TEMPTEST WHERE ID = 1", queryops)
	have, _ := query.Execute()

	want := ServerResponse{
		ID:          "3",
		Success:     true,
		Error:       nil,
		UpdateCount: 1,
	}

	if have.ID != want.ID {
		t.Errorf("have %v, want %v", have.ID, want.ID)
	}
	if have.Error != want.Error {
		t.Errorf("should not throw error")
	}
	if have.Success != want.Success {
		t.Errorf("have %v, want %v", have.Success, want.Success)
	}
	if have.UpdateCount != want.UpdateCount {
		t.Errorf("have %v, want %v", have.UpdateCount, want.UpdateCount)
	}
}

func TestNewExecuteSelectParam(t *testing.T) {
	queryops := QueryOptions{
		Parameters: [][]any{{1}},
	}
	_, query := initSQLTable("SELECT * FROM TEMPTEST WHERE ID = ?", queryops)

	have, _ := query.Execute()

	want := &ServerResponse{
		ID:         "3",
		Success:    true,
		IsDone:     true,
		Error:      nil,
		HasResults: true,
	}

	if have.ID != want.ID {
		t.Errorf("got %s, want %s", have.ID, want.ID)
	}
	if have.Success != want.Success {
		t.Errorf("got %t, want %t", have.Success, want.Success)
	}
	if have.Error != want.Error {
		t.Errorf("should not throw error")
	}
	if have.IsDone != want.IsDone {
		t.Errorf("got %t, want %t", have.IsDone, want.IsDone)
	}
	if have.HasResults != want.HasResults {
		t.Errorf("got %v, want %v", have.HasResults, want.HasResults)
	}
	if have.Data == nil {
		t.Errorf("should have data")
	}
	if have.Metadata == nil {
		t.Errorf("should have metadata")
	}
}

// (v2.1.6)
func TestNewExecuteSelectParamInvalid(t *testing.T) {
	queryops := QueryOptions{
		Parameters: [][]any{{1}, {2}},
	}
	_, query := initSQLTable("SELECT * FROM TEMPTEST WHERE ID = ?", queryops)

	have, _ := query.Execute()

	if have.ID != "3" {
		t.Errorf("got %s, want 3", have.ID)
	}
	if have.Success {
		t.Errorf("got %t, want false", have.Success)
	}
	if have.Error == nil {
		t.Error("expected an error for mismatched parameter sets")
	}
	if have.SqlRC == 0 {
		t.Errorf("expected a non-zero SQL return code, got %v", have.SqlRC)
	}
	if have.SqlState == "" {
		t.Error("expected a SQL state")
	}
}

// Prepare and Execute SQL with Params
func TestNewExecuteUpdateParam(t *testing.T) {
	queryops := QueryOptions{
		Parameters: [][]any{{123, "test", "345678", "1"}, {321, "testtest", "876543", "2"}},
		Rows:       5,
	}
	_, query := initSQLTable("UPDATE TEMPTEST SET ID = ?, DESCRIPTION = ?, SERIALNO = ? WHERE ID = ?", queryops)

	have, _ := query.Execute()

	want := &ServerResponse{
		ID:             "3",
		Success:        true,
		IsDone:         true,
		Error:          nil,
		ParameterCount: 4,
	}

	if have.ID != want.ID {
		t.Errorf("got %s, want %s", have.ID, want.ID)
	}
	if have.Success != want.Success {
		t.Errorf("got %t, want %t", have.Success, want.Success)
	}
	if have.Error != want.Error {
		t.Errorf("got %s, want %s", have.Error, want.Error)
	}
	if have.IsDone != want.IsDone {
		t.Errorf("got %t, want %t", have.IsDone, want.IsDone)
	}
	if have.ParameterCount != want.ParameterCount {
		t.Errorf("got %v, want %v", have.ParameterCount, want.ParameterCount)
	}
	if have.Data == nil {
		t.Errorf("should have data")
	}
	if have.Metadata == nil {
		t.Errorf("should have metadata")
	}
}

func TestNewExecuteInsertParam(t *testing.T) {
	queryops := QueryOptions{
		Parameters: [][]any{{123, "test", "345678"}, {321, "testtest", "876543"}},
		Rows:       5,
	}
	_, query := initSQLTable("INSERT INTO TEMPTEST (ID, DESCRIPTION, SERIALNO) VALUES(?,?,?)", queryops)

	have, _ := query.Execute()

	want := &ServerResponse{
		ID:      "3",
		Success: true,
		Error:   nil,
	}

	if have.ID != want.ID {
		t.Errorf("got %s, want %s", have.ID, want.ID)
	}
	if have.Success != want.Success {
		t.Errorf("got %t, want %t", have.Success, want.Success)
	}
	if have.Error != want.Error {
		t.Errorf("got %s, want %s", have.Error, want.Error)
	}
}

func TestNewExecuteDeleteParam(t *testing.T) {
	queryops := QueryOptions{
		Parameters: [][]any{{1}, {2}},
	}
	_, query := initSQLTable("DELETE FROM TEMPTEST WHERE ID = ?", queryops)

	have, _ := query.Execute()

	want := &ServerResponse{
		ID:          "3",
		Success:     true,
		Error:       nil,
		UpdateCount: 2,
	}

	if have.ID != want.ID {
		t.Errorf("got %s, want %s", have.ID, want.ID)
	}
	if have.Success != want.Success {
		t.Errorf("got %t, want %t", have.Success, want.Success)
	}
	if have.Error != want.Error {
		t.Errorf("got %s, want %s", have.Error, want.Error)
	}
	if have.UpdateCount != want.UpdateCount {
		t.Errorf("got %v, want %v", have.UpdateCount, want.UpdateCount)
	}
}

// Execute CL command
func TestNewExecuteCL(t *testing.T) {
	queryops := QueryOptions{
		IsCLcommand: true,
	}

	job := NewSQLJob("test")
	job.Connect(server)

	command := fmt.Sprintf("DSPUSRPRF %s", server.User)
	query, err := job.QueryWithOptions(command, queryops)
	if err != nil {
		t.Errorf("should not throw error")
	}

	have, _ := query.Execute()

	want := &ServerResponse{
		ID:      "1",
		Success: true,
		Error:   nil,
		IsDone:  true,
	}

	if have.ID != want.ID {
		t.Errorf("got %s, want %s", have.ID, want.ID)
	}
	if have.Success != want.Success {
		t.Errorf("got %t, want %t", have.Success, want.Success)
	}
	if have.Error != want.Error {
		t.Errorf("got %s, want %s", have.Error, want.Error)
	}
	if have.IsDone != want.IsDone {
		t.Errorf("got %t, want %t", have.IsDone, want.IsDone)
	}
}

// Execute CL command invalid (v2.1.6)
func TestNewExecuteCLinvalid(t *testing.T) {
	queryops := QueryOptions{
		IsCLcommand: true,
	}
	job := NewSQLJob("test")
	job.Connect(server)

	query, err := job.QueryWithOptions("INVALIDCOMMAND", queryops)
	if err != nil {
		t.Errorf("should not throw error")
	}

	have, _ := query.Execute()

	if have.ID != "1" {
		t.Errorf("got %s, want 1", have.ID)
	}
	if have.Success {
		t.Errorf("got %t, want false", have.Success)
	}
	if have.Error == nil {
		t.Error("expected an error for an invalid CL command")
	}
	if have.SqlRC == 0 {
		t.Errorf("expected a non-zero SQL return code, got %v", have.SqlRC)
	}
	if have.SqlState == "" {
		t.Error("expected a SQL state")
	}
}

// FetchMore valid
func TestFetchMore(t *testing.T) {
	queryops := QueryOptions{
		Rows: 1,
	}
	_, query := initSQLTable("SELECT * FROM TEMPTEST", queryops)

	query.Execute()
	have, _ := query.FetchMore(query.ID, "5")

	want := &ServerResponse{
		ID:         "3",
		Success:    true,
		IsDone:     true,
		Error:      nil,
		HasResults: true,
	}

	if have.ID != want.ID {
		t.Errorf("got %s, want %s", have.ID, want.ID)
	}
	if have.Success != want.Success {
		t.Errorf("got %t, want %t", have.Success, want.Success)
	}
	if have.Error != want.Error {
		t.Errorf("got %v, want %v", have.Error, want.Error)
	}
	if have.IsDone != want.IsDone {
		t.Errorf("got %t, want %t", have.IsDone, want.IsDone)
	}
	if have.HasResults != want.HasResults {
		t.Errorf("got %v, want %v", have.HasResults, want.HasResults)
	}
	if have.Data == nil {
		t.Errorf("should have data")
	}
}

// Fetch more in terse format
func TestFetchMoreTerse(t *testing.T) {
	queryops := QueryOptions{
		Rows:        1,
		TerseResult: true,
	}
	_, query := initSQLTable("SELECT * FROM TEMPTEST", queryops)

	query.Execute()
	have, _ := query.FetchMore(query.ID, "1")

	want := &ServerResponse{
		ID:         "3",
		Success:    true,
		IsDone:     false,
		Error:      nil,
		HasResults: true,
	}

	if have.ID != want.ID {
		t.Errorf("got %s, want %s", have.ID, want.ID)
	}
	if have.Success != want.Success {
		t.Errorf("got %t, want %t", have.Success, want.Success)
	}
	if have.Error != want.Error {
		t.Errorf("got %s, want %s", have.Error, want.Error)
	}
	if have.IsDone != want.IsDone {
		t.Errorf("got %t, want %t", have.IsDone, want.IsDone)
	}
	if have.HasResults != want.HasResults {
		t.Errorf("got %v, want %v", have.HasResults, want.HasResults)
	}
	if have.Data != nil {
		t.Errorf("should be in Terse_data")
	}
	if have.TerseData == nil {
		t.Errorf("should have Terse_data")
	}
}

func TestFetchMoreinvalid(t *testing.T) {
	queryops := QueryOptions{
		Rows: 1,
	}

	_, query := initSQLTable("SELECT * FROM TEMPTEST", queryops)
	query.Execute()
	have, err := query.FetchMore("invalid", "1")

	want := &ServerResponse{
		Success:    false,
		HasResults: false,
	}

	if have.Success != want.Success {
		t.Errorf("got %t, want %t", have.Success, want.Success)
	}
	if err == nil {
		t.Errorf("should throw error")
	}
	if have.HasResults != want.HasResults {
		t.Errorf("got %v, want %v", have.HasResults, want.HasResults)
	}
}

// Fetch more invalid 2
func TestFetchMoreInvalid2(t *testing.T) {
	queryops := QueryOptions{
		Rows: 1,
	}

	_, query := initSQLTable("SELECT * FROM TEMPTEST", queryops)
	query.Execute()
	have, err := query.FetchMore("", "1")

	want := &ServerResponse{
		Success:    false,
		HasResults: false,
	}

	if have.Success != want.Success {
		t.Errorf("got %t, want %t", have.Success, want.Success)
	}
	if err == nil {
		t.Errorf("should throw error")
	}
	if have.HasResults != want.HasResults {
		t.Errorf("got %v, want %v", have.HasResults, want.HasResults)
	}
}

func TestFetchMoreinvalid3(t *testing.T) {
	queryops := QueryOptions{
		Rows: 1,
	}

	_, query := initSQLTable("SELECT * FROM TEMPTEST", queryops)
	have, err := query.FetchMore("5", "1")

	want := &ServerResponse{
		Success:    false,
		HasResults: false,
	}

	if have.Success != want.Success {
		t.Errorf("got %t, want %t", have.Success, want.Success)
	}
	if err == nil {
		t.Errorf("should throw error")
	}
	if have.HasResults != want.HasResults {
		t.Errorf("got %v, want %v", have.HasResults, want.HasResults)
	}
}

// SQL Close
func TestSQLClose(t *testing.T) {
	queryops := QueryOptions{
		Rows: 5,
	}
	_, query := initSQLTable("SELECT * FROM TEMPTEST", queryops)
	query.Execute()
	err := query.SQLClose(query.ID)
	if err != nil {
		t.Errorf("should not throw error")
	}
}

// SQL Close after query closes automatically
func TestSQLCloseAutomatic(t *testing.T) {
	queryops := QueryOptions{}
	_, query := initSQLTable("SELECT * FROM TEMPTEST", queryops)
	query.Execute()
	err := query.SQLClose(query.ID)
	if err == nil {
		t.Errorf("should throw error")
	}
}

// SQLClose null
func TestSQLCloseNULL(t *testing.T) {
	queryops := QueryOptions{
		Rows: 5,
	}
	_, query := initSQLTable("SELECT * FROM TEMPTEST", queryops)
	query.Execute()
	err := query.SQLClose("")
	if err == nil {
		t.Errorf("should throw error")
	}
}

// SQLClose invalid
func TestSQLCloseInvalid(t *testing.T) {
	queryops := QueryOptions{
		Rows: 50,
	}
	_, query := initSQLTable("SELECT * FROM TEMPTEST", queryops)
	query.Execute()
	err := query.SQLClose("invalid")
	if err == nil {
		t.Errorf("should throw error")
	}
}
