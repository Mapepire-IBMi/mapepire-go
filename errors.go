package mapepire

import "fmt"

type WebsocketError struct {
	Method  string
	Message string
}

func (e *WebsocketError) Error() string {
	return fmt.Sprintf("websocket error in %v method: %v", e.Method, e.Message)
}

// TimeoutError is a websocket deadline expiry; the connection is desynced and must be closed
type TimeoutError struct {
	Method  string
	Message string
}

func (e *TimeoutError) Error() string {
	return fmt.Sprintf("timeout error in %v method: %v", e.Method, e.Message)
}

type ServerError struct {
	Method  string
	Message string
}

// DeadJobError means the DB2 job behind the websocket is gone; close the job, don't return it to the pool
type DeadJobError struct {
	Method   string
	SqlState string
	SqlRC    int
}

func (e *DeadJobError) Error() string {
	return fmt.Sprintf("dead db2 job in %v method: sql_state=%v sql_rc=%v", e.Method, e.SqlState, e.SqlRC)
}

// IsDeadJob reports whether sqlState/sqlRC indicate a dead DB2 job (HY017 only)
func IsDeadJob(sqlState string, sqlRC int) bool {
	return sqlState == "HY017"
}

func (e *ServerError) Error() string {
	return fmt.Sprintf("server error in %v method: %v", e.Method, e.Message)
}
