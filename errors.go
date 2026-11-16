package mapepire

import "fmt"

type WebsocketError struct {
	Method  string
	Message string
}

func (e *WebsocketError) Error() string {
	return fmt.Sprintf("websocket error in %v method: %v", e.Method, e.Message)
}

type ServerError struct {
	Method   string
	Message  string
	sqlRC    int    // SQL error code reported with the error
	sqlState string // SQL state reported with the error
}

func (e *ServerError) Error() string {
	return fmt.Sprintf("server error in %v method: %v", e.Method, e.Message)
}
