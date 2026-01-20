package mapepire

// Represents the server response.
type ServerResponse struct {
	ID             string // The unique identifier of the request
	Job            string // The name of the DB Job
	Success        bool   // Whether the request was successful
	Metadata       *metadata
	Data           []map[string]interface{}
	TerseData      [][]any `json:"terse_data"`      // Data in terse format if specified
	HasResults     bool    `json:"has_results"`     // Whether the response has results
	IsDone         bool    `json:"is_done"`         // Whether the query execution is complete
	ParameterCount int     `json:"parameter_count"` // The number of parameters in the prepared query
	UpdateCount    int     `json:"update_count"`    // The number of rows affected by the query (returns -1 if none)
	Error          error   // The error message, if any
	SqlState       string  // The SQL state code
	SqlRC          int     // The SQL error code
}

// Represents trace configuration options
type TraceOptions struct {
	Tracelevel       string // OFF, ON, ERRORS, DATASTREAM, INPUT_AND_ERRORS
	Tracedest        string // One of (file, in_mem)
	Jtopentracelevel string // OFF, ON, ERRORS, DATASTREAM, INPUT_AND_ERRORS
	Jtopentracedest  string // One of (file, in_mem)
	tracing          bool   // Whether tracing is enabled
}

// Stores the trace data
type traceData struct {
	Tracedata       string
	Tracedest       string
	Jtopentracedata string
	Jtopentracedest string
}

// Represents the request sent to the server
type serverRequest struct {
	id      string
	jsonreq string
}

// Represents metadata of the DB
type metadata struct {
	Job         string
	Columns     []column
	ColumnCount int `json:"column_count"`
}

// Represents the columns of the DB
type column struct {
	Name          string
	Type          string
	Label         string
	DisplaySize   int `json:"display_size"`
	Precision     int
	Scale         int
	AutoIncrement bool
	Nullable      int
	ReadOnly      bool
	Writeable     bool
	Table         string
}
