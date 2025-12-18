# Mapepire Golang Client SDK
`mapepire-go` is a golang client implementation for [Mapepire](https://github.com/Mapepire-IBMi) that provides a simple interface for connecting to an IBM i server and running SQL queries. The client is designed to work with the [Mapepire Server Component](https://github.com/Mapepire-IBMi/mapepire-server).

## Setup
### Install with go get
```bash
go get github.com/Mapepire-IBMi/mapepire-go
```
### Server Component Setup
To use mapepire-go, you will need to have the Mapepire Server Component running on your IBM i server. Follow these instructions to set up the server component: [Mapepire Server Installation](https://mapepire-ibmi.github.io/guides/sysadmin/)

## Example Usage
The following go program initializes a `DaemonServer` object that will be used to connect with the Server Component. A single `SQLJob` object is created to facilitate this connection from the client side. A query object is then initialized with a simple `SELECT` query which is finally executed.

```go
package main

import (
	"log"

	mapepire "github.com/Mapepire-IBMi/mapepire-go"
)

func main() {
	// Initialize credentials
	creds := mapepire.DaemonServer{
		Host:               "HOST",
		User:               "USER",
		Password:           "PASS",
		Port:               "8076",
		IgnoreUnauthorized: true,
	}

	// Establish connection
	job := mapepire.NewSQLJob("ID")
	job.Connect(creds)

	// Initialize and execute query
	query, _ := job.Query("SELECT * FROM employee")
	result, _ := query.Execute()

	// Access data and information
	log.Println(result.Success)
	log.Println(result.Metadata)
	log.Println(result.Data)
}
```
The result is a JSON object that contains the metadata and data from the query, which will be stored in the `ServerResponse` object. Here are the different fields returned:
* `id` field contains the query ID
* `hasResults` field indicates whether the query returned any results
* `success` field indicates whether the query was successful
* `metadata` field contains information about the columns returned by the query
* `data` field contains the results of the query
* `isDone` field indicates whether the query has finished executing
* `updateCount` field indicates the number of rows updated by the query (-1 if the query did not update any rows)
* `error` field contains errors, if any.

### Query Options
In the `QueryOptions` object are some additional options for the query execution:
```go
options := mapepire.QueryOptions{
		Rows:        5,
		Parameters:  [][]any{},
		TerseResult: false,
		IsCLcommand: false,
	}
query, _ := job.QueryWithOptions("SELECT * FROM employee", options)
```

### Prepared Statements
Statements can be easily prepared and executed with parameters:
```go
options := mapepire.QueryOptions{Parameters: [][]any{{"Max", "Olly"}}}
query, _ := job.QueryWithOptions("SELECT * FROM employee WHERE (name = ? OR name = ?)", options)
result, _ := query.Execute()
```
There can also be added more to the batch:
```go
options := mapepire.QueryOptions{Parameters: [][]any{{"1264", "Mark"}, {"1265", "Tom"}}}
query, _ := job.QueryWithOptions("INSERT INTO employee (ID, NAME) VALUES (?, ?)", options)
result, _ := query.Execute()
```
### CL Commands
CL commands can be easily run by setting the `IsCLcommand` option to be `true` on the `QueryOptions` object or by directly using the `CLCommand` function on a job.
```go
query, _ := job.ClCommand("CRTLIB LIB(MYLIB1) TEXT('My cool library')")
result, _ := query.Execute()
```
### Pooling
To streamline the creation and reuse of `SQLJob` objects, your application should establish a connection pool on startup. This is recommended as connection pools significantly improve performance as it reduces the number of connection object that are created.

A pool can be initialized with a given starting size and maximum size. Once initialized, the pool provides APIs to access a free job or to send a query directly to a free job.
```go
// Create a pool with a max size of 5, starting size of 3 and maximum wait time of 1 second
options := mapepire.PoolOptions{Creds: creds, MaxSize: 5, StartingSize: 3, MaxWaitTime: 1}
pool, _ := mapepire.NewPool(options)

// Initialize and execute query
resultChan, _ := pool.ExecuteSQL("SELECT * FROM employee")
result := <-resultChan

// Close pool and jobs
pool.Close()
```
### JDBC Options
When specifying the credentials in the `DaemonServer` object, JDBC options can be defined in the `Properties` field. For a full list of all options, check out the documentation [here](https://www.ibm.com/docs/en/i/7.4?topic=jdbc-toolbox-java-properties).
```go
creds := mapepire.DaemonServer{
		Host:               "HOST",
		User:               "USER",
		Password:           "PASS",
		Port:               "8076",
		IgnoreUnauthorized: true,
		Properties:         "prompt=false;translate binary=true;naming=system"
	}
```
### Tracing Options
Tracing can be achieved by setting the configuration level and destination of the same tracer.

* `Tracelevel` (`jtopentracelevel`): OFF, ON, ERRORS, DATASTREAM, INPUT_AND_ERRORS
* `Tracedest` (`jtopentracedest`): file, in_mem
```go
// Establish connection
job := mapepire.NewSQLJob("ID")
job.Connect(creds)

// Set Trace configuration
options := mapepire.TraceOptions{Tracelevel: "ERRORS", Tracedest: "file"}
job.SetTraceConfig(options)

// Receive Trace data
job.GetTraceData()
```

## Secure Connections
By default, Mapepire will always try to connect securely. A majority of the time, servers are using their own self-signed certificate that is not signed by a recognized CA (Certificate Authority). There is currently only one option with the Golang client.

### Allow all Certificates
On the `DaemonServer` object, the `IgnoreUnauthorized` option can be set to `true` which will allow either self-signed certificates or certificates from a CA.

### Validate Self-signed Certificates
> [!WARNING]
> Validation of self-signed certificates is currently not supported. 
