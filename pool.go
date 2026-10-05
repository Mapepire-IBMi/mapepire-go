package mapepire

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Connection pool
type JobPool struct {
	jobPool chan *SQLJob   // A channel of SQLJobs managed by the pool
	options PoolOptions    // Represents the options for configuring a connection pool
	counter *atomic.Uint32 // Atomic counter
	closed  atomic.Bool    // Set once Close() has drained the channel
	mu      sync.Mutex     // Serializes GetJob/AddJob against Close's drain
}

// Represents the options for configuring a connection pool
type PoolOptions struct {
	Creds        DaemonServer // Credentials to connect to the server
	MaxWaitTime  int          // Max time to wait for a job (in seconds)
	MaxSize      int          // Pool max size
	StartingSize int          // Pool starting count
}

// Create a new pool object
func NewPool(options PoolOptions) (*JobPool, error) {
	if options.MaxSize <= 0 {
		return nil, fmt.Errorf("max size must be greater than 0")
	} else if options.StartingSize <= 0 {
		return nil, fmt.Errorf("starting size must be greater than 0")
	} else if options.MaxSize < options.StartingSize {
		return nil, fmt.Errorf("max size must be greater than or equal to starting size")
	}

	if options.Creds.Host == "" || options.Creds.Password == "" {
		return nil, fmt.Errorf("hostname and password required")
	}

	jobChannel := make(chan *SQLJob, options.MaxSize)
	wg := new(sync.WaitGroup)
	wg.Add(options.StartingSize)

	for i := 0; i < options.StartingSize; i++ {
		go func(id string) {
			jobChannel <- NewSQLJob("Pooljob " + id)
			wg.Done()
		}(strconv.Itoa(i + 1))
	}
	wg.Wait()
	var counter atomic.Uint32
	counter.Add(uint32(options.StartingSize))

	pool := &JobPool{
		jobPool: jobChannel,
		counter: &counter,
		options: options,
	}
	return pool, nil
}

// Receive a job from the pool
func (jp *JobPool) GetJob() (s *SQLJob, err error) {
	if jp.closed.Load() {
		return nil, fmt.Errorf("pool is closed")
	}
	select {
	case s := <-jp.jobPool:
		if s.connection == nil {
			if err := s.Connect(jp.options.Creds); err != nil {
				_ = s.Close()
				jp.counter.Add(^uint32(0))
				return nil, err
			}
		}
		return s, nil
	case <-time.After(time.Duration(jp.options.MaxWaitTime) * time.Second):
		if jp.closed.Load() {
			return nil, fmt.Errorf("pool is closed")
		}
		if jp.GetJobCount() < jp.options.MaxSize {
			if jp.closed.Load() {
				return nil, fmt.Errorf("pool is closed")
			}
			return jp.newPoolJob()
		}
		return nil, fmt.Errorf("exceeded time limit")
	}
}

func (jp *JobPool) newPoolJob() (*SQLJob, error) {
	jobCount := fmt.Sprint(jp.counter.Add(1))

	id := "PoolJob " + jobCount
	job := NewSQLJob(id)

	err := job.Connect(jp.options.Creds)
	if err != nil {
		_ = job.Close()
		jp.counter.Add(^uint32(0))
		return nil, err
	}
	return job, nil
}

// Add a job back to the pool
func (jp *JobPool) AddJob(s *SQLJob) error {
	jp.mu.Lock()
	defer jp.mu.Unlock()
	if jp.closed.Load() {
		_ = s.Close()
		jp.counter.Add(^uint32(0))
		return fmt.Errorf("pool is closed")
	}
	if len(jp.jobPool) >= jp.options.MaxSize {
		_ = s.Close()
		jp.counter.Add(^uint32(0))
		return fmt.Errorf("not enough space in the pool")
	}
	select {
	case jp.jobPool <- s:
		return nil
	default:
		_ = s.Close()
		jp.counter.Add(^uint32(0))
		return fmt.Errorf("not enough space in the pool")
	}
}

// Execute a SQL query with a job from the pool
func (jp *JobPool) ExecuteSQL(sql string) (*ServerResponse, error) {
	return jp.ExecuteSQLWithOptions(sql, QueryOptions{})
}

// Execute a SQL query with options, using a job from the pool.
// A non-zero QueryOptions.Timeout sets the job's websocket read deadline for the query.
func (jp *JobPool) ExecuteSQLWithOptions(command string, queryops QueryOptions) (*ServerResponse, error) {

	job, err := jp.GetJob()
	if err != nil {
		return nil, err
	}
	if queryops.Timeout > 0 {
		job.SetReadTimeout(queryops.Timeout)
	}

	query, err := job.QueryWithOptions(command, queryops)
	if err != nil {
		_ = jp.AddJob(job)
		return nil, err
	}

	resp, executeErr := query.Execute()

	var wsErr *WebsocketError
	var djErr *DeadJobError
	if errors.As(executeErr, &wsErr) || errors.As(executeErr, &djErr) {
		_ = job.Close()
	}

	err = jp.AddJob(job)
	err = errors.Join(executeErr, err)
	if err != nil {
		return resp, err
	}

	return resp, nil
}

// Receive the count of jobs that have been initialized by the pool
func (jp *JobPool) GetJobCount() int {
	return int(jp.counter.Load())
}

// IdleJobs returns the jobs currently sitting idle in the pool
func (jp *JobPool) IdleJobs() []*SQLJob {
	if !jp.mu.TryLock() {
		return nil
	}
	defer jp.mu.Unlock()
	jobs := make([]*SQLJob, 0, len(jp.jobPool))
	for {
		select {
		case j := <-jp.jobPool:
			jobs = append(jobs, j)
		default:
			for _, j := range jobs {
				jp.jobPool <- j
			}
			return jobs
		}
	}
}

// IdleCount returns the number of idle jobs without locking.
func (jp *JobPool) IdleCount() int { return len(jp.jobPool) }

// Closes the pool and its jobs
func (jp *JobPool) Close() {
	if !jp.closed.CompareAndSwap(false, true) {
		return
	}
	jp.mu.Lock()
	defer jp.mu.Unlock()
	for {
		select {
		case job := <-jp.jobPool:
			job.Close()
			jp.counter.Add(^uint32(0))
			continue
		default:
			return
		}
	}
}
