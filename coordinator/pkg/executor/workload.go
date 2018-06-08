package executor

import (
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/NetSys/kappa/coordinator/pkg/handler"
)

const nullChkID = ""
const mainProcessName = "main"

type ProcessResT string // Result type of an entire workload.
type QidT int           // ID for queues.

// workload manages the execution of a workload on serverless.
type workload struct {
	Handler handler.Handler

	rpcTimeout time.Duration

	// FatalErr takes a fatal error observed by a process, signalling the entire workload should abort.
	FatalErr chan error

	// Use GetProcess() to get a process struct from a PID; DO NOT access the map directly.
	processMutex sync.Mutex
	// The following fields are protected by processMutex.
	nextPid handler.PidT
	pidMap  map[handler.PidT]*process

	// Use createQueue() and getQueue() to access queues between processes.
	qMutex sync.Mutex
	// The following fields are protected by qMutex.
	nextQid QidT
	qidMap  map[QidT]chan string
}

// lockedWriter synchronizes writes to an ordinary Writer.
// The executor uses a lockedWriter to make sure that multiple goroutines writing to a file concurrently play nicely.
type lockedWriter struct {
	w io.Writer
	m sync.Mutex
}

func (lw *lockedWriter) Write(p []byte) (n int, err error) {
	lw.m.Lock()
	defer lw.m.Unlock()
	return lw.w.Write(p)
}

// NewWorkload creates a workload, but doesn't start running it.
func NewWorkload(platform string, conf io.Reader, name string, deployedFiles []string, timeoutSecs int,
	rpcAddr *net.TCPAddr, logWriter io.Writer, env map[string]string, rpcTimeout time.Duration) (*workload, error) {

	// Make a copy of the environment variables passed in.
	handlerEnv := make(map[string]string)
	for k, v := range env {
		handlerEnv[k] = v
	}

	if rpcAddr != nil {
		handlerEnv["RPC_IP"] = rpcAddr.IP.String()
		handlerEnv["RPC_PORT"] = strconv.Itoa(rpcAddr.Port)
	}

	// Add a bit to account for network latency, etc.
	handlerEnv["RPC_HTTP_TIMEOUT"] = fmt.Sprintf("%g", rpcTimeout.Seconds()+1)

	if logWriter != ioutil.Discard {
		// Optimization: don't make writer take mutex if the writer discards everything.
		logWriter = &lockedWriter{w: logWriter}
	}
	h, err := handler.Create(platform, conf, name, deployedFiles, timeoutSecs, handlerEnv, logWriter)
	if err != nil {
		return nil, err
	}

	w := &workload{
		Handler:    h,
		rpcTimeout: rpcTimeout,
		FatalErr:   make(chan error),
		pidMap:     make(map[handler.PidT]*process),
		qidMap:     make(map[QidT]chan string),
	}
	return w, nil
}

// Run runs a workload, blocks until the main process completes, and returns the result.
// FIXME(zhangwen): should we wait for all processes to finish?
func (w *workload) Run(appEv handler.AppEvT) (humanReadableResult string, err error) {
	// The main process has the same name as the workload, and doesn't have a checkpoint ID (starting fresh).
	// Always run the main process on the coordinator.
	p := w.CreateProcess(mainProcessName, handler.OnCoordinator)
	go p.Run(nullChkID, appEv, nil)

	// Wait until either the main process terminates or a fatal error occurs.
	select {
	case <-p.Done:
		return resultHumanReadable(p.Ret)
	case err = <-w.FatalErr:
		return "", fmt.Errorf("FATAL: %v", err)
	}
}

func (w *workload) Finalize() {
	w.Handler.Finalize()
}

// CreateProcess creates a process and returns a pointer to its process struct.
func (w *workload) CreateProcess(name string, target handler.InvokeTarget) *process {
	w.processMutex.Lock()
	defer w.processMutex.Unlock()

	pid := w.nextPid
	w.nextPid += 1
	p := process{
		Workload: w,
		Pid:      pid,
		Name:     fmt.Sprintf("%s-%d", name, pid),
		Target:   target,

		RPC:  make(chan asyncCall, 1),
		Done: make(chan struct{}),
	}
	w.pidMap[pid] = &p

	return &p
}

// GetProcess returns the process with the given pid, or nil if no such process exists.
func (w *workload) GetProcess(pid handler.PidT) *process {
	w.processMutex.Lock()
	defer w.processMutex.Unlock()
	return w.pidMap[pid]
}

// CreateQueue creates a queue and returns its ID.
func (w *workload) CreateQueue(maxSize int) QidT {
	w.qMutex.Lock()
	defer w.qMutex.Unlock()

	qid := w.nextQid
	w.nextQid += 1
	w.qidMap[qid] = make(chan string, maxSize)
	return qid
}

// GetQueue retrieves the channel for a previously created queue.
func (w *workload) GetQueue(qid QidT) chan string {
	w.qMutex.Lock()
	defer w.qMutex.Unlock()
	return w.qidMap[qid]
}
