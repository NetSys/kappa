package executor

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/NetSys/kappa/coordinator/pkg/handler"
)

// wouldBlock is an error indicating that a coordinator call would block.
type wouldBlock struct{}

func (wouldBlock) Error() string { return "Coordinator call would block." }

// asyncCall indicates a coordinator call made through RPC.
type asyncCall struct {
	req     *handler.Request
	timeout time.Duration // RPC only waits for this long before returning.
	resp    chan<- asyncResponse
}

// asyncResponse represents the result of an asynchronous coordinator call.
type asyncResponse struct {
	res handler.CCResT // Return value of coordinator call.

	err error // If non-nil, the call hasn't been processed successfully.
	// Specifically, if the call would block, err is set to a wouldBlock object.
}

func (w *workload) ServeHTTP(wr http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" { // A coordinator call can be non-idempotent.
		rpcErrorf(wr, nil, nil, nil, http.StatusMethodNotAllowed,
			"an RPC request must have method POST, not %s", r.Method)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		// TODO(zhangwen): is the HTTP status code appropriate?
		rpcErrorf(wr, nil, nil, nil, http.StatusBadRequest, "read body: %v", err)
		return
	}

	startTime := time.Now()

	req, err := handler.ParseRequest(body)
	if err != nil { // Request is malformed.
		rpcErrorf(wr, body, nil, nil, http.StatusBadRequest, "parse body: %v", err)
		return
	}

	p := w.GetProcess(req.Pid)
	if p == nil {
		rpcErrorf(wr, body, req, nil, http.StatusBadRequest, "process not found: %d", req.Pid)
		return
	}

	respChan := make(chan asyncResponse, 1)
	// TODO(zhangwen): triage RPCs by whether the lambda must wait on the call? (opportunistic vs. mandatory checkpoint)
	p.RPC <- asyncCall{req, w.rpcTimeout, respChan}
	resp := <-respChan

	if resp.err != nil {
		if _, ok := (resp.err).(wouldBlock); ok {
			wr.WriteHeader(http.StatusAccepted)
			p.logf(req.Seqno, "responding: would block")
			return
		}

		rpcErrorf(wr, body, req, p, http.StatusBadRequest, "coordinator call failed: %v", err)
		return
	}

	ob, err := handler.EncodeCoordCallResult(resp.res)
	if err != nil {
		rpcErrorf(wr, body, req, p, http.StatusInternalServerError, "encoding result failed: %v", err)
		return
	}

	// This is how long the coordinator spent processing the RPC (after reading the request, before writing the response).
	p.logfTime(req.Seqno, startTime, "begin: coordinator rpc")
	p.logf(req.Seqno, "end: coordinator rpc")

	wr.Write(ob)
}

// rpcErrorf returns an error to an RPC request.  The caller should quit the HTTP handler after calling this function.
// Arguments body, cc, and p are optional and for logging only.
func rpcErrorf(wr http.ResponseWriter, body []byte, req *handler.Request, p *process, code int,
	format string, a ...interface{}) {

	msg := fmt.Sprintf(format, a...)

	ccStr := ""
	if req != nil {
		ccStr = fmt.Sprintf(" [%s]", prettifyRequest(req))
	} else if body != nil {
		ccStr = fmt.Sprintf(" [%s]", body)
	}

	logMsg := fmt.Sprintf("[HTTP %d]%s %s", code, ccStr, msg)
	if p != nil {
		p.logf(-1, logMsg)
	} else {
		log.Println(logMsg)
	}

	http.Error(wr, msg, code)
}
