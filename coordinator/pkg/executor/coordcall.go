package executor

// This source file has a corresponding, easyjson-generated file named `coordcall_easyjson.go`.
// If you update this file, don't forget to run `easyjson -all coordcall.go`.

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/mailru/easyjson"

	"github.com/NetSys/kappa/coordinator/pkg/handler"
)

// ccMap maps a coordinator call name to a function that returns a corresponding coordCallImpl object.
// An alternative is to have one copy of each type of coordCallImpl object, and have json.Unmarshal() fill it in every
// time a call of that type comes in.  One nuisance with this alternative approach is that, in case a field is missing
// in the JSON, json.Unmarshal() simply leaves the corresponding struct field untouched, so the field may contain a
// value from a previous coordinator call.
// TODO(zhangwen): this looks ugly.
var ccMap = map[string]func() coordCallImpl{
	"exit":         func() coordCallImpl { return &ccExit{} },
	"checkpoint":   func() coordCallImpl { return &ccCheckpoint{} },
	"spawn":        func() coordCallImpl { return &ccSpawn{} },
	"map_spawn":    func() coordCallImpl { return &ccMapSpawn{} },
	"wait":         func() coordCallImpl { return &ccWait{} },
	"create_queue": func() coordCallImpl { return &ccCreateQueue{} },
	"enqueue":      func() coordCallImpl { return &ccEnqueue{} },
	"dequeue":      func() coordCallImpl { return &ccDequeue{} },
	"remap_store":  func() coordCallImpl { return &ccRemapStore{} },
}

// handleCCRes stores the result of handling a coordinator call.
type handleCCRes struct {
	res  handler.CCResT
	done bool
	err  error
}

type coordCallImpl interface {
	easyjson.Unmarshaler
	// run executes a coordinator call, returning a channel from which to read the result.
	// If the call blocks (e.g., waiting for a process to finish), reading from the channel would block.
	// If the call signifies process completion (i.e., "exit"), done returned as true.
	run(p *process) <-chan handleCCRes
}

// Each coordinator call struct below implements the coordCallImpl interface.

// The "exit" coordinator call sets a process' return value before ending the process.
type ccExit struct {
	Result ProcessResT `json:"result"`
}

func (cc *ccExit) run(p *process) <-chan handleCCRes {
	p.Ret = cc.Result // Ret is synchronized through the Done channel.
	close(p.Done)
	return immediateRes(nil, true, nil)
}

// The "checkpoint" coordinator call records a new checkpoint taken by the handler.
// However, since all coordinator calls involve taking a checkpoint, the "checkpoint" call is practically a "no-op".
type ccCheckpoint struct{}

func (*ccCheckpoint) run(p *process) <-chan handleCCRes {
	return immediateRes(nil, false, nil)
}

// The "spawn" coordinator call launches a new process that starts at a specified checkpoint.
type ccSpawn struct {
	Name       string         `json:"name"`
	ChildChkID string         `json:"child_chk_id"`
	FuturePids []handler.PidT `json:"future_pids"` // Processes whose return values the spawned process depends on
	AwaitPids  []handler.PidT `json:"await_pids"`  // Other processes that the spawned process waits for
	Blocking   bool           `json:"blocking"`
	Copies     int            `json:"copies"`

	OnCoordinator bool `json:"on_coordinator"`
}

func (cc *ccSpawn) run(p *process) <-chan handleCCRes {
	w := p.Workload
	target := handler.OnLambda
	if cc.OnCoordinator {
		target = handler.OnCoordinator
	}

	var children []*process
	for i := 0; i < cc.Copies; i++ {
		child := w.CreateProcess(cc.Name, target)
		children = append(children, child)
		// spawn returns immediately with the child's PID, but the child doesn't start running until all processes it
		// depends on have finished.
		go func() {
			// Wait for preceding processes to complete.
			// AwaitPids refers to the processes that must finish before the child process can start.  However, the
			// child process doesn't need their return values.
			for _, pid := range cc.AwaitPids {
				<-w.GetProcess(pid).Done
			}

			// FuturePids refers to the processes whose return values are needed by the child process.  In addition to
			// waiting for them to finish, we also gather their return values and pass them to the child.
			predRes := make(map[handler.PidT]ProcessResT)
			for _, pid := range cc.FuturePids {
				p := w.GetProcess(pid)
				<-p.Done
				predRes[pid] = p.Ret
			}

			child.Run(cc.ChildChkID, nil, predRes)
		}()
	}

	if cc.Blocking {
		ch := make(chan handleCCRes, 1)
		go func() {
			var results []ProcessResT
			for _, child := range children {
				<-child.Done
				results = append(results, child.Ret)
			}
			res := map[string]interface{}{"rets": results}
			ch <- handleCCRes{res, false, nil}
		}()
		return ch
	} else {
		var pids []handler.PidT
		for _, child := range children {
			pids = append(pids, child.Pid)
		}
		return immediateRes(map[string]interface{}{"child_pids": pids}, false, nil)
	}
}

// The "map_spawn" coordinator call launches new processes to execute a `map`.
type ccMapSpawn struct {
	Name       string         `json:"name"`
	ChildChkID string         `json:"child_chk_id"`
	FuturePids []handler.PidT `json:"future_pids"` // Processes whose return values the spawned process depends on
	Elems      []string       `json:"elems"`
	AwaitPids  []handler.PidT `json:"await_pids"` // Other processes that the spawned process waits for

	OnCoordinator bool `json:"on_coordinator"`
}

func (cc *ccMapSpawn) run(p *process) <-chan handleCCRes {
	w := p.Workload
	target := handler.OnLambda
	if cc.OnCoordinator {
		target = handler.OnCoordinator
	}

	var children []*process
	var childrenPids []handler.PidT
	numChildren := len(cc.Elems)
	for i := 0; i < numChildren; i++ {
		child := w.CreateProcess(cc.Name, target)
		children = append(children, child)
		childrenPids = append(childrenPids, child.Pid)
	}

	go func() {
		// Wait for preceding processes to complete.
		predRes := make(map[handler.PidT]ProcessResT)
		for _, pid := range cc.FuturePids {
			p := w.GetProcess(pid)
			<-p.Done
			predRes[pid] = p.Ret
		}

		// AwaitPids refers to the processes that must finish before the child process can start.  However, the
		// child process doesn't need their return values.
		for _, pid := range cc.AwaitPids {
			<-w.GetProcess(pid).Done
		}

		// map_spawn returns immediately with the children's PIDs, but the children don't start running until all
		// processes they depend on have finished.
		for i := 0; i < numChildren; i++ {
			go func(child *process, elem string) {
				child.Run(cc.ChildChkID, nil, []interface{}{predRes, elem})
			}(children[i], cc.Elems[i])
		}
	}()
	return immediateRes(childrenPids, false, nil)
}

// The "wait" coordinator call waits until a process finishes, then returns the process' result.
type ccWait struct {
	Pid handler.PidT `json:"pid"`
}

func (cc *ccWait) run(p *process) <-chan handleCCRes {
	w := p.Workload
	pWait := w.GetProcess(cc.Pid) // Process to wait for.
	if pWait == nil {
		// Invalid pid: treat as fatal.  In the runtime library, the "wait" call can only be issued by calling "wait()"
		// on a "Future" object, so there's no excuse for the lambda to get the pid wrong.
		err := fmt.Errorf("wait: no process exists with pid %d", cc.Pid)
		return immediateErr(err)
	}

	ch := make(chan handleCCRes, 1)
	select {
	case <-pWait.Done: // In case the process to wait for has completed.
		ch <- handleCCRes{pWait.Ret, false, nil}
	default:
		go func() {
			<-pWait.Done
			ch <- handleCCRes{pWait.Ret, false, nil}
		}()
	}

	return ch
}

// The "create_queue" coordinator call creates a queue, which allows communication between processes.
type ccCreateQueue struct {
	MaxSize int `json:"max_size"`
	Copies  int `json:"copies"` // If -1, creates one queue.
}

func (cc *ccCreateQueue) run(p *process) <-chan handleCCRes {
	w := p.Workload

	if cc.Copies == -1 {
		qid := w.CreateQueue(cc.MaxSize)
		return immediateRes(qid, false, nil)
	} else {
		qids := make([]QidT, 0) // Be careful not to make it nil.
		for i := 0; i < cc.Copies; i++ {
			qids = append(qids, w.CreateQueue(cc.MaxSize))
		}
		return immediateRes(qids, false, nil)
	}
}

// The "enqueue" coordinator call puts an object into a queue; blocks if queue is full.
type ccEnqueue struct {
	Qid  QidT     `json:"qid"`
	Objs []string `json:"objs"` // serialization of the objects; simply treat as blob
}

func (cc *ccEnqueue) run(p *process) <-chan handleCCRes {
	q := p.Workload.GetQueue(cc.Qid)
	if q == nil {
		err := fmt.Errorf("enqueue: no queue exists with qid %d", cc.Qid)
		return immediateErr(err)
	}

	ch := make(chan handleCCRes, 1)
	go func() {
		for _, obj := range cc.Objs {
			q <- obj
		}
		ch <- handleCCRes{nil, false, nil}
	}()
	return ch
}

// The "dequeue" coordinator call retrieves an object from a queue; blocks if queue is empty.
type ccDequeue struct {
	Qid QidT `json:"qid"`
}

func (cc *ccDequeue) run(p *process) <-chan handleCCRes {
	q := p.Workload.GetQueue(cc.Qid)
	if q == nil {
		err := fmt.Errorf("dequeue: no queue exists with qid %d", cc.Qid)
		return immediateErr(err)
	}

	ch := make(chan handleCCRes, 1)
	select {
	case obj := <-q:
		ch <- handleCCRes{obj, false, nil}
	default:
		go func() {
			ch <- handleCCRes{<-q, false, nil}
		}()
	}
	return ch
}

func immediateRes(res handler.CCResT, done bool, err error) <-chan handleCCRes {
	ch := make(chan handleCCRes, 1)
	ch <- handleCCRes{res, done, err}
	return ch
}

// The "remap_store" coordinator call receives a mapping of temporary S3 objects and renames
// them to their true name.
type ccRemapStore struct {
	TmpBucket string `json:"tmp_bucket"`
	TmpKey    string `json:"tmp_key"`
	Bucket    string `json:"bucket"`
	Key       string `json:"key"`
}

func (cc *ccRemapStore) run(p *process) <-chan handleCCRes {
	ch := make(chan handleCCRes, 1)
	go func() {
		err := Rename(cc.TmpBucket, cc.TmpKey, cc.Bucket, cc.Key)
		ch <- handleCCRes{nil, false, err}
	}()
	return ch
}

func immediateErr(err error) <-chan handleCCRes {
	return immediateRes(nil, true, err)
}

func parseCoordCall(cc *handler.CoordCall) (coordCallImpl, error) {
	op, pbs := cc.Op, cc.Params
	ccf, ok := ccMap[op]
	if !ok {
		err := fmt.Errorf("handleCoordCall: unrecognized: %s(%v)", op, string(pbs))
		return nil, err
	}

	// Unfortunately, json.Unmarshal() doesn't report an error if a field in cc is absent in the JSON; the missing
	// field is simply untouched in the struct (in our case, having the default value for its type).
	// TODO(zhangwen): stricter parameter checking?
	cci := ccf()
	if err := easyjson.Unmarshal(pbs, cci); err != nil {
		return nil, err
	}

	return cci, nil
}

// handleCoordCall runs a coordinator call.
func (p *process) handleCoordCall(cc *handler.CoordCall) (<-chan handleCCRes, error) {
	cci, err := parseCoordCall(cc)
	if err != nil {
		return nil, err
	}

	p.logf(cc.Seqno, prettifyCoordCall(cc))
	return cci.run(p), nil
}

// handleRequest runs coordinator calls in a request.
func (p *process) handleRequest(req *handler.Request, processSeqno handler.SeqnoT) (<-chan handleCCRes, error) {
	// Filter and parse coordinator calls.
	var parsedCalls []coordCallImpl
	for _, cc := range req.Calls {
		if cc.Seqno < processSeqno {
			p.logf(processSeqno, "outdated RPC (seqno=%d): %s", cc.Seqno, prettifyCoordCall(&cc))
			continue
		}

		parsed, err := parseCoordCall(&cc)
		if err != nil {
			return nil, err
		}

		p.logf(processSeqno, "[seqno=%d]  %s", cc.Seqno, prettifyCoordCall(&cc))
		parsedCalls = append(parsedCalls, parsed)
	}

	if len(parsedCalls) == 0 {
		p.logf(processSeqno, "OOPS: entire request is outdated (seqno=%d)", req.Seqno)
		return immediateRes(nil, false, nil), nil
	}

	ch := make(chan handleCCRes, 1)
	go func() {
		var res handleCCRes
		for _, cci := range parsedCalls {
			res = <-cci.run(p)
			if res.done || res.err != nil {
				break
			}
		}
		ch <- res
	}()
	return ch, nil
}

// truncateString returns a "preview" of a string of at most some length (ellipses at the end if too long).
// Taken from https://play.golang.org/p/EzvhWMljku.
func truncateString(str string, num int) string {
	bnoden := str
	if len(str) > num {
		if num > 3 {
			num -= 3
		}
		bnoden = str[0:num] + "..."
	}
	return bnoden
}

// prettifyCoordCall returns a pretty string representation of a coordinator call.
func prettifyCoordCall(cc *handler.CoordCall) string {
	var params map[string]json.RawMessage

	shouldPrettify := true
	if len(cc.Params) > 500 {
		// Params too long; don't prettify.
		shouldPrettify = false
	} else if err := json.Unmarshal(cc.Params, &params); err != nil {
		shouldPrettify = false
	}

	if !shouldPrettify {
		// Fall back to less pretty format.
		return fmt.Sprintf("%s(%s)", cc.Op, truncateString(string(cc.Params), 500))
	}

	var components []string
	for k, v := range params {
		v := string(v)

		// Special case "result" param of exit(): attempt to print human-readable version of process result.
		// Disabled due to performance degradation.
		//if k == "result" && cc.Op == "exit" {
		//	if r, err := resultHumanReadable(ProcessResT(v)); err == nil {
		//		v = r
		//	}
		//}

		components = append(components, fmt.Sprintf("%s=%s", k, truncateString(v, 100)))
	}
	return fmt.Sprintf("%s(%s)", cc.Op, strings.Join(components, ", "))
}

// prettifyRequest returns a pretty string representation of a request to the coordinator.
func prettifyRequest(req *handler.Request) string {
	var components []string
	for _, call := range req.Calls {
		components = append(components, prettifyCoordCall(&call))
	}
	return strings.Join(components, ", ")
}
