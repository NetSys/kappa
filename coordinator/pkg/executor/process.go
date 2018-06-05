package executor

import (
	"fmt"
	"log"
	"time"

	"github.com/NetSys/kappa/coordinator/pkg/handler"
)

type process struct {
	Workload *workload    // the workload to which this process belongs
	Pid      handler.PidT // unique ID of process.
	Name     string       // human-readable process name.
	Target   handler.InvokeTarget
	// ^^ these fields are constant after initialization ^^

	RPC chan asyncCall // send an asynchronous coordinator call request here

	Done chan struct{} // this channel will be closed on process completion.
	Ret  ProcessResT   // return value of process; wait on Done before reading this value
}

const crashRetries = 3 // A handler crash is fatal after this number of retries.

// fatal is called by a process that encounters a fatal error.  This function may block.  The goroutine for the process
// should immediately return after calling this function.
func (p *process) fatal(seqno handler.SeqnoT, err error) {
	p.Workload.FatalErr <- fmt.Errorf("[%s, seqno=%d]\t%v", p.Name, seqno, err)
}

/// logf prepends the process name to a message and logs it.
func (p *process) logf(seqno handler.SeqnoT, format string, v ...interface{}) {
	p.logfTime(seqno, time.Now(), format, v...)
}

// logfTime is the same as logf, except that the timestamp for the log message is specified.
func (p *process) logfTime(seqno handler.SeqnoT, timestamp time.Time, format string, v ...interface{}) {
	timeMicro := timestamp.UnixNano() / 1e3
	format = fmt.Sprintf("[%s, seqno=%d, time=%d]\t", p.Name, seqno, timeMicro) + format
	log.Printf(format, v...)
}

type runState struct { // Running state of this process.
	res       handler.CCResT // Result of the last coordinator call.
	nextSeqno handler.SeqnoT // Expected sequence number of the next coordinator call (and checkpoint), initially 0.
	chkID     string         // Last checkpoint ID.
}

// update the running state after request req has been successfully processed and produced result res.
func (rs *runState) update(req *handler.Request, res handler.CCResT) {
	if req.Seqno < rs.nextSeqno {
		return
	}
	*rs = runState{res: res, nextSeqno: req.Seqno + 1, chkID: req.ChkID}
}

func (p *process) Run(startingChkID string, appEv handler.AppEvT, ccRes handler.CCResT) {
	h := p.Workload.Handler
	numCrashes := 0 // Number of consecutive crashes.
	s := runState{res: ccRes, nextSeqno: 0, chkID: startingChkID}

	// Channel to read invocation result from; nil means no invocation is going on.
	var invokeCh <-chan handler.InvokeRet
	// FIXME(zhangwen): drain RPC queue on exit.
	for {
		if invokeCh == nil { // If no outstanding invocation, start one.
			// Since no invocation is running right now, every coordinator call made so far has seqno < s.nextSeqno.
			// So any future coordinator calls will have seqno >= s.nextSeqno.
			// TODO(zhangwen): maybe (invokeID, seqno) would be easier to reason about.
			invokeCh = h.InvokeAsync(p.Pid, s.nextSeqno, s.chkID, s.res, appEv, p.Target)
			p.logf(s.nextSeqno, "begin: invocation")
		}

		var req *handler.Request
		var hr handleCCRes

		select { // "Event-driven programming" with two events: (1) invocation finished, (2) RPC came in.
		case ir := <-invokeCh:
			invokeCh = nil // Invocation completed; mark that no invocation is going on.
			p.logf(s.nextSeqno, "end: invocation")

			var err error
			req, err = ir.Req, ir.Err
			if err != nil {
				if terr, ok := err.(*handler.CrashedError); ok {
					numCrashes += 1
					if numCrashes > crashRetries {
						p.fatal(s.nextSeqno, terr)
						return
					}

					p.logf(s.nextSeqno, "crashed:\n%s\nRestarting...", terr.ErrorMessage)
					continue
				}
				numCrashes = 0

				if _, ok := err.(*handler.TimeoutError); ok {
					p.logf(s.nextSeqno, "timed out.  Restarting...")
					continue
				}

				// Otherwise, treat the error as fatal and give up.
				p.fatal(s.nextSeqno, err)
				return
			}

			if req.Blocked { // Lambda blocked on a previous async call.
				p.logf(s.nextSeqno, "blocked")
				continue
			}

			// Handler has exited normally with a coordinator call.
			if req.Seqno < s.nextSeqno {
				// Coordinator call is out-of-date, which may happen if the same call has taken place via RPC.
				// Simply ignore and restart.
				p.logf(s.nextSeqno, "outdated synchronous request (seqno=%d): %s", req.Seqno,
					prettifyRequest(req))
				continue
			}

			resCh, err := p.handleRequest(req, s.nextSeqno)
			if err != nil {
				p.fatal(s.nextSeqno, err)
				return
			}
			hr = <-resCh

		case ac := <-p.RPC: // RPC came in.
			// No matter what, respond to the goroutine that delivered the RPC so that it doesn't hang.
			req = ac.req
			if req.Seqno < s.nextSeqno {
				p.logf(s.nextSeqno, "outdated RPC (seqno=%d): %s", req.Seqno, prettifyRequest(req))
				ac.resp <- asyncResponse{nil, nil} // Just respond with something...
				continue
			}

			resCh, err := p.handleRequest(req, s.nextSeqno)
			if err != nil {
				ac.resp <- asyncResponse{nil, err}
				p.fatal(s.nextSeqno, err)
				return
			}

			// The coordinator call is determined to be valid.
			select {
			case hr = <-resCh: // Call is non-blocking.
				if hr.err != nil {
					ac.resp <- asyncResponse{nil, hr.err}
				} else {
					ac.resp <- asyncResponse{hr.res, nil}
				}
			case <-time.After(ac.timeout): // Call is blocking; let the requester know.
				ac.resp <- asyncResponse{nil, wouldBlock{}}
				hr = <-resCh
			}
		}

		// In either case, a coordinator call has been handled.  Update state accordingly.
		if hr.err != nil {
			p.fatal(s.nextSeqno, hr.err)
			return
		}
		if hr.done {
			return
		}
		s.update(req, hr.res)
	}
}
