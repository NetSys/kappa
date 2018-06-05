// The handler package allows managing Kappa handlers, an abstraction on top of serverless handlers.
package handler

import (
	"encoding/json"
	"fmt"
	"io"

	cp "github.com/NetSys/kappa/coordinator/pkg/cloudplatform"
)

type EnvT map[string]string
type InvokeRet struct {
	Req *Request
	Err error
}

type InvokeTarget int

const (
	OnCoordinator InvokeTarget = iota
	OnLambda
)

type Handler interface {
	// Returns a channel to read the invocation result from; the channel is closed after the result is written to it.
	InvokeAsync(pid PidT, seqno SeqnoT, chkID string, ccRes CCResT, appEv AppEvT, target InvokeTarget) <-chan InvokeRet

	// Finalize cleans up allocated resources.  Any error encountered during cleanup is logged but ignored.
	Finalize()
}

// TimeoutError is returned when a handler has been killed by timeout.
type TimeoutError struct{}

// CrashedError is returned when a handler has crashed (e.g., due to an uncaught exception).
type CrashedError struct {
	ErrorMessage string
}

func (*TimeoutError) Error() string {
	return "handler timed out"
}

func (e *CrashedError) Error() string {
	return fmt.Sprintf("handler crashed: %s", e.ErrorMessage)
}

func Create(platform string, conf io.Reader, name string, deployedFiles []string, timeoutSecs int,
	env EnvT, logWriter io.Writer) (Handler, error) {

	switch platform {
	case "local":
		return createLocal(conf, name, deployedFiles, timeoutSecs, env, logWriter)
	case "aws":
		return createAWS(conf, name, deployedFiles, timeoutSecs, env, logWriter)
	default:
		return nil, fmt.Errorf("unsupported platform: %s", platform)
	}
}

// Implementation below.

type common struct {
	handlers map[InvokeTarget]cp.Handler
}

func (hc *common) invoke(pid PidT, seqno SeqnoT, chkID string, ccRes CCResT, appEv AppEvT,
	target InvokeTarget) (*Request, error) {

	payload, err := json.Marshal(event{
		// Information for the Kappa runtime.
		Pid:             pid,
		Seqno:           seqno,
		ChkID:           chkID,
		CoordCallResult: ccRes,
		// Event for application code.
		AppEvent: appEv,
	})
	if err != nil {
		return nil, err
	}

	ob, err := hc.handlers[target].Invoke(payload)
	if err != nil {
		// Return more specific errors if possible.
		switch et := err.(type) {
		case *cp.HandlerTimeoutError:
			return nil, &TimeoutError{}
		case *cp.HandlerCrashedError:
			return nil, &CrashedError{ErrorMessage: et.Message}
		}
		return nil, err
	}

	var decoded string
	if err := json.Unmarshal(ob, &decoded); err != nil {
		return nil, fmt.Errorf("handler.invoke: %v", err)
	}

	return ParseRequest([]byte(decoded))
}

func (hc *common) InvokeAsync(pid PidT, seqno SeqnoT, chkID string, ccRes CCResT, appEv AppEvT,
	target InvokeTarget) <-chan InvokeRet {

	ch := make(chan InvokeRet, 1)
	go func() {
		cc, err := hc.invoke(pid, seqno, chkID, ccRes, appEv, target)
		ch <- InvokeRet{cc, err}
		close(ch)
	}()
	return ch
}

func (hc *common) finalizePlatform() {
	for _, h := range hc.handlers {
		h.Finalize()
	}
}
