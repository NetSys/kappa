// The cloudplatform package abstracts over the functionality provided by a cloud platform, e.g., AWS.
package cloudplatform

import (
	"fmt"
)

type EnvT map[string]string // Environment variables passed to lambdas.

type Handler interface {
	Name() string
	TimeoutSecs() int
	Invoke(payload []byte) ([]byte, error)

	// Finalize cleans up allocated resources.  Any error encountered during cleanup is logged but ignored.
	Finalize()
}

type HandlerTimeoutError struct {
	Handler Handler
}

func (e *HandlerTimeoutError) Error() string {
	h := e.Handler
	return fmt.Sprintf("handler \"%s\" exceeded the time limit of %d sec", h.Name(), h.TimeoutSecs())
}

type HandlerCrashedError struct {
	Handler Handler
	Message string
}

func (e *HandlerCrashedError) Error() string {
	return fmt.Sprintf("handler \"%s\" crashed with error message:\n%s", e.Handler.Name(), e.Message)
}
