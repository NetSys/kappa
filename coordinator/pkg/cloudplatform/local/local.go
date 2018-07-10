// The local package exposes a local, simulated serverless platform.
package local

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"syscall"

	cp "github.com/NetSys/kappa/coordinator/pkg/cloudplatform"
	"github.com/NetSys/kappa/coordinator/pkg/util"
)

const (
	invokerInterpreter = "python3"
	invokerRelPath     = "compiler/invoker.py" // relative to Kappa path.
)

// Correspond to constants in invoker.
const (
	timeoutExitCode     = 42
	uncaughtExcExitCode = 43
)

type handler struct {
	invokerPath string
	name        string
	deployDir   string
	env         cp.EnvT
	timeoutSecs int
	logWriter   io.Writer
}

// CreateHandler creates a handler that runs locally.
// Pass in timeoutSecs=0 for no time limit.
func CreateHandler(kappaDir string, name string, deployedFiles []string, env cp.EnvT, timeoutSecs int,
	logWriter io.Writer) (*handler, error) {

	// Deploy package to a temporary local directory.
	deployDir, err := ioutil.TempDir("", "")
	if err != nil {
		return nil, fmt.Errorf("cloudplatform.local.CreateHandler: %v", err)
	}

	invokerPath := path.Join(kappaDir, invokerRelPath)
	args := []string{invokerPath}
	deployedFiles = util.ParseFilterPathByPlatform("local", deployedFiles)
	args = append(args, deployedFiles...)
	args = append(args, "--deploy", deployDir)

	cmd := exec.Command(invokerInterpreter, args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("cloudplatform.local.CreateHandler: deploy failed: %v (stderr: %s)", err,
			stderr.String())
	}
	log.Println("cloudplatform.local.CreateHandler: package deployed locally at:", deployDir)

	env["WHERE"] = "coordinator"

	return &handler{
		invokerPath: invokerPath,
		name:        name,
		deployDir:   deployDir,
		env:         env,
		timeoutSecs: timeoutSecs,
		logWriter:   logWriter,
	}, nil
}

func (h *handler) Name() string {
	return h.name
}

func (h *handler) TimeoutSecs() int {
	return h.timeoutSecs
}

func (h *handler) Invoke(p []byte) ([]byte, error) {
	args := []string{h.invokerPath, h.deployDir}
	args = append(args, "--event", string(p))
	if h.timeoutSecs > 0 {
		args = append(args, "--timeout-secs", strconv.Itoa(h.timeoutSecs))
	}
	cmd := exec.Command(invokerInterpreter, args...)
	log.Println("cloudplatform.local.Invoke:", invokerInterpreter, strings.Join(args, " "))

	// Construct list of environment variables.
	el := os.Environ()
	for k, v := range h.env {
		el = append(el, fmt.Sprintf("%s=%s", k, v))
	}
	cmd.Env = el

	var stderr bytes.Buffer // Keep stderr in memory to return in case of error.
	cmd.Stderr = io.MultiWriter(&stderr, h.logWriter)

	ob, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			// The invoker exited with exit code != 0.
			// Now, determine the exit code.  If we understand the code, return a more specific error.
			// Adapted from: https://stackoverflow.com/questions/10385551/get-exit-code-go.
			// Works on both Unix and Windows.
			if status, ok := exitErr.Sys().(syscall.WaitStatus); ok {
				code := status.ExitStatus()
				switch code {
				case timeoutExitCode:
					err = &cp.HandlerTimeoutError{Handler: h}
				case uncaughtExcExitCode:
					err = &cp.HandlerCrashedError{Handler: h, Message: stderr.String()}
				default:
					// The invoker has crashed for some other reason.
					err = fmt.Errorf("invoker script exited with status %d:\n%s", code, stderr.String())
				}
			}
		}
		return nil, err
	}

	return ob, nil
}

func (*handler) Finalize() {}
