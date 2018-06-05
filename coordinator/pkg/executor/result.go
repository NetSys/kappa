package executor

import (
	"bytes"
	"fmt"
	"os/exec"
)

const (
	pythonInterpreter = "python3"
	decodeProgram     = `
from base64 import b64decode
import pickle
import sys

obj = pickle.loads(b64decode(sys.argv[1]))
sys.stdout.write(repr(obj))
`
)

// resultHumanReadable turns a process' result into a human-readable string so that it can be printed out.
// The conversion fails if the result contains references to non-Python-builtin objects.
// Requires the Python 3.6 interpreter to be in PATH.
func resultHumanReadable(result ProcessResT) (string, error) {
	cmd := exec.Command(pythonInterpreter, "-c", decodeProgram, string(result))

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	ob, err := cmd.Output()
	if err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			// The Python interpreter exited with exit code != 0.
			return "", fmt.Errorf("result decoding failed:\n%s", stderr.String())
		}

		return "", err
	}

	return string(ob), nil
}
