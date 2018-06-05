package handler

import (
	"fmt"
	"go/build"
	"os"
	"path"

	cp "github.com/NetSys/kappa/coordinator/pkg/cloudplatform"
)

// mergeEnv merges env into penv; returns an error in case of duplicate keys between the two maps.
func mergeEnv(penv cp.EnvT, env EnvT) error {
	for k, v := range env {
		if _, ok := penv[k]; ok {
			return fmt.Errorf("mergeEnv: duplicate key: %s", k)
		}
		penv[k] = v
	}
	return nil
}

// ensureDirectory returns nil if the path can be determined to be a directory; otherwise, returns an error.
func ensureDirectory(path string) error {
	stat, err := os.Stat(path)
	if err != nil {
		return err
	}

	if !stat.IsDir() {
		return fmt.Errorf("ensureDirectory: not a directory: %s", path)
	}

	return nil
}

// detectKappaDir tries to detect the directory where Kappa is located.
func detectKappaDir() (string, error) {
	if p := os.Getenv("KAPPAPATH"); p != "" {
		return p, nil
	}

	gopath := os.Getenv("GOPATH")
	if gopath == "" {
		gopath = build.Default.GOPATH
	}

	kappaDir := path.Join(gopath, "src/github.com/NetSys/kappa")
	if err := ensureDirectory(kappaDir); err == nil {
		return kappaDir, nil
	}

	// By default, assume the current working directory is the Kappa directory.
	return os.Getwd()
}
