package handler

import (
	"io"
	"io/ioutil"
	"log"
	"os"

	"gopkg.in/yaml.v2"

	cp "github.com/NetSys/kappa/coordinator/pkg/cloudplatform"
	"github.com/NetSys/kappa/coordinator/pkg/cloudplatform/local"
)

// localHandler exposes a handler that runs on the local machine.
type localHandler struct {
	common
	tempCheckpointDir string // If not an empty string, the directory is deleted at cleanup time.
}

func (lh *localHandler) Finalize() {
	lh.finalizePlatform()

	if lh.tempCheckpointDir == "" {
		return
	}

	if err := os.RemoveAll(lh.tempCheckpointDir); err != nil {
		log.Printf("localHandler.Finalize: %v", err)
	}
	log.Println("localHandler.Finalize: temporary directory removed:", lh.tempCheckpointDir)
}

// createLocal creates an Kappa handler that runs lambdas locally.
// logWriter.Write MAY be called concurrently; the Writer is responsible for preventing undesired interleaving.
func createLocal(conf io.Reader, name string, deployedFiles []string, timeoutSecs int, env EnvT, logWriter io.Writer) (
	*localHandler, error) {

	var config struct {
		CheckpointDir string `yaml:"checkpoint_dir"`
	}

	if conf != nil {
		b, err := ioutil.ReadAll(conf)
		if err != nil {
			return nil, err
		}
		if err = yaml.UnmarshalStrict(b, &config); err != nil {
			return nil, err
		}
	}

	kappaDir, err := detectKappaDir()
	if err != nil {
		return nil, err
	}

	checkpointDir, tempCheckpointDir := config.CheckpointDir, ""
	if checkpointDir == "" { // Make a temporary checkpoint directory.
		tempCheckpointDir, err = ioutil.TempDir("", "chk")
		if err != nil {
			return nil, err
		}
		log.Printf("handler.createLocal: temporary checkpoint directory created: %s", tempCheckpointDir)
		checkpointDir = tempCheckpointDir
	} else if err = ensureDirectory(checkpointDir); err != nil {
		return nil, err
	}

	penv := cp.EnvT{
		"PLATFORM":       "local",
		"CHECKPOINT_DIR": checkpointDir,
	}
	if err = mergeEnv(penv, env); err != nil {
		return nil, err
	}

	onLambdaHandler, err := local.CreateHandler(kappaDir, name, deployedFiles, penv, timeoutSecs, logWriter)
	if err != nil {
		return nil, err
	}
	onCoordinatorHandler, err := local.CreateHandler(kappaDir, name, deployedFiles, penv, 0, logWriter)
	if err != nil {
		return nil, err
	}
	h := &localHandler{
		common: common{map[InvokeTarget]cp.Handler{
			OnLambda: onLambdaHandler, OnCoordinator: onCoordinatorHandler}},
		tempCheckpointDir: tempCheckpointDir,
	}
	return h, nil
}
