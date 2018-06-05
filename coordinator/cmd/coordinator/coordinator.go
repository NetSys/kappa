package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

	"github.com/NetSys/kappa/coordinator/pkg/executor"
)

// envT helps take handler environment variables as command line arguments.
type envT map[string]string

func (e envT) String() string {
	if e == nil {
		return ""
	}

	var varStrings []string
	for k, v := range e {
		varStrings = append(varStrings, k+"="+v)
	}

	return strings.Join(varStrings, ";")
}

func (e envT) Set(value string) error {
	if value == "" {
		return nil
	}

	splitValue := strings.SplitN(value, "=", 2)
	if len(splitValue) < 2 {
		return fmt.Errorf("misformatted environment variable: %s", value)
	}

	e[strings.TrimSpace(splitValue[0])] = strings.TrimSpace(splitValue[1])
	return nil
}

func makeConfigPath(platformName string) (string, error) {
	homeDir, ok := os.LookupEnv("HOME")
	if !ok {
		return "", fmt.Errorf("environment variable $HOME is not set")
	}

	configFileName := fmt.Sprintf("%s_config.yml", platformName)
	configFilePath := path.Join(homeDir, ".config", "kappa", configFileName)
	return configFilePath, nil
}

// makeRPCListener starts listening for TCP connections; the Listener is later used to start an RPC server.
// If lambdas are invoked remotely (e.g., on AWS), this machine must be able to accept HTTP requests from outside.
// If port is 0, uses arbitrary unused TCP port.
// Return value rpcAddr should be used by lambdas to contact the coordinator.
func makeRPCListener(platform string, port int) (l net.Listener, rpcAddr *net.TCPAddr, err error) {
	var listenIP, rpcIP net.IP
	if platform == "local" { // Just use localhost IP.
		listenIP = net.IPv4(127, 0, 0, 1) // Only listen on localhost.
		rpcIP = listenIP
	} else {
		if rpcIP, err = getExternalIP(); err != nil {
			return nil, nil, err
		}

		listenIP = net.IPv4zero
	}

	l, err = net.ListenTCP("tcp4", &net.TCPAddr{
		IP:   listenIP,
		Port: port,
	})
	if err != nil {
		return nil, nil, err
	}

	rpcAddr = &net.TCPAddr{
		IP:   rpcIP,                        // IP to contact coordinator at; might differ from listenIP.
		Port: l.Addr().(*net.TCPAddr).Port, // Get the actual listen port (unknown a priori if port passed in == 0).
	}
	return l, rpcAddr, nil
}

// getExternalIP fetches the external IP of this machine using the ipify service.
func getExternalIP() (net.IP, error) {
	// Adapted from: https://www.ipify.org/.
	resp, err := http.Get("https://api.ipify.org")
	if err != nil {
		return nil, fmt.Errorf("getExternalIP: %v", err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("getExternalIP: %v", err)
	}

	ip := net.ParseIP(string(body))
	if ip == nil {
		return nil, fmt.Errorf("getExternalIP: IP parsing failed: %s", string(body))
	}
	log.Println("getExternalIP: this machine's external IP is determined to be:", ip)
	return ip, nil
}

// launchRPCServer starts an RPC web server on a different goroutine; blocks until the server is up.
func launchRPCServer(l net.Listener, rpcHandler http.Handler) {
	mux := http.NewServeMux() // To add "ping" endpoint for detecting whether server is up.
	mux.HandleFunc("/ping", func(w http.ResponseWriter, req *http.Request) {
		w.Write([]byte("pong"))
	})
	mux.Handle("/", rpcHandler)
	go func() {
		log.Println("launching RPC server at:", l.Addr())
		log.Fatal(http.Serve(l, mux))
	}()

	const pollInterval = 500 * time.Millisecond
	for { // Poll the RPC server until it responds to ping correctly.
		err := pingRPCServer(l.Addr())
		if err == nil {
			log.Println("RPC server is up!")
			break
		}

		log.Printf("%v; retrying in %v...", err, pollInterval)
		time.Sleep(pollInterval)
	}
}

// pingRPCServer sends a request to the RPC Server's "ping" endpoint; returns an error if the ping fails.
// TODO(zhangwen): just because the RPC server can be reached locally doesn't mean it can be reached remotely.
func pingRPCServer(addr net.Addr) error {
	pingURL := fmt.Sprintf("http://%s/ping", addr)
	resp, err := http.Get(pingURL)
	if err != nil {
		return fmt.Errorf("RPC server not ready: %v", err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read RPC server response: %v", err)
	}

	if string(body) != "pong" {
		return fmt.Errorf(`RPC server responded %q to ping (should be "pong")`, body)
	}

	return nil
}

// openLogFiles opens files for coordinator and handler logs.
// If dirPath is empty, a unique directory path is chosen automatically in the working directory based on the workload name.
// The directory is created if non-existent.
// The caller is responsible for closing any files returned.
func openLogFiles(dirPath string, workloadName string) (coordLogF *os.File, handlerLogF *os.File, err error) {
	const perm = 0777

	if dirPath != "" {
		if err := os.Mkdir(dirPath, perm); err != nil && !os.IsExist(err) {
			// mkdir failed, but not because the directory already exists.
			return nil, nil, err
		}
	} else { // Generate a directory.
		for i := 0; ; i++ {
			dirPath = fmt.Sprintf("%s-log-%d", workloadName, i)
			err := os.Mkdir(dirPath, perm)
			if err == nil {
				break
			}

			if !os.IsExist(err) {
				return nil, nil, err
			}
		}
	}

	log.Println("openLogFiles: logging to directory:", dirPath)

	coordLogF, err = os.Create(path.Join(dirPath, "coordinator.log"))
	if err != nil {
		return nil, nil, fmt.Errorf("openLogFiles: %v", err)
	}

	handlerLogF, err = os.Create(path.Join(dirPath, "handlers.log"))
	if err != nil {
		closeErr := coordLogF.Close()
		if closeErr != nil {
			log.Println("openLogFiles:", closeErr)
		}
		return nil, nil, fmt.Errorf("openLogFiles: %v", err)
	}

	return coordLogF, handlerLogF, nil
}

func coordinator() (err error) {
	var platform, configPath, workloadName, eventJSON, logDir string
	var timeoutSecs, rpcTimeoutSecs, rpcPort int
	var useRPC, noLogging bool
	env := make(envT)

	// Parse command line arguments
	flag.StringVar(&platform, "platform", "aws", "cloud platform to run lambdas on")
	flag.StringVar(&configPath, "config", "",
		"configuration file for the platform; auto-detected if unspecified")
	flag.StringVar(&workloadName, "name", "workload", "name of the workload")
	flag.StringVar(&eventJSON, "event", "{}", "application event (in JSON)")
	flag.IntVar(&timeoutSecs, "timeout", 300, "handler timeout (in seconds)")
	flag.BoolVar(&useRPC, "rpc", true,
		"use RPC for coordinator calls (coordinator must be able to receive HTTP request from lambda)")
	flag.IntVar(&rpcPort, "rpc-port", 43731,
		"TCP port number for RPC server (pass 0 for arbitrary unused port)")
	flag.IntVar(&rpcTimeoutSecs, "rpc-timeout", 1,
		"maximum amount of time to keep lambda wait for an RPC before terminating lambda")
	flag.Var(env, "env",
		`environment variables to pass to handler, e.g., "--env KEY1=value1 --env KEY2=value2"`)
	flag.BoolVar(&noLogging, "no-logging", false,
		"prevents outputting logs to disk (coordinator logs still written to stderr)")
	flag.StringVar(&logDir, "log-dir", "",
		"directory to store log files in (automatically chosen if not specified)")
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	handlerLogWriter := ioutil.Discard // By default, discard handler logs.
	if !noLogging {
		// Set up log file for coordinator and handlers.
		coordLogF, handlerLogF, err := openLogFiles(logDir, workloadName)
		if err != nil {
			return err
		}
		defer coordLogF.Close()
		defer handlerLogF.Close()

		handlerLogWriter = handlerLogF
		log.SetOutput(io.MultiWriter(os.Stderr, coordLogF)) // Log to both stderr and log file.
	} else if logDir != "" {
		log.Println(`coordinator: WARNING: "log-dir" flag ignored since "no-logging" flag is set`)
	}

	log.Println("coordinator: using platform:", platform)

	var appEv interface{}
	if err := json.Unmarshal([]byte(eventJSON), &appEv); err != nil {
		return err
	}

	deployedFiles := flag.Args() // Assume all non-flag arguments are files to be deployed.
	if len(deployedFiles) == 0 {
		return fmt.Errorf("must supply at least one file to deploy")
	}

	var f io.ReadCloser
	if configPath == "" {
		// Look for configuration file at default location.
		if configPath, err = makeConfigPath(platform); err != nil {
			return err
		}
		if f, err = os.Open(configPath); err != nil {
			f = nil
			log.Printf("coordinator: cannot open config file: %s; using default config...", configPath)
		}
	} else {
		if f, err = os.Open(configPath); err != nil {
			return fmt.Errorf("coordinator: cannot load config file (%s): %v", configPath, err)
		}
	}
	if f != nil {
		defer f.Close() // TODO(zhangwen): maybe close the file sooner?
	}

	var l net.Listener
	var rpcAddr *net.TCPAddr
	if useRPC {
		if l, rpcAddr, err = makeRPCListener(platform, rpcPort); err != nil {
			return fmt.Errorf("%v (does this machine have a external IP?)", err)
		}
	}

	rpcTimeout := time.Second * time.Duration(rpcTimeoutSecs)
	w, err := executor.NewWorkload(platform, f, workloadName, deployedFiles, timeoutSecs, rpcAddr, handlerLogWriter,
		env, rpcTimeout)
	if err != nil {
		return err
	}
	// TODO(zhangwen): add in option to skip cleanup.
	defer w.Finalize()

	if useRPC {
		launchRPCServer(l, w)
	}

	startTime := time.Now()
	res, err := w.Run(appEv)
	log.Println("Workload duration (s):", time.Since(startTime).Seconds())

	if err != nil {
		return err
	}

	log.Println("coordinator: final result:", res)
	fmt.Println(res)
	return nil
}

func main() {
	err := coordinator()
	if err != nil {
		log.Fatal(err)
	}
}
