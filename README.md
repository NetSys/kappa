# Kappa

Kappa is a framework for creating and running applications for serverless
computing platforms.  For more information, check out
[our website](https://kappa.cs.berkeley.edu/).

## Get Kappa

See the [Quick-Start Tutorial](https://kappa.cs.berkeley.edu/quick-start.html).

## Development

This section contains instructions on building and running Kappa on your local machine for development purposes.
If you only wish to use Kappa to write and run applications, see the "Get Kappa" section above.

Be sure to go through the [Quick-Start Tutorial](https://kappa.cs.berkeley.edu/quick-start.html) and the
[Programming Model documentation](https://kappa.cs.berkeley.edu/programming.html) from our website before proceeding.

### Requirements

To build and run Kappa, you need to have:

- A Unix-like environment (e.g., Linux or Mac OS).
- Python 3.6 or higher ([install](https://www.python.org/downloads/)).
  - We recommended creating a
    [virtual environment](http://docs.python-guide.org/en/latest/dev/virtualenvs/)
    with the desired Python version.

  ```console
  $ python3 --version
  Python 3.6.5
  ```
- Go 1.10 or higher ([install](https://golang.org/doc/install)).
  ```console
  $ go version
  go version go1.10.2 darwin/amd64
  ```
  - If you just installed Go, don't forget to configure your `GOPATH` if needed
    ([here's how](https://github.com/golang/go/wiki/SettingGOPATH)).
- Your AWS credentials set up.
  - We recommend installing the AWS command-line utilities:
    ```console
    $ pip install awscli
    ```
    and following their
    [configuration instructions](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html).

Kappa furthermore requires that your machine be **publicly accessible on the Internet**.  Specifically, your machine
should have a public IP address and should not be behind a NAT or a firewall that prevents incoming TCP connections.

> If your machine isn't publicly accessible, consider using an EC2 instance.

Certain example programs also require the `unbuffer` command.

- To install on Mac OS using Homebrew:
  ```console
  $ brew install expect
  ```
- To install on Ubuntu:
  ```console
  $ sudo apt install expect
  ```

### Installation

Fetch the repository and install the coordinator:
```console
$ go get -u github.com/NetSys/kappa/...
```
You should now be able to invoke the coordinator from the command line:
```console
$ coordinator --help
Usage of coordinator:
  -config string
        configuration file for the platform; auto-detected if unspecified
  -env value
        environment variables to pass to handler, e.g., "--env KEY1=value1 --env KEY2=value2"
...
```

> If the `coordinator` command cannot be found, make sure that the `$GOPATH/bin` directory is under your `PATH`
> ([details](https://golang.org/doc/code.html#GOPATH)).

You may want to create a symlink to the local repository at a more convenient location:
```console
$ ln -s $(go env GOPATH)/github.com/NetSys/kappa .
```

Next, `cd` into the repository and install Python dependencies:
```console
$ pip install -r requirements.txt
```

And Kappa is thus installed!  To test out your installation, see the next (Usage) section.

If you update the coordinator code, you can re-install the coordinator like this:
```console
$ cd $(go env GOPATH)/src/github.com/NetSys/kappa
$ go install ./...
```
No re-installation is required for the compiler or the runtime code because they're in Python.

### Usage

After you write a Kappa program according to the [Programming Model](https://kappa.cs.berkeley.edu/programming.html),
you can run the program on AWS Lambda in two steps:

1. Transform the program source code using the Kappa compiler.
2. Execute the transformed code using the Kappa coordinator.

The `kappa` script from our release is a simple wrapper around these two steps.

We now use an example to illustrate how to perform these steps manually.

First, `cd` into the `compiler/tests` directory of your local Kappa repository.  We'll use the `test_factorial.py`
program there as our example.
```console
$ cd compiler/tests
$ ls test_factorial.py
test_factorial.py
```

To compile the program, invoke the `do_transform.py` script, which takes the input source code from stdin and emits
transformed source code to stdout:
```console
$ ../do_transform.py < test_factorial.py > test_factorial_transformed.py
```

We can now invoke the transformed program using the `coordinator` command:
```console
$ coordinator --event='{"n": 100}' test_factorial_transformed.py ../rt
2018/06/08 15:28:02.531788 openLogFiles: logging to directory: workload-log-0
2018/06/08 15:28:02.532021 coordinator: using platform: aws
...
```
Note that we supply application input using the `--event` flag.  The positional arguments taken by `coordinator` are
all the files and directories that make up the application; in this case, the application consists of the transformed
source and the Kappa runtime library.  The first positional argument identifies the entry point script of the
application.

Type `coodinator --help` to learn more about the usage of the Kappa coordinator.

### Examples

There are several example Kappa applications under the [examples](examples) directory.

### Testing

The Kappa tests are written using the [`pytest`](https://docs.pytest.org/en/latest/) framework and are located at
[test_integration.py](test_integration.py).  To run the tests, use a command like:
```console
$ pytest test_integration.py -v -n 4
```
This command runs the tests in verbose mode using 4 parallel processes.

The major Python components of Kappa, i.e., the [compiler](compiler/transform) and the [runtime library](compiler/rt),
have decent [type annotation](https://www.python.org/dev/peps/pep-0484) coverage.  You may use Python type checkers like
[mypy](http://mypy-lang.org/) to type check these modules:
```console
$ mypy compiler/transform
$ mypy --ignore-missing-imports compiler/rt  # The flag silences mypy re missing boto3 annotations.
```
