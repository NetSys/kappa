# Kappa

Kappa is a framework for creating and running applications for serverless
computing platforms.  For more information, check out
[our website](http://kappa.cs.berkeley.edu/).

## Get Kappa

Coming soon.

## Development

This section contains instructions on building and running Kappa on your local machine for development purposes.
If you only wish to use Kappa to write and run applications, see the "Get Kappa" section above.

### Requirements

To build and run Kappa, you need:

- A Unix-like environment (e.g., Linux or Mac OS).
- Python 3.6 or higher ([install](https://www.python.org/downloads/)).
  - We recommended creating a Python
    [virtual environment](http://docs.python-guide.org/en/latest/dev/virtualenvs/)
    with the desired Python version.

  ```console
  user:./$ python3 --version
  Python 3.6.5
  ```
- Go 1.10 or higher ([install](https://golang.org/doc/install)).
  ```console
  user:./$ go version
  go version go1.10.2 darwin/amd64
  ```

Certain example programs also require the `unbuffer` command.

- To install on Mac OS using Homebrew:
  ```
  brew install expect
  ```
- To install on Ubuntu:
  ```
  sudo apt install expect
  ```

### Installation

Coming soon.

### Examples

There are several example Kappa applications under the [examples](examples) directory.

### Testing

The Kappa tests are written using the [`pytest`](https://docs.pytest.org/en/latest/) framework and are located at
[test_integration.py](test_integration.py).  To run the tests, use a command like:
```
pytest test_integration.py -v -n 4
```
This command runs the tests in verbose mode using 4 parallel processes.

The major Python components of Kappa, i.e., the [compiler](compiler/transform) and the [runtime library](compiler/rt),
have decent [type annotation](https://www.python.org/dev/peps/pep-0484) coverage.  You may use Python type checkers like
[mypy](http://mypy-lang.org/) to type check these modules:
```console
user:./kappa$ mypy compiler/transform
user:./kappa$ mypy --ignore-missing-imports compiler/rt  # The flag silences mypy re missing boto3 annotations.
```
