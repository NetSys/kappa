---
layout: default
---
# Quick-Start Tutorial
In this tutorial, we will set up the {{ site.name }} framework and use it to
run a simple "factorial" application on the
[AWS Lambda](https://aws.amazon.com/lambda/) serverless platform.

## Requirements
To set up {{ site.name }}, you need:
- a UNIX-like environment (e.g., Mac or Ubuntu); and,
- [Docker](https://docs.docker.com/install/) installed.
    - Make sure you can run the `docker` command without `sudo` (e.g., see [these instructions](https://docs.docker.com/install/linux/linux-postinstall/#manage-docker-as-a-non-root-user) if you are runnning Linux).

You may set up this environment either on your local machine or on a
virtual machine in the cloud (e.g., an Amazon EC2 instance).
From now on, we'll refer to this machine as the *coordinator machine*.

The coordinator machine, in order to receive requests from lambda
functions, must be publicly accessible on the Internet.
For example, a machine behind a NAT or a firewall preventing incoming
connections may not satisfy this requirement.

For {{ site.name }} to run applications on AWS Lambda, you need to have
an account with [Amazon Web Services](https://aws.amazon.com/) (AWS).
{{ site.name }} will need an **access key** to your AWS account.
If you have already set up your AWS credentials, e.g., through the AWS CLI,
you're all set as {{ site.name }} will detect your credentials automatically.
Otherwise, now's a good time to get your access key ready
([here's how](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html));
it should look something like this:
```
Access key ID: AKIAIOSFODNN7EXAMPLE
Secret access key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```
and you will need to enter it when prompted later on.

## Get {{ site.name }}
{{ site.name }} comes in a single Bash script, `kappa`, which is responsible
both for downloading {{ site.name }} (when invoked for the first time) and for
executing {{ site.name }} applications.

**TODO**: make this work.

**TODO**: this snippet doesn't seem copy-pastable.
```console
user:./$ mkdir kappa_home; cd kappa_home
user:./kappa_home$ wget {{ site.url }}{{ site.baseurl }}/kappa
user:./kappa_home$ chmod +x kappa
```

You may want to add `kappa` to your `PATH` so that it can be easily invoked
anywhere.

## Create an Application
Let's now create a simple {{ site.name }} application that computes factorials.
Create a directory for the application:
```console
user:./kappa_home$ mkdir factorial_app
```

The entry point to a {{ site.name }} application is a Python script named
`handler.py`.  Create `factorial_app/handler.py` with the following content:

```python
from rt import checkpoint

def factorial(n):
    result = 1
    for i in range(1, n + 1):
        print("i = %d" % i)
        result *= i
        if i % 10 == 0:
            checkpoint()

    return result

def handler(event, _):
    n = event["n"]
    return factorial(n)
```

Application execution begins from the `handler` function.  It takes an `event`
argument, which contains application input provided by the user at invocation
time.  The second argument is currently unused.

The script imports the `checkpoint` function from the {{ site.name }} library
`rt`.  The `checkpoint` function takes and persists a checkpoint.  Since the
`factorial` function calls `checkpoint` every ten iterations, no matter when
the lambda function dies, the progress lost is at most ten iterations of the
loop.

## Run the Application Using {{ site.name }}
Let's compute `100!` by running the factorial application on AWS Lambda using
{{ site.name }}:
```console
user:./kappa_home$ ./kappa ./factorial_app --event='{"n": 100}'
```
where the `event` argument specifies, in JSON, the application input passed to
the `handler` function as the `event` argument.

> The first time you run {{ site.name }}, you may be prompted for your AWS
> credentials like this:
> ```
> AWS Access Key ID [None]: AKIAIOSFODNN7EXAMPLE
> AWS Secret Access Key [None]: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
> Default region name [None]: us-west-2
> Default output format [json]:
> ```
> Enter your AWS access key obtained in the Requirements section.  For good
> performance, the
> [AWS region](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html#concepts-available-regions)
> you enter should ideally be close to the coordinator machine (e.g., if the
> coordinator machine is on EC2, use the same region as the EC2 instance).

> If you see this warning:
> ```
> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
> [!!!!! WARNING  !!!!!] RPC: timeout, falling back to synchronous (is your coordinator machine publicly accessible?)
> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
> ```
> it is likely that the RPC failed because the coordinator machine is not
> accessible from the public web.  If the machine is on EC2, make sure that the
> security policy allows inbound TCP connections on the RPC port `43731`.
>
> While the usage of RPCs may greatly improve application performance, it is
> not required for functionality; you may disable RPCs with the flag
> `--rpc=false`.

As the application runs, {{ site.name }} should be printing out quite a bit of
log messages to your terminal.  Towards the end, you should find the
application's final result, i.e., the `handler` function's return value:
```
2018/05/28 15:17:23.536420 coordinator: final result: 93326215443944152681699238856266700490715968264381621468592963895217599993229915608941463976156518286253697920827223758251185210916864000000000000000000000000
```

The {{ site.name }} logs are also written to files located in the directory
displayed in the last line of the output:
```console
<2018-05-28 15:17:33> Kappa logs can be found in /your/files/logs
```

Check out the log files:
```console
user:./kappa_home$ ls logs
factorial-log-0
user:./kappa_home$ ls logs/factorial-log-0/
coordinator.log handlers.log
```
As can be seen, the log directory contains two files:
- `coordinator.log` is the *coordinator log*, which just contains log messages
  printed to the terminal and describes events such as lambda function
  launches.
- `handlers.log` is the *handler log*, which is produced by the application
  code and the {{ site.name }} library.  For example, the log contains anything
  printed to stdout and stderr:
  ```console
  user:./kappa_home$ grep "i =" logs/factorial-log-0/handlers.log | head
  i = 1
  i = 2
  i = 3
  i = 4
  i = 5
  i = 6
  i = 7
  i = 8
  i = 9
  i = 10
  ```
  These lines correspond to the `print` statement in the `factorial` function.

## More Options

### AWS Credentials
{{ site.name }}, by default, looks in `~/.aws`, then `./kappa_home/.aws` for your AWS credentials. If neither of those directories exists, it creates `./kappa_home/.aws` and asks you to input AWS credentials.
    However, if you have AWS credentials in another folder, simply run {{ site.name }} with the `AWS_DIR` environment variable set:
```console
user:./kappa_home$ AWS_DIR=your/aws/dir ./kappa ...
```

### Command Line Options
- `--env` specifies environment variables to pass to handler, e.g., `--env KEY1=value1 --env KEY2=value2`
- `--event` specifies the application event (in JSON) (default `{}`)
- `--platform` can be either `aws` or `local`. Runs your handler either on AWS or the machine you run {{ site.name }} on.
- `--rpc-timeout` specifies maximum amount of time in seconds to keep a lambda waiting for an RPC before terminating the lambda (default `1`)
- `--timeout` specifies the lambda function timeout (in seconds) (default `300`, the maximum on AWS Lambda at time of writing)
- `--no-logging` instructs {{ site.name }} to not produce log files.
