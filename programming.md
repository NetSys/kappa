---
layout: doc
title: Programming Model
category: user-guide
---
* TOC
{:toc}

{{ site.name }} aims to run mostly unmodified **Python 3** code on serverless
platforms.  As such, {{ site.name }} application code is mostly just ordinary
Python 3 code, with a few extra features and restrictions which are described
below.

## Application Structure

A {{ site.name }} application lives in a directory named after the application
(e.g., `./factorial`).  The application directory must contain a file named
`handler.py`, which is the entry point script of the application.  This Python
file must, in turn, define a function named `handler` with the following
signature:
```python
def handler(event, _):
    pass
```
{{ site.name }} starts an application by calling this function.

The `handler` function takes two positional arguments.  The first argument,
`event`, contains application input specified by the user in JSON form at
invocation time.  The `event` argument can be of arbitrary type as long as it
is expressible in JSON.  The second argument is currently unused.

A {{ site.name }} application may also contain files and directory other than
the entry point script; they can be Python packages imported by the
application (e.g., numpy), configuration files, etc.  Be mindful, however, of
these caveats:

- Due to the stateless nature of serverless computing, writes to these files by
  application code may **not** be persisted.
- Only the entry point script `handler.py` is allowed to make coordinator calls
  (see below).
- The serverless platform may impose limits on code size.  For instance, at the
  time of writing, AWS Lambda
  [limits](https://docs.aws.amazon.com/lambda/latest/dg/limits.html) the
  deployment package size to 50 MB compressed and 250 MB uncompressed.

## Execution

An execution of a {{ site.name }} application consists of executing
{{ site.name }} **tasks**, each of which is a logical thread of execution.

Each task executes a Python function call `f(*args)`, where both the function
`f` and the arguments `args` are specified when the task is **spawned**.
At startup time, the {{ site.name }} library automatically spawns a very first
task running `handler(event, None)`, where `event` is user-supplied application
input.

Generally, each task is run on top of **lambda functions**, which are
time-bounded execution contexts provided by the serverless platform.  Since
each lambda function has a time limit, a {{ site.name }} task should take
checkpoints periodically so that when a lambda function dies, the task can be
resumed from a recent checkpoint on a fresh lambda function.

The programmer is responsible for ensuring that adequate checkpoints be taken.
For example, as explained in the [Quick-Start Tutorial](/quick-start), you may
call `checkpoint()` every `x` number of iterations in a loop to take checkpoints
frequently.  The `checkpoint` function belongs to a class of special functions
called **coordinator calls** (see below); making any coordinator call
automatically takes a checkpoint.

To take a checkpoint, the {{ site.name }} runtime library serializes and saves
all **live variables** at the program point where the checkpoint is taken.
Informally, a variable can be live at a program point if it can be accessed by
subsequent code.  For example, in the following code snippet:
```python
def foo(x, y):
    z = x * y
    checkpoint()
    t = z + x
    return t
```
the checkpoint will contain values for variables `x` and `z` (variable `y` is
not live as it is not accessed in later code).

{{ site.name }} serializes live variables using Python's `pickle` module, and
raises a runtime exception if serialization fails.  The programmer is
responsible for making sure that any live values at checkpoint locations are
**picklable**, i.e., capable of being serialized using `pickle`.  See
[`pickle` documentation](https://docs.python.org/3/library/pickle.html)
for details.

While tasks in {{ site.name }} are single-threaded, {{ site.name }} enables
concurrent processing by allowing each task to spawn other tasks which execute
in parallel, and by providing inter-task communication mechanisms that allow
tasks to communicate and coordinate with each other.  The concurrency mechanism
is detailed in a subsequent section.

## Coordinator Calls

The {{ site.name }} library, `rt`, provides a special set of functions called
**coordinator calls**.  These functions implement features such as
checkpointing and synchronization between tasks.  Here is a list of core
coordinator calls:

- `checkpoint()`: Takes a checkpoint.
- `fut = spawn(f, args)` Spawns a task to run `f(*args)` and returns a future
  for the result.
- `exit(ret)`: Exits current task with result `ret` (called automatically when
  the `handler` function completes).
- `ret = fut.wait()`: Returns result of `fut`; blocks until result is ready.
- `q = create_queue(maxsize)`: Creates a queue that holds at most `maxsize`
  elements.
- `q.enqueue(obj)`: Enqueues `obj` into queue `q`; blocks if queue is full.
- `obj = q.dequeue()`: Dequeues object from queue `q`; blocks if queue is
  empty.

Coordinator calls differ from regular Python functions in the following ways:

- **They can only be called from within `handler.py`.**  {{ site.name }}
  doesn't support making coordinator calls from other Python files in the
  application or from  external libraries.
- **Coordinator calls have at-most once semantics.**  Coordinator calls are
  guaranteed to not be duplicated in face of lambda function timeouts.  For
  example, a call to `spawn` will never be executed twice even if the task gets
  restarted due to lambda function timeout.
- **Each coordinator call takes a checkpoint.**  To guarantee at-most once
  semantics, each coordinator call automatically takes a checkpoint.  In this
  sense, `checkpoint` is considered the "no-op" coordinator call since it does
  nothing other than taking a checkpoint.
- **Arguments to coordinator calls must be picklable.**  For example, you can
  only `enqueue` objects that `pickle` can serialize.

Coordinator calls carry greater overhead than ordinary Python function calls
because they need to take and persist a checkpoint, as well as contact the
coordinator machine over the network.  Reducing the number of coordinator calls
made may improve application performance.

## Concurrency

To allow parallel computation on the serverless platform, {{ site.name }}
provides mechanisms by which a running task can **spawn** additional tasks that
run in parallel, and parallel tasks can communicate and synchronize through
FIFO **queues**.  We will showcase these mechanisms using two examples.

### Example: Parallel Fibonacci

Our first example features a recursive computation of Fibonacci numbers.  To
compute `fib(n)`, we spawn two sub-tasks to compute `fib(n-1)` and `fib(n-2)`
in parallel, then spawn a third sub-task to compute their sum.

```python
from rt import spawn

def sum_two(a, b):
    return a + b  # a, b are ints, not futures.

def fib(n):
    if n <= 1:
        return n
    else:
        fut1 = spawn(fib, (n-1,))
        fut2 = spawn(fib, (n-2,))
        fut_sum = spawn(sum_two, (fut1, fut2))
        return fut_sum.wait()

def handler(event, _):
    return fib(event["n"])
```
{: .copy}

The `spawn` function takes a function `f` and a sequence of arguments `args`,
spawns a task that runs `f(*args)`, and returns a **future** object to the
result.

> Recall that the function `f`, a coordinator call argument, must be picklable.
> In practice, this means `f` can be any function defined at the module level
> or any built-in function in Python.

There are two ways to use the result of a future `fut`:

1. Retrieve the result **explicitly** through `fut.wait()`, a coordinator call
   that blocks until the result is produced (i.e., waits for the spawned task
   to complete), and returns the result.

   For example, the `fib` function above calls `fut_sum.wait()` to retrieve
   the result of the `sum_two` task.

2. Alternatively, pass `fut` to a spawned sub-task as an argument, in which case
   the sub-task doesn't start until the result of `fut` is ready, and the result is
   **implicitly** substituted for the future as argument to the sub-task.

   For example, the `sum_two` task is spawned with arguments `fut1` and
   `fut2`, which are automatically turned into their values (i.e., `fib(n-1)`
   and `fib(n-2)`) before being passed to the `sum_two` function.

   This mechanism simplifies constructing dependency graphs for tasks.

### Example: Message Passing

{{ site.name }} allows currently-executing tasks to communicate and synchronize
with each other through FIFO **queues**.  Take a look at this example:

```python
from rt import create_queue, on_coordinator, spawn

def count(q):
    """Counts the strings passed into queue, stopping at None."""
    ctr = 0
    while q.dequeue() is not None:
        ctr += 1
    return ctr

def gen(q):
    """Passes two strings into queue."""
    q.enqueue("a")
    q.enqueue("b")
    q.enqueue(None)

@on_coordinator
def handler(_event, _):
    q = create_queue(maxsize=1)
    fut = spawn(count, (q,))
    spawn(gen, (q,))
    assert fut.wait() == 2
```
{: .copy}

The entry point `handler` function creates a queue `q` and passes it to two
spawned tasks:

- The `gen` task **enqueues** two strings into the queue, then a `None` to
  signal completion.
- The `count` task repeatedly **dequeues** strings from the queue and maintains
  a count, stopping upon seeing a `None`.

In the end, we `assert` that the `count` task has retrieved the correct number
of strings from the queue.

Queues can also be used for synchronization: `dequeue` blocks if the queue is
empty, and `enqueue` blocks if the queue is full.  One can implement other
synchronization primitives, e.g., semaphores, on top of queues.

Finally, note that the `handler` function is annotated with `@on_coordinator`.
When a task is spawned running an `on_coordinator` function, the task is
launched as a normal Python process **on the coordinator machine** instead of
on a lambda function.  As a result, such tasks can issue coordinator calls
faster (no network latency), and do not suffer from lambda function
timeouts.

However, these functions take up resources on the coordinator machine.  The
`handler` function above is a good candidate for an `on_coordinator` task
because it mostly just spawns and waits on other tasks and does little
computation on its own.

## Idempotence

Recall that when a {{ site.name }} task gets killed, it gets restarted from a
previous checkpoint.  As such, code may be re-executed when timeouts occur,
which can be problematic if the re-executed code is non-idempotent.

Consider, for example, a {{ site.name }} task that sends an email.  If this
task is restarted in the middle of sending an email, the re-executed task may
possibly send a duplicate copy of the email.

{{ site.name }}, by default, provides at-most once semantics only for
coordinator calls.  Care must thus be taken in all other scenarios to prevent
re-execution of non-idempotent code, either written by yourself or imported
from a third-party library.

At-most once semantics for arbitrary non-idempotent code can be achieved by
running such code as an **on-coordinator task**.  For example:
```python
from rt import spawn, on_coordinator

@on_coordinator
def send_email():
    # ... code to send email ...
    pass

def handler(event, _):
    # ...
    fut = spawn(send_email, ())
    fut.wait()
    # ...
```
Recall that the `@on_coordinator` annotation causes the `send_email` task to be
run as a regular process on the coordinator machine rather than on a lambda
function.  Since the coordinator machine (e.g., your laptop) is assumed to
never fail, the `send_email` task has no risk of failing in the middle either.
Failures in the `handler` task do not cause any problems as `spawn` and `wait`
are guaranteed at-most once semantics by virtue of being coordinator calls.

## Python Restrictions

{{ site.name }} supports a fair subset of Python features that has allowed us
to implement interesting applications.  That said, below is a partial list of
Python features that {{ site.name }} currently doesn't support.  Note that
these restrictions **apply only to the entry point script** `handler.py`, and
not to any other Python modules in the application.

- Mutable global variables.
  - {{ site.name }} assumes any module-level variables in `handler.py` to be
    constant and so doesn't save them in checkpoints.
- Generators (`yield` statements) and generator expressions;
- Nested functions and classes; lambdas;
- Context managers, i.e., `with` statements;
- General function and class decorators; metaclasses;
- Exceptions, i.e., `try` and `raise` statements;
- Pausing inside magic methods such as `__add__` (however, pausing inside
  `__init__` is allowed);
- `for` and `while` statements that contain an `else` block.

If your `handler.py` uses any of these features, {{ site.name }} should display
an error message showing the offending unsupported code snippet.
