# The Kappa Compiler

The Kappa compiler implements a code transformation that enables checkpointing
of Python code; this allows executing long-running tasks on time-bounded lambda
functions.  In the Kappa workflow, an application is first transformed by the
compiler, and the transformed code is executed on lambda functions by the
coordinator.

To invoke the compiler, run `do_transform.py`, which reads input code from
stdin and writes transformed code to stdout.  For example,
```console
user:./compiler$ ./do_transform.py < tests/test_factorial.py > tests/test_factorial_transformed.py
```

Kappa checkpoints are implemented using
[**continuations**](https://en.wikipedia.org/wiki/Continuation), a
language-level mechanism that can be executed entirely in user mode.  A
continuation can be thought of as a closure (i.e., a function with some
associated data) that captures program state and control flow information at
some point in the program's execution; calling the closure resumes execution
from this point in the program.

Kappa takes checkpoints by generating a continuation and serializing it to
stable storage (e.g., Amazon S3), and restores from checkpoints by
deserializing and invoking a previously stored continuation.  Checkpoint
persistence and restoration is implemented by the Kappa runtime library (`rt`).

## Why user-level checkpointing?

As explained above, Kappa implements *user-level* checkpointing: the mechanism
works with existing operating systems and Python 3 execution environments; it
requires no kernel modification or root privilege.

Kappa takes a white-box approach to checkpointing: it analyzes the application
source code to determine which variables need to be saved at each pause point.
This is in comparison to blackbox approaches to checkpointing, where the
runtime has no insight into the program behavior and treats a process address
space as an uninterpreted blob.

These design decisions come with several advantages:
  * No modifications to the host environment: Kappa can be easily used
    on today's serverless platforms, with no effort from the cloud provider.
  * Portability: Kappa works on any platform supported by Python 3.
  * Potentially smaller checkpoint size: Knowledge of program behavior allows
    Kappa to save only necessary state for resumption.  As a result,
    Kappa checkpoints can be smaller than entire address space dumps.

One restriction is that checkpoints cannot be taken at arbitrary program
points; they can only be taken through making coordinator calls (e.g., calling
`checkpoint()`).  This design allows the Kappa compiler to statically
compute the set of live variables for each pause point.

## Transformation basics

We now delve into how the Kappa compiler transforms source code.  Note that
we'll be giving a high-level explanation that only conveys the general idea.
The explanation will omit many details, and so it may not match the
implementation word-for-word.

To help with the explanation, we will frequently refer to this running example:
```python
def foo(x, y):
    z = x * y
    checkpoint()  # <-- pause point
    t = z + x
    return t
```

Wherever a function's execution could potentially take a checkpoint (*pause
points*), the Kappa compiler needs to insert code that saves all necessary
state in the function in case a pause occurs at runtime.  Every pause point is
at a function invocation: a checkpoint can only be taken by making a
coordinator call (e.g., `checkpoint()`) either directly or indirectly (through
calling another function).  The compiler, therefore, simply treats *every*
function call as a pause point.  In the example, the only pause point is at
`checkpoint()`.

For every pause point in the program, the compiler answers two questions:

- **What code is run next?**  The answer to this question reveals what code to
  run when resuming from a checkpoint taken here.  In the example, the code
  that is run after the pause point is:

  ```python
  t = z + x
  return t
  ```

  Conceptually, to resume from this pause point, we need to "jump" to the
  middle of the `foo()` function, right after `checkpoint()`.  However,
  Python doesn't easily allow arbitrary jumping.  To achieve the same effect,
  the compiler copies the subsequent code and into a newly-defined **continuation
  function**:

  ```python
  def cont_foo(...):
      t = z + x
      return t
  ```

  > In reality, this function would be emitted as a method on a **continuation
  > class**.  Run the compiler to see for yourself.

  After inserting this definition into the source, we can invoke `cont_foo` on
  resumption so that only the statements after the pause point is run.

- **What data needs to be saved?**  Since we simply invoke the continuation
  function on resumption, we only need to save the variables that are used by
  the continuation function.  In `cont_foo`, only variables `z` and `x` are used
  (`t` is only assigned to but not read from, and `y` isn't referred to at all).
  We therefore add these two variables as arguments to `cont_foo`:

  ```python
  def cont_foo(z, x):
      t = z + x
      return t
  ```

  In general, the live variables at a program point can be determined statically
  through standard
  [liveness analysis](https://en.wikipedia.org/wiki/Live_variable_analysis).

In summary, to take a checkpoint this pause point, we save the values of `z`
and `x` and an identifier for `cont_foo` to mark where to continue.  To resume,
we simply call `cont_foo(z, x)` with saved values of `z` and `x`.

## Control flow

The preceding example is a simple one in that it features only straight-line
code.  How does the compiler deal with more complex control flow?

### Function calls

A call to `checkpoint()` can happen deep down the call stack.  At resumption
time, program execution begins at the caller of `checkpoint()` and, after
completing that function, jumps to its caller and finishes that function,
before jumping to its caller, etc.

To capture every function on the call stack when a checkpoint is taken, the
Kappa compiler creates a continuation function at **every** function
invocation, because a function's state must still be saved if a checkpoint is
triggered by one of its callees.

At runtime, when a checkpoint is taken, we unwind the stack level by level and,
at each level, record the values of all live variables reachable from the
function's stack frame along with the corresponding continuation function,
packaged into a continuation object.  In the end, all continuation objects are
packed into a list, serialized, and persisted.

Concretely, to unwind the stack, `checkpoint()` (as well as any other
coordinator call) raises an exception, which gets caught at every
level of the call stack.  The compiler wraps every function call inside a
`try`-`except` block catching the exception, and installs code in the exception
handler to save live variables and to re-raise the exception in order to
continue climbing up the stack.

### Conditionals

Handling a pause point inside an `if` statement is straightforward.  For this
example snippet:

```python
if x < y:
    checkpoint()
    z = x + 1
else:
    z = y + 2

return z
```
a continuation function is created for the pause point:
```python
def cont(x):
    z = x + 1
    return z
```
In particular, the branch that the pause point doesn't belong to is omitted in
the continuation function on the grounds that it will not be executed.

The arguments taken by `cont()` is, again, the set of live variables at the
pause point (in this case, just `x`).

### Loops

Handling pause points inside a loop body requires more thought.  Consider this
example:
```python
msg = "hi"
while i < 100:
    print(msg)
    checkpoint()  # <- pause point
    i += 1
```
The continuation function created for the pause point looks something like
this:
```python
def cont(i, msg):
    i += 1
    while i < 100:
        print(msg)
        checkpoint()  # <- pause point
        i += 1
```
Intuitively, when restarting the program at the pause point, we first finish
the remainder of the current iteration of the loop, and then go back to the top
of the loop, checking the loop condition and possibly carrying out more
iterations of the loop.

One subtlety is that the `while` loop in `cont()` is **not** a verbatim copy of
the original loop; it is the **transformed** loop, where each function call has
been wrapped in a `try`-`except` block with a reference to its continuation
function, etc.  However, we don't have the transformed loop ready at this
moment, because we're in the process of transforming the loop!

To solve this problem, we create a placeholder AST node for the transformed
loop so that `cont()`'s body AST can refer to it.  We keep adding to the
placeholder AST node as we transform the loop body.  When the transformation is
done, `cont()` will contain the correct function body.

One final complication arises from `break` and `continue` statements appearing
below a pause point.  Consider the following example:
```python
while i < 20:
    checkpoint()  # <-- pause point
    if i == 10:   # -+
        break     #  |- (*)
    i += 1        # -+
```

The naive transformation fails because in the continuation function, the `(*)`
portion won't appear in any loop and so the `break` statement within would be
illegal.  To preserve the functionality of the `break`, the compiler emits an
extra wrapping loop that has only one iteration:
```python
def cont(i):
    for _ in range(1):
        if i == 10:  # -+
            break    #  |- (*)
        i += 1       # -+
    else:
        while i < 20:
            # Original loop body...
```

In the continuation function, the `break` statement applies to the `for` loop,
skipping the rest of the loop body *and* the
[`else` branch](https://docs.python.org/3.6/tutorial/controlflow.html#break-and-continue-statements-and-else-clauses-on-loops)
of the `for` loop.  A `continue` statement in its place would also skip the
remainder of the loop body, but would jump to the `else` branch because the
`for` loop, in this case, would have terminated normally.

The treatment of `for` loops is similar, except that we make sure the object
being looped over is an iterator, not just any iterable object.  This way, a
checkpoint would serialize and save the iterator, thus keeping track of loop
progress.

To make sure of this, the compiler applies `iter()` to the object:
```python
for x in iter(l):
    # ...
```
