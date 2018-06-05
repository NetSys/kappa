# Example: Factorial

Computes `n! % MODULUS` for a user-specified integer `n` and a fixed constant
`MODULUS`.  Run this application using [run.py](run.py).

This application doesn't call `checkpoint()` explicitly; instead, it relies on
automatic checkpointing.  For example,
```console
./run.py 100000 1 -- --timeout=5
```
computes `100000! % MODULUS`, with the auto-checkpoint interval set to 1s
and the lambda function timeout set to 5s.
