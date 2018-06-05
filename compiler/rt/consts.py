"""Contains constants for the runtime module."""
from typing import Any, Callable, NewType, List

ContinuationT = Callable[[Any], Any]  # A continuation takes a previous result and resumes execution.
Continuations = List[ContinuationT]

# Stronger typing for integers and strings.
CheckpointID = NewType("CheckpointID", str)
Seqno = NewType("Seqno", int)
Pid = NewType("Pid", int)


MAIN_PID = Pid(0)
NEW_PID = Pid(-1)  # Represents a new process that hasn't been spawned yet.
INITIAL_SEQNO = Seqno(0)
