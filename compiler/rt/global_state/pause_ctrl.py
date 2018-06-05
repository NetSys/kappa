import os
import sys
import time


class PauseControl(object):
    """Contains policy for whether a checkpoint should be taken at a given time."""
    DEFAULT_PAUSE_INTERVAL_SECS = 5.0
    PAUSE_INTERVAL_ENV = "PAUSE_INTERVAL_SECS"

    def __init__(self) -> None:
        self.last_pause_timestamp = time.time()

        self.pause_interval_secs = self.DEFAULT_PAUSE_INTERVAL_SECS
        pause_interval_str = os.environ.get(self.PAUSE_INTERVAL_ENV)
        if pause_interval_str is not None:
            try:
                self.pause_interval_secs = float(pause_interval_str)
            except ValueError:
                print(f"Environment {self.PAUSE_INTERVAL_ENV} not a float: {pause_interval_str}", file=sys.stderr)
            else:
                print(f"Auto-pause interval set to: {self.pause_interval_secs} s", file=sys.stderr)

    def should_pause(self) -> bool:
        """Returns True if, according to a policy, a checkpoint should be taken at this point."""
        return time.time() - self.last_pause_timestamp >= self.pause_interval_secs

    def record_pause(self) -> None:
        """Records the fact that a pause just occurred."""
        self.last_pause_timestamp = time.time()
