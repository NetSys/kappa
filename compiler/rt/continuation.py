"""Support for saving program state using continuations."""
import abc


class Continuation(abc.ABC):
    """Represents a continuation; subclassed by all compiler-generated continuation classes."""
    def __init__(self, *args) -> None:
        """Takes as arguments the values of the captured variables (except the result of the current computation)."""
        self.data = args

    def __call__(self, result):
        """Takes the result of the current computation and resumes execution."""
        return self.run(result, *self.data)

    @staticmethod
    @abc.abstractmethod
    def run(*args):
        """Runs continuation code; implemented by subclass."""
        pass
