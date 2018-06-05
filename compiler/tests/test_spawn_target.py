import os

import rt


def assert_on_coordinator():
    """Asserts that the task is running on the coordinator machine.

    kappa:ignore
    """
    assert os.environ["WHERE"] == "coordinator"
    assert "LAMBDA_TASK_ROOT" not in os.environ  # This environment variable is set on AWS Lambda.


def assert_on_aws_lambda():
    """Asserts that the task is running on AWS Lambda.

    kappa:ignore
    """
    assert os.environ["WHERE"] == "aws-lambda"
    assert "LAMBDA_TASK_ROOT" in os.environ


@rt.on_coordinator
def on_coordinator_child():
    assert_on_coordinator()
    return 10


def on_lambda_child():
    assert_on_aws_lambda()
    return 20


@rt.on_coordinator
def handler(_event, _context):
    assert_on_coordinator()

    f1 = rt.spawn(on_coordinator_child, ())
    f2 = rt.spawn(on_lambda_child, ())
    return f1.wait() + f2.wait()
