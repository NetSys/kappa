"""py.test configuration."""
import pytest


def pytest_addoption(parser):
    parser.addoption("--write-logs", action="store_true", help="write coordinator & handler logs to working directory")
    parser.addoption("--no-build", action="store_true", help="don't build the coordinator")


@pytest.fixture(scope="session")
def should_log(request) -> bool:
    return request.config.getoption("--write-logs")


@pytest.fixture(scope="session")
def no_build(request) -> bool:
    return request.config.getoption("--no-build")
