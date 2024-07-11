import time
from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True)
def mock_sleep(request: pytest.FixtureRequest):
    if request.node.get_closest_marker("mock_sleep"):
        with patch.object(time, "sleep", lambda x: None):
            yield
    else:
        yield


class MockTime:
    def __init__(self, use_time: float):
        self.use_time = use_time

    def get_time(self) -> float:
        return self.use_time

    def __enter__(self):
        self.patcher = patch.object(time, "time", lambda: self.use_time)
        self.patcher.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.patcher.stop()

    def advance(self, seconds: float):
        self.use_time += seconds


@pytest.fixture(autouse=True)
def mock_time(request: pytest.FixtureRequest):
    if marker := request.node.get_closest_marker("mock_time"):
        use_time = marker.kwargs.get("use_time", 1560926388)
        mock_timer = MockTime(use_time)

        with mock_timer:
            yield mock_timer
    else:
        yield None
