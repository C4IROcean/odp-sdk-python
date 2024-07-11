import pytest
import responses


@pytest.fixture
def request_mock() -> responses.RequestsMock:
    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        yield rsps
