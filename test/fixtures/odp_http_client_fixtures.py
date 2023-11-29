import pytest

from odp_sdk.auth import TokenProvider
from odp_sdk.http_client import OdpHttpClient

__all__ = [
    "mock_odp_endpoint",
    "mock_token_provider",
    "http_client",
]


@pytest.fixture(scope="session")
def mock_odp_endpoint() -> str:
    return "http://odp.local"


@pytest.fixture(scope="session")
def mock_token_provider() -> TokenProvider:
    class MockTokenProvider(TokenProvider):
        def __init__(self):
            pass

        def get_token(self) -> str:
            return "mock_token"

    return MockTokenProvider()


@pytest.fixture(scope="session")
def http_client(mock_odp_endpoint: str, mock_token_provider: TokenProvider) -> OdpHttpClient:
    return OdpHttpClient(base_url=mock_odp_endpoint, token_provider=mock_token_provider)
