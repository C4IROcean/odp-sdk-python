import pytest
from odp.client.auth import TokenProvider
from odp.client.http_client import OdpHttpClient

__all__ = [
    "mock_odp_endpoint",
    "http_client",
]


@pytest.fixture(scope="session")
def mock_odp_endpoint() -> str:
    return "http://odp.local"


@pytest.fixture
def http_client(mock_odp_endpoint: str, jwt_token_provider: TokenProvider) -> OdpHttpClient:
    return OdpHttpClient(base_url=mock_odp_endpoint, token_provider=jwt_token_provider)
