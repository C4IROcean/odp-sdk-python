from typing import List

import pytest
import requests
import responses
from pydantic import SecretStr

from odp_sdk.auth import AzureTokenProvider, JwtTokenProvider, OdpWorkspaceTokenProvider

__all__ = ["odp_workspace_token_provider", "azure_token_provider"]

MOCK_TOKEN_ENDPOINT = "http://token_endpoint.local"
MOCK_CLIENT_ID = SecretStr("foo")
MOCK_CLIENT_SECRET = SecretStr("bar")
MOCK_AUTHORITY = "http://authority.local"
MOCK_SCOPE = ["scope1"]
MOCK_TENANT_ID = "tenant_id"
MOCK_TOKEN_URI = "http://token_uri.local"
MOCK_JWKS_URI = "http://jwks_uri.local"


class MockWorkspaceTokenProvider(OdpWorkspaceTokenProvider):
    def get_token(self) -> str:
        res = requests.post(MOCK_TOKEN_ENDPOINT)
        res.raise_for_status()

        return "Bearer " + res.json()["token"]


class MockAzureTokenProvider(JwtTokenProvider):
    """Token provider for OAuth2 tokens"""

    client_id: SecretStr = MOCK_CLIENT_ID
    """IDP client ID"""

    client_secret: SecretStr = MOCK_CLIENT_SECRET
    """IDP client secret"""

    authority: str = MOCK_AUTHORITY
    """IDP token Authority"""

    scope: List[str] = MOCK_SCOPE
    """IDP token scope"""

    tenant_id: str = MOCK_TENANT_ID
    """Azure AD tenant id"""

    token_uri: str = MOCK_TOKEN_URI
    """Token endpoint. Will default to 'https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token'"""

    jwks_uri: str = MOCK_JWKS_URI
    """JWKS endpoint."""

    def get_jwks_uri(self) -> str:
        return self.jwks_uri

    def authenticate(self) -> dict[str, str]:
        res = requests.post(
            self.token_uri,
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id.get_secret_value(),
                "client_secret": self.client_secret.get_secret_value(),
                "audience": self.audience,
                "scope": " ".join(self.scope),
            },
        )

        res.raise_for_status()
        return res.json()


@pytest.fixture()
def odp_workspace_token_provider() -> OdpWorkspaceTokenProvider:
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            MOCK_TOKEN_ENDPOINT,
            json={"token": "test"},
        )

        yield MockWorkspaceTokenProvider()


@pytest.fixture()
def azure_token_provider() -> AzureTokenProvider:
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            MOCK_TOKEN_URI,
            json={"access_token": "test"},
        )

        yield MockAzureTokenProvider()
