import os
from unittest import mock

import pytest
import responses
from pydantic import SecretStr

from odp_sdk.auth import AzureTokenProvider, OdpWorkspaceTokenProvider

__all__ = ["odp_workspace_token_provider", "azure_token_provider", "mock_env_vars"]

MOCK_SIDECAR_URL = "http://token_endpoint.local"
MOCK_CLIENT_ID = SecretStr("foo")
MOCK_CLIENT_SECRET = SecretStr("bar")
MOCK_AUTHORITY = "http://authority.local"
MOCK_SCOPE = ["scope1"]
MOCK_TENANT_ID = "tenant_id"
MOCK_TOKEN_URI = "http://token_uri.local"
MOCK_JWKS_URI = "http://jwks_uri.local"


@pytest.fixture()
def odp_workspace_token_provider() -> OdpWorkspaceTokenProvider:
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            MOCK_SIDECAR_URL,
            json={"token": "test"},
        )

        yield OdpWorkspaceTokenProvider(token_uri=MOCK_SIDECAR_URL)


@pytest.fixture()
def azure_token_provider() -> AzureTokenProvider:
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            MOCK_TOKEN_URI,
            json={"access_token": "test"},
        )

        yield AzureTokenProvider(
            client_id=MOCK_CLIENT_ID,
            client_secret=MOCK_CLIENT_SECRET,
            token_uri=MOCK_TOKEN_URI,
        )


@pytest.fixture(autouse=True)
def mock_env_vars():
    names_to_remove = {"ODP_ACCESS_TOKEN", "JUPYTERHUB_API_TOKEN", "ODP_CLIENT_SECRET"}
    modified_environ = {k: v for k, v in os.environ.items() if k not in names_to_remove}
    with mock.patch.dict(os.environ, modified_environ, clear=True):
        yield
