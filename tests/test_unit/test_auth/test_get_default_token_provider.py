import pytest

from odp_sdk.auth import (
    AzureTokenProvider,
    HardcodedTokenProvider,
    InteractiveTokenProvider,
    OdpWorkspaceTokenProvider,
    get_default_token_provider,
)
from odp_sdk.exc import OdpAuthError


@pytest.fixture(scope="function")
def clean_env(monkeypatch):
    """Clean environment variables for each test. Some environment variables have priority over others while choosing
    the authentication method so all of them need to be cleaned before the relevant ones are set in tests."""
    monkeypatch.delenv("ODP_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("JUPYTERHUB_API_TOKEN", raising=False)
    monkeypatch.delenv("ODP_CLIENT_SECRET", raising=False)


def test_interactive_auth():
    auth = get_default_token_provider()
    assert isinstance(auth, InteractiveTokenProvider)


def test_hardcoded_auth(monkeypatch):
    monkeypatch.setenv("ODP_ACCESS_TOKEN", "Test")
    auth = get_default_token_provider()
    assert isinstance(auth, HardcodedTokenProvider)


def test_workspace_auth(monkeypatch):
    monkeypatch.setenv("JUPYTERHUB_API_TOKEN", "Test")
    auth = get_default_token_provider()
    assert isinstance(auth, OdpWorkspaceTokenProvider)


def test_azure_auth(monkeypatch):
    monkeypatch.setenv("ODP_CLIENT_SECRET", "Test")
    auth = get_default_token_provider()
    assert isinstance(auth, AzureTokenProvider)


def test_auth_error():
    with pytest.raises(OdpAuthError):
        get_default_token_provider(fallback=False)
