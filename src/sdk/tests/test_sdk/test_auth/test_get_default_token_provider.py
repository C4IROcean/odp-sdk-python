import pytest
from odp.client.auth import (
    AzureTokenProvider,
    HardcodedTokenProvider,
    InteractiveTokenProvider,
    OdpWorkspaceTokenProvider,
    get_default_token_provider,
)
from odp.client.exc import OdpAuthError
from odp.client.utils import get_version


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
    assert auth.user_agent == f"odp-sdk/{get_version()} (Interactive)"


def test_hardcoded_auth(monkeypatch):
    monkeypatch.setenv("ODP_ACCESS_TOKEN", "Test")
    auth = get_default_token_provider()
    assert isinstance(auth, HardcodedTokenProvider)
    assert auth.user_agent == f"odp-sdk/{get_version()} (Hardcoded)"


def test_workspace_auth(monkeypatch):
    monkeypatch.setenv("JUPYTERHUB_API_TOKEN", "Test")
    auth = get_default_token_provider()
    assert isinstance(auth, OdpWorkspaceTokenProvider)
    assert auth.user_agent == f"odp-sdk/{get_version()} (Workspaces)"


def test_azure_auth(monkeypatch):
    monkeypatch.setenv("ODP_CLIENT_SECRET", "Test")
    auth = get_default_token_provider()
    assert isinstance(auth, AzureTokenProvider)
    assert auth.user_agent == f"odp-sdk/{get_version()} (Azure)"


def test_auth_error():
    with pytest.raises(OdpAuthError):
        get_default_token_provider(fallback=False)
