import os
from unittest import mock

import pytest

from odp_sdk.auth import (
    AzureTokenProvider,
    HardcodedTokenProvider,
    InteractiveTokenProvider,
    OdpWorkspaceTokenProvider,
    get_default_token_provider,
)
from odp_sdk.exc import OdpAuthError


def test_interactive_auth():
    names_to_remove = {"ODP_ACCESS_TOKEN", "JUPYTERHUB_API_TOKEN", "ODP_CLIENT_SECRET"}
    modified_environ = {k: v for k, v in os.environ.items() if k not in names_to_remove}
    with mock.patch.dict(os.environ, modified_environ, clear=True):
        auth = get_default_token_provider()
        assert isinstance(auth, InteractiveTokenProvider)


@mock.patch.dict(os.environ, {"ODP_ACCESS_TOKEN": "Test"})
def test_hardcoded_auth():
    auth = get_default_token_provider()
    assert isinstance(auth, HardcodedTokenProvider)


@mock.patch.dict(os.environ, {"JUPYTERHUB_API_TOKEN": "Test"})
def test_workspace_auth():
    auth = get_default_token_provider()
    assert isinstance(auth, OdpWorkspaceTokenProvider)


@mock.patch.dict(os.environ, {"ODP_CLIENT_SECRET": "Test"})
def test_azure_auth():
    auth = get_default_token_provider()
    assert isinstance(auth, AzureTokenProvider)


def test_auth_error():
    names_to_remove = {"ODP_ACCESS_TOKEN", "JUPYTERHUB_API_TOKEN", "ODP_CLIENT_SECRET"}
    modified_environ = {k: v for k, v in os.environ.items() if k not in names_to_remove}
    with mock.patch.dict(os.environ, modified_environ, clear=True):
        with pytest.raises(OdpAuthError):
            get_default_token_provider(fallback=False)
