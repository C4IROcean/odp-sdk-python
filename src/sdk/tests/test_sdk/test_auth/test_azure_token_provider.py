from typing import Callable

import pytest
import responses
from odp.client.auth import AzureTokenProvider


def test_get_token(azure_token_provider: AzureTokenProvider, mock_token_response_body: str):
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            azure_token_provider.token_uri,
            body=mock_token_response_body,
        )
        access_token = azure_token_provider.get_token()

        assert rsps.assert_call_count(azure_token_provider.token_uri, 1)
        assert access_token


def test_get_token_reuse(azure_token_provider: AzureTokenProvider, mock_token_response_body: str):
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            azure_token_provider.token_uri,
            body=mock_token_response_body,
        )
        access_token = azure_token_provider.get_token()

        assert rsps.assert_call_count(azure_token_provider.token_uri, 1)
        assert access_token

        new_access_token = azure_token_provider.get_token()

        assert rsps.assert_call_count(azure_token_provider.token_uri, 1)
        assert access_token == new_access_token


@pytest.mark.mock_time(use_time=123)
def test_get_token_renew(
    azure_token_provider: AzureTokenProvider, mock_token_response_callback: Callable[[], str], mock_time
):
    with responses.RequestsMock() as rsps:
        rsps.add_callback(
            responses.POST,
            azure_token_provider.token_uri,
            callback=lambda _: (200, {}, mock_token_response_callback()),
            content_type="application/json",
        )

        access_token = azure_token_provider.get_token()
        assert access_token
        assert rsps.assert_call_count(azure_token_provider.token_uri, 1)

        mock_time.advance(3600)

        new_access_token = azure_token_provider.get_token()
        assert rsps.assert_call_count(azure_token_provider.token_uri, 2)
        assert new_access_token
        assert new_access_token != access_token


@pytest.mark.mock_time(use_time=123)
def test_get_token_renew_before_leeway(
    azure_token_provider: AzureTokenProvider, mock_token_response_callback: Callable[[], str], mock_time
):
    with responses.RequestsMock() as rsps:
        rsps.add_callback(
            responses.POST,
            azure_token_provider.token_uri,
            callback=lambda _: (200, {}, mock_token_response_callback()),
            content_type="application/json",
        )

        access_token = azure_token_provider.get_token()
        assert access_token
        assert rsps.assert_call_count(azure_token_provider.token_uri, 1)

        mock_time.advance(3600 - (azure_token_provider.token_exp_lee_way + 1))

        new_access_token = azure_token_provider.get_token()

        assert rsps.assert_call_count(azure_token_provider.token_uri, 1)
        assert new_access_token == access_token


@pytest.mark.mock_time(use_time=123)
def test_get_token_renew_after_leeway(
    azure_token_provider: AzureTokenProvider, mock_token_response_callback: Callable[[], str], mock_time
):
    with responses.RequestsMock() as rsps:
        rsps.add_callback(
            responses.POST,
            azure_token_provider.token_uri,
            callback=lambda _: (200, {}, mock_token_response_callback()),
            content_type="application/json",
        )

        access_token = azure_token_provider.get_token()
        assert access_token
        assert rsps.assert_call_count(azure_token_provider.token_uri, 1)

        mock_time.advance(3600 - (azure_token_provider.token_exp_lee_way - 1))

        new_access_token = azure_token_provider.get_token()
        assert rsps.assert_call_count(azure_token_provider.token_uri, 2)
        assert new_access_token
        assert new_access_token != access_token
