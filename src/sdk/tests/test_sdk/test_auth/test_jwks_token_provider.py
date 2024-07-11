import pytest
import responses
from odp.client.auth import JwtTokenProvider
from test_sdk.fixtures.jwt_fixtures import MOCK_TOKEN_ENDPOINT


def test_authenticate(jwt_token_provider: JwtTokenProvider):
    access_token = jwt_token_provider.authenticate()
    assert access_token

    new_access_token = jwt_token_provider.authenticate()
    assert access_token != new_access_token


def test_get_token_novalidate(jwt_token_provider: JwtTokenProvider):
    expected_prefix = "Bearer "
    jwt_token_provider.validate_token = False

    access_token = jwt_token_provider.get_token()

    assert access_token.startswith(expected_prefix)

    # The token should be cached and reused
    new_access_token = jwt_token_provider.get_token()

    assert new_access_token.startswith(expected_prefix)
    assert access_token == new_access_token


def test_get_token_validate(jwt_token_provider: JwtTokenProvider):
    expected_prefix = "Bearer "

    jwt_token_provider.validate_token = True

    access_token = jwt_token_provider.get_token()

    assert access_token.startswith(expected_prefix)

    # The token should be cached and reused
    new_access_token = jwt_token_provider.get_token()

    assert new_access_token.startswith(expected_prefix)
    assert access_token == new_access_token


@pytest.mark.mock_time(use_time=123)
def test_renew_token(jwt_token_provider: JwtTokenProvider, request_mock: responses.RequestsMock, mock_time):
    responses.assert_call_count(MOCK_TOKEN_ENDPOINT, 0)

    access_token = jwt_token_provider.get_token()
    assert access_token
    request_mock.assert_call_count(MOCK_TOKEN_ENDPOINT, 1)

    new_access_token = jwt_token_provider.get_token()
    assert access_token == new_access_token
    request_mock.assert_call_count(MOCK_TOKEN_ENDPOINT, 1)

    mock_time.advance(3600)

    new_access_token = jwt_token_provider.get_token()
    assert access_token != new_access_token
    request_mock.assert_call_count(MOCK_TOKEN_ENDPOINT, 2)


@pytest.mark.mock_time(use_time=123)
def test_renew_token_before_leeway(
    jwt_token_provider: JwtTokenProvider, request_mock: responses.RequestsMock, mock_time
):
    responses.assert_call_count(MOCK_TOKEN_ENDPOINT, 0)

    access_token = jwt_token_provider.get_token()
    assert access_token
    request_mock.assert_call_count(MOCK_TOKEN_ENDPOINT, 1)

    mock_time.advance(3600 - (jwt_token_provider.token_exp_lee_way + 1))

    new_access_token = jwt_token_provider.get_token()
    assert access_token == new_access_token
    request_mock.assert_call_count(MOCK_TOKEN_ENDPOINT, 1)


@pytest.mark.mock_time(use_time=123)
def test_renew_token_after_leeway(
    jwt_token_provider: JwtTokenProvider, request_mock: responses.RequestsMock, mock_time
):
    responses.assert_call_count(MOCK_TOKEN_ENDPOINT, 0)

    access_token = jwt_token_provider.get_token()
    assert request_mock.assert_call_count(MOCK_TOKEN_ENDPOINT, 1)
    assert access_token

    mock_time.advance(3600 - (jwt_token_provider.token_exp_lee_way - 1))

    new_access_token = jwt_token_provider.get_token()
    assert request_mock.assert_call_count(MOCK_TOKEN_ENDPOINT, 2)
    assert new_access_token
    assert access_token != new_access_token
