import pytest

from odp_sdk.auth import JwtTokenProvider


def test_authenticate_novalidate(jwt_token_provider: JwtTokenProvider):
    jwt_token_provider.validate_token = False

    access_token = jwt_token_provider.authenticate()

    # The token should be cached and reused
    new_access_token = jwt_token_provider.authenticate()

    assert access_token == new_access_token


def test_authenticate_validate(jwt_token_provider: JwtTokenProvider):
    jwt_token_provider.validate_token = True

    access_token = jwt_token_provider.authenticate()

    # The token should be cached and reused
    new_access_token = jwt_token_provider.authenticate()

    assert access_token == new_access_token
