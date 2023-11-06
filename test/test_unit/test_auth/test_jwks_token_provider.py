from odp_sdk.auth import JwtTokenProvider


def test_authenticate(jwt_token_provider: JwtTokenProvider):
    access_token = jwt_token_provider.authenticate()
    assert access_token

    new_access_token = jwt_token_provider.authenticate()
    assert access_token != new_access_token


def test_get_token_novalidate(jwt_token_provider: JwtTokenProvider):
    jwt_token_provider.validate_token = False

    access_token = jwt_token_provider.get_token()

    # The token should be cached and reused
    new_access_token = jwt_token_provider.get_token()

    assert access_token == new_access_token


def test_get_token_validate(jwt_token_provider: JwtTokenProvider):
    jwt_token_provider.validate_token = True

    access_token = jwt_token_provider.get_token()

    # The token should be cached and reused
    new_access_token = jwt_token_provider.get_token()

    assert access_token == new_access_token
