from odp.client.auth import JwtTokenProvider


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
