import json
import random
import time
from typing import Union

import jwt
import pytest
import requests
import responses
from cryptography.hazmat.primitives.asymmetric import rsa
from jwt.utils import to_base64url_uint

from odp_sdk.auth import JwtTokenProvider

__all__ = [
    "rsa_public_private_key_pair",
    "rsa_public_key",
    "rsa_private_key",
    "jwt_response",
    "auth_response",
    "jwt_token_provider",
]

ALGORITHM = "RS256"
PUBLIC_KEY_ID = "sample-key-id"

MOCK_TOKEN_ENDPOINT = "http://token_endpoint.local"
MOCK_JWKS_ENDPOINT = "http://jwks_endpoint.local"
MOCK_ISSUER = "http://issuer.local"
MOCK_SCOPE = ["scope1"]
MOCK_AUDIENCE = "audience"


class MockTokenProvider(JwtTokenProvider):
    audience: str = MOCK_AUDIENCE
    """IDP token audience"""

    scope: list[str] = MOCK_SCOPE
    """IDP token scope"""

    def get_jwks_uri(self) -> str:
        return MOCK_JWKS_ENDPOINT

    def authenticate(self) -> dict[str, str]:
        res = requests.post(
            MOCK_TOKEN_ENDPOINT,
            data={
                "grant_type": "client_credentials",
                "client_id": "foo",
                "client_secret": "bar",
                "audience": self.audience,
                "scope": " ".join(self.scope),
            },
        )

        res.raise_for_status()
        return res.json()


@pytest.fixture(scope="session")
def rsa_public_private_key_pair() -> tuple[rsa.RSAPublicKey, rsa.RSAPrivateKey]:
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public_key = private_key.public_key()
    return public_key, private_key


@pytest.fixture(scope="session")
def rsa_public_key(rsa_public_private_key_pair) -> rsa.RSAPublicKey:
    public_key, _ = rsa_public_private_key_pair
    return public_key


@pytest.fixture(scope="session")
def rsa_private_key(rsa_public_private_key_pair) -> rsa.RSAPrivateKey:
    _, private_key = rsa_public_private_key_pair
    return private_key


def jwt_response(mock, rsa_public_key: rsa.RSAPublicKey):
    mock.add(
        responses.GET,
        MOCK_JWKS_ENDPOINT,
        json={
            "keys": [
                {
                    "kty": "RSA",
                    "use": "sig",
                    "kid": PUBLIC_KEY_ID,
                    "n": to_base64url_uint(rsa_public_key.public_numbers().n).decode("utf-8"),
                    "e": to_base64url_uint(rsa_public_key.public_numbers().e).decode("utf-8"),
                    "issuer": MOCK_ISSUER,
                }
            ]
        },
    )


def auth_response(mock, rsa_private_key: rsa.RSAPrivateKey):
    def token_callback(request: requests.Request) -> tuple[int, dict, Union[str, bytes]]:
        t = int(time.time())
        claims = {
            "sub": "123",
            "iss": MOCK_ISSUER,
            "aud": MOCK_AUDIENCE,
            "iat": t,
            "exp": t + 3600,
            "nonce": random.randint(0, 1000000),
        }

        token = encode_token(claims, rsa_private_key)
        return (
            200,
            {},
            json.dumps(
                {
                    "access_token": token,
                }
            ),
        )

    mock.add_callback(responses.POST, MOCK_TOKEN_ENDPOINT, callback=token_callback, content_type="application/json")


def encode_token(payload: dict, private_key: rsa.RSAPrivateKey) -> str:
    return jwt.encode(
        payload=payload,
        key=private_key,  # The private key created in the previous step
        algorithm=ALGORITHM,
        headers={
            "kid": PUBLIC_KEY_ID,
        },
    )


@pytest.fixture()
def jwt_token_provider(
    rsa_public_key: rsa.RSAPublicKey,
    rsa_private_key: rsa.RSAPrivateKey,
) -> JwtTokenProvider:
    with responses.RequestsMock(assert_all_requests_are_fired=False) as mock:
        auth_response(mock, rsa_private_key)
        jwt_response(mock, rsa_public_key)

        yield MockTokenProvider()
