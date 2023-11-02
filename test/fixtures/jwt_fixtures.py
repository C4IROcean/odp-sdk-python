from cryptography.hazmat.primitives.asymmetric import rsa
import jwt
from jwt.utils import to_base64url_uint
import pytest
import requests

from odp_sdk.auth import JwtTokenProvider

ALGORITHM = "RS256"
PUBLIC_KEY_ID = "sample-key-id"

MOCK_TOKEN_ENDPOINT = "http://token_endpoint.local"
MOCK_JWKS_ENDPOINT = "http://jwks_endpoint.local"
MOCK_ISSUER = "http://issuer.local"


class MockTokenProvider(JwtTokenProvider):
    scope: list[str] = []
    """IDP token scope"""

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
    return (public_key, private_key)


def encode_token(payload: dict, private_key: rsa.RSAPrivateKey) -> str:
    return jwt.encode(
        payload=payload,
        key=private_key,  # The private key created in the previous step
        algorithm=ALGORITHM,
        headers={
            "kid": PUBLIC_KEY_ID,
        },
    )


def get_mock_user_claims():
    return {
        "sub": "123",
        "iss": MOCK_ISSUER,
        "aud": "audience",  # Should match the audience your app expects
        "iat": 0,  # Issued a long time ago: 1/1/1970
        "exp": 9999999999,  # One long-lasting token, expiring 11/20/2286
    }


def get_jwk(public_key):
    public_numbers = public_key.public_numbers()

    return {
        "kid": PUBLIC_KEY_ID,  # Public key id constant from previous step
        "alg": ALGORITHM,  # Algorithm constant from previous step
        "kty": "RSA",
        "use": "sig",
        "n": to_base64url_uint(public_numbers.n).decode("ascii"),
        "e": to_base64url_uint(public_numbers.e).decode("ascii"),
    }


def get_mock_token(private_key: rsa.RSAPrivateKey) -> str:
    return encode_token(
        get_mock_user_claims(),
        private_key,
    )
