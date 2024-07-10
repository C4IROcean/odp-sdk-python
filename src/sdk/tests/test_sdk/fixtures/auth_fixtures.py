import json
import random
import time

import jwt
import pytest
import responses
from cryptography.hazmat.primitives.asymmetric import rsa
from odp.client.auth import AzureTokenProvider, OdpWorkspaceTokenProvider
from pydantic import SecretStr

__all__ = ["odp_workspace_token_provider", "azure_token_provider", "mock_token_response_body"]

ALGORITHM = "RS256"
PUBLIC_KEY_ID = "sample-key-id"

MOCK_SIDECAR_URL = "http://token_endpoint.local"
MOCK_CLIENT_ID = SecretStr("foo")
MOCK_CLIENT_SECRET = SecretStr("bar")
MOCK_TOKEN_URI = "http://token_uri.local"
MOCK_ISSUER = "http://issuer.local"
MOCK_AUDIENCE = "audience"


@pytest.fixture()
def odp_workspace_token_provider() -> OdpWorkspaceTokenProvider:
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            MOCK_SIDECAR_URL,
            json={"token": "test"},
        )

        yield OdpWorkspaceTokenProvider(token_uri=MOCK_SIDECAR_URL)


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
def mock_token_response_body(rsa_private_key) -> str:
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

    return json.dumps(
        {
            "access_token": token,
        }
    )


@pytest.fixture()
def azure_token_provider() -> AzureTokenProvider:
    return AzureTokenProvider(
        client_id=MOCK_CLIENT_ID,
        client_secret=MOCK_CLIENT_SECRET,
        token_uri=MOCK_TOKEN_URI,
    )
