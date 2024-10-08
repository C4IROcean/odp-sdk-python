import os
import uuid
from typing import Callable, Tuple

import pytest
from dotenv import load_dotenv
from odp.client import OdpClient
from odp.client.auth import AzureTokenProvider
from odp.client.exc import OdpResourceNotFoundError
from pydantic import SecretStr


@pytest.fixture(scope="session")
def dotenv() -> None:
    load_dotenv()


@pytest.fixture(scope="session")
def token_provider(dotenv) -> AzureTokenProvider:
    return AzureTokenProvider(
        authority=os.getenv(
            "ODCAT_AUTH_AUTHORITY",
            "https://oceandataplatform.b2clogin.com/755f6e58-74f0-4a07-a599-f7479b9669ab/v2.0/",
        ),
        client_id=SecretStr(os.getenv("ODCAT_AUTH_CLIENT_ID")),
        client_secret=SecretStr(os.getenv("ODCAT_AUTH_CLIENT_SECRET")),
        audience=os.getenv("ODCAT_AUTH_AUDIENCE", "a2e4df44-ed57-4673-8824-548256b92543"),
        tenant_id=os.getenv("ODCAT_AUTH_TENANT_ID", "755f6e58-74f0-4a07-a599-f7479b9669ab"),
        token_uri=os.getenv(
            "ODCAT_AUTH_TOKEN_ENDPOINT",
            "https://oceandataplatform.b2clogin.com/oceandataplatform.onmicrosoft.com/b2c_1a_signup_signin_custom/oauth2/v2.0/token",  # noqa: E501
        ),
        jwks_uri=os.getenv(
            "ODCAT_AUTH_JWKS_URI",
            "https://oceandataplatform.b2clogin.com/oceandataplatform.onmicrosoft.com/b2c_1a_signup_signin_custom/discovery/v2.0/keys",  # noqa: E501
        ),
        scope=[os.getenv("ODCAT_AUTH_SCOPE", "https://oceandataplatform.onmicrosoft.com/odcat/.default")],
    )


@pytest.fixture(scope="session")
def odp_client(token_provider: AzureTokenProvider) -> OdpClient:
    base_url = os.getenv("ODCAT_BASE_URL", "https://api.hubocean.earth")

    return OdpClient(
        base_url=base_url,
        token_provider=token_provider,
    )


def delete_element(func: Callable, *args, **kwargs) -> None:
    try:
        func(*args, **kwargs)
    except OdpResourceNotFoundError:
        pass


@pytest.fixture
def odp_client_test_uuid(odp_client: OdpClient) -> Tuple[OdpClient, uuid.UUID]:
    test_uuid = uuid.uuid4()
    yield odp_client, test_uuid

    # Clean up
    for manifest in odp_client.catalog.list({"#EQUALS": ["$metadata.labels.test_uuid", str(test_uuid)]}):
        storage_class = getattr(manifest.spec, "storage_class", "")
        if "raw" in storage_class:
            for file in odp_client.raw.list(manifest):
                delete_element(odp_client.raw.delete_file, manifest, file)
                if os.path.exists(os.path.basename(file.name)):
                    os.remove(os.path.basename(file.name))
        if "tabular" in storage_class:
            delete_element(odp_client.tabular.delete_schema, manifest, True)
        delete_element(odp_client.catalog.delete, manifest.metadata.uuid)
