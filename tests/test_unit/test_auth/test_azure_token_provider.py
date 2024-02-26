from odp_sdk.auth import AzureTokenProvider


def test_get_token(azure_token_provider: AzureTokenProvider):
    access_token = azure_token_provider.authenticate()

    assert access_token
    assert access_token["access_token"]
