import responses

from odp_sdk.auth import AzureTokenProvider


def test_get_token(azure_token_provider: AzureTokenProvider):
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            azure_token_provider.token_uri,
            json={"access_token": "test"},
        )
        access_token = azure_token_provider.authenticate()

        assert access_token
        assert access_token["access_token"]

        assert rsps.assert_call_count(azure_token_provider.token_uri, 1)
