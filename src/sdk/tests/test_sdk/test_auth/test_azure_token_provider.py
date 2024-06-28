import responses
from odp_sdk.auth import AzureTokenProvider


def test_get_token(azure_token_provider: AzureTokenProvider, mock_token_response_body: str):
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            azure_token_provider.token_uri,
            body=mock_token_response_body,
        )
        access_token = azure_token_provider.get_token()
        assert access_token

        new_access_token = azure_token_provider.get_token()
        assert access_token == new_access_token

        assert rsps.assert_call_count(azure_token_provider.token_uri, 1)
