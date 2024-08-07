from odp.client.auth import OdpWorkspaceTokenProvider


def test_get_token(odp_workspace_token_provider: OdpWorkspaceTokenProvider):
    access_token = odp_workspace_token_provider.get_token()

    assert access_token
    assert access_token.startswith("Bearer")
