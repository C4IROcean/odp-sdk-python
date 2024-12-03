from odp.client.auth import TokenProvider
from odp.client.tabular_v2.client import Client


class ClientAuthorization(Client):
    def __init__(self, base_url, token_provider: TokenProvider):
        if base_url.endswith(":8888"):
            base_url = base_url.replace(":8888", ":31337")
        super().__init__(base_url)
        self.token_provider = token_provider

    def _request(
            self,
            path: str,
            data: dict | bytes | None = None,
            params: dict | None = None,
            headers: dict | None = None,
    ) -> Client.Response:
        headers = headers or {}
        headers["Authorization"] = self.token_provider.get_token()
        return super()._request(path, data, params, headers)