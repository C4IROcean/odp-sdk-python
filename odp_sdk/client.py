from pydantic import BaseModel, Field, PrivateAttr

from .auth import TokenProvider, get_default_token_provider
from .http_client import OdpHttpClient


class OdpClient(BaseModel):
    """Client for the ODP API"""

    base_url: str = "https://api.hubocean.earth"
    token_provider: TokenProvider = Field(default_factory=get_default_token_provider)

    _http_client: OdpHttpClient = PrivateAttr()

    def __init__(self, **data):
        super().__init__(**data)

        self._http_client = OdpHttpClient(base_url=self.base_url, token_provider=self.token_provider)

    @property
    def resource_store(self):
        # TODO: Implement resource store
        raise NotImplementedError("Resource store not implemented")

    @property
    def catalog(self):
        # TODO: Implement data catalog controller
        raise NotImplementedError("Catalog not implemented")

    @property
    def iam(self):
        # TODO: Implement IAM controller
        raise NotImplementedError("IAM not implemented")

    @property
    def registry(self):
        # TODO: Implement registry/core controller
        raise NotImplementedError("Registry not implemented")

    @property
    def raw(self):
        # TODO: Implement raw storage controller client
        raise NotImplementedError("Raw not implemented")
