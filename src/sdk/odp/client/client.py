from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field, PrivateAttr

from .auth import TokenProvider, get_default_token_provider
from .http_client import OdpHttpClient
from .raw_storage_client import OdpRawStorageClient
from .resource_client import OdpResourceClient
from .tabular_storage_client import OdpTabularStorageClient


class OdpClient(BaseModel):
    """Client for the ODP API"""

    base_url: str = "https://api.hubocean.earth"
    token_provider: TokenProvider = Field(default_factory=get_default_token_provider)

    _http_client: OdpHttpClient = PrivateAttr()
    _catalog_client: OdpResourceClient = PrivateAttr()
    _raw_storage_client: OdpRawStorageClient = PrivateAttr()
    _tabular_storage_client: OdpTabularStorageClient = PrivateAttr()

    def __init__(self, **data):
        super().__init__(**data)

        self._http_client = OdpHttpClient(base_url=self.base_url, token_provider=self.token_provider)
        self._catalog_client = OdpResourceClient(http_client=self._http_client, resource_endpoint="/catalog")
        self._raw_storage_client = OdpRawStorageClient(http_client=self._http_client)
        self._tabular_storage_client = OdpTabularStorageClient(http_client=self._http_client)

    def personalize_name(self, name: str, fmt: Optional[str] = None) -> str:
        """Personalize a name by adding a postfix unique to the user

        Args:
            name: The name to personalize
            fmt: Used to override the default format string. Should be a python format-string with placeholders
                for the variables `uid` and `name`. For example: `"{uid}-{name}"`

        Returns:
            The personalized name
        """
        fmt = fmt or "{name}-{uid}"
        uid = self.token_provider.get_user_id()

        # Attempt to simplify the UID by only using the node part of the UUID
        try:
            uid = UUID(uid).node
        except ValueError:
            # User ID is not a valid UUID, use it as-is
            pass

        return fmt.format(uid=uid, name=name)

    @property
    def resource_store(self):
        # TODO: Implement resource store
        raise NotImplementedError("Resource store not implemented")

    @property
    def catalog(self) -> OdpResourceClient:
        return self._catalog_client

    @property
    def iam(self):
        # TODO: Implement IAM controller
        raise NotImplementedError("IAM not implemented")

    @property
    def registry(self):
        # TODO: Implement registry/core controller
        raise NotImplementedError("Registry not implemented")

    @property
    def raw(self) -> OdpRawStorageClient:
        return self._raw_storage_client

    @property
    def tabular(self) -> OdpTabularStorageClient:
        return self._tabular_storage_client
