import re
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union
from uuid import UUID

import requests
from pydantic import BaseModel, field_validator

from .dto import ResourceDto
from .exc import OdpResourceExistsError, OdpResourceNotFoundError, OdpValidationError
from .http_client import OdpHttpClient


class OdpResourceClient(BaseModel):
    """Client for interacting with ODP resources."""

    http_client: OdpHttpClient
    resource_endpoint: str

    @field_validator("resource_endpoint")
    @classmethod
    def _validate_resource_endpoint(cls, v: str) -> str:
        m = re.match(r"^\/[/.a-zA-Z0-9-]+$", v)
        if not m:
            raise ValueError(f"Invalid resource endpoint: {v}")

        return v

    @property
    def resource_url(self) -> str:
        """The URL of the resource endpoint, including the base URL.

        Returns:
            The resource URL
        """
        return f"{self.http_client.base_url}{self.resource_endpoint}"

    def get(self, ref: Union[UUID, str]) -> ResourceDto:
        """Get a resource by reference.

        The reference can be either a UUID or a qualified name.

        Args:
            ref: Resource reference

        Returns:
            The manifest of the resource corresponding to the reference

        Raises:
            OdpResourceNotFoundError: If the resource does not exist
            OdpValidationError: Invalid input
        """
        res = self.http_client.get(f"{self.resource_endpoint}/{ref}")

        try:
            res.raise_for_status()
        except requests.HTTPError as e:
            if res.status_code == 400:
                raise OdpValidationError("Invalid input") from e
            if res.status_code == 404:
                raise OdpResourceNotFoundError(f"Resource not found: {ref}") from e
            raise requests.HTTPError(f"HTTP Error - {res.status_code}: {res.text}")

        return ResourceDto(**res.json())

    def list(self, oqs_filter: Optional[Dict[str, Any]] = None, cursor: Optional[str] = None) -> Iterable[ResourceDto]:
        """List all resources based on the provided filter

        Args:
            oqs_filter: OQS filter
            cursor: Optional cursor for pagination

        Yields:
            Resources matching the provided filter
        """
        while True:
            page, cursor = self.list_paginated(oqs_filter=oqs_filter, cursor=cursor)
            yield from page
            if not cursor:
                break

    def list_paginated(
        self,
        oqs_filter: Optional[dict] = None,
        cursor: Optional[str] = None,
        limit: int = 1000,
    ) -> Tuple[List[ResourceDto], str]:
        """List a page of resources based on the provided filter

        Args:
            oqs_filter: OQS filter
            cursor: Cursor for pagination
            limit: Maximum number of resources to return

        Returns:
            A page of resources

        Raises:
            OdpValidationError: Invalid input
        """
        params = {}
        body = None

        if cursor:
            params["page"] = cursor
        if limit:
            params["page_size"] = limit

        if oqs_filter:
            body = oqs_filter

        res = self.http_client.post(self.resource_endpoint + "/list", params=params, content=body)
        try:
            res.raise_for_status()
        except requests.HTTPError as e:
            if res.status_code == 401:
                raise OdpValidationError("API argument error") from e
            raise requests.HTTPError(f"HTTP Error - {res.status_code}: {res.text}")

        content = res.json()
        return [ResourceDto(**item) for item in content["results"]], content.get("next")

    def create(self, manifest: ResourceDto) -> ResourceDto:
        """Create a resource from a manifest

        Args:
            manifest: Resource manifest

        Returns:
            The manifest of the created resource, populated with uuid and status

        Raises:
            OdpValidationError: Invalid input
            OdpResourceExistsError: Resource already exists
        """

        res = self.http_client.post(self.resource_endpoint, content=manifest)
        try:
            res.raise_for_status()
        except requests.HTTPError as e:
            if res.status_code == 400:
                raise OdpValidationError("Invalid input", res.text) from e
            if res.status_code == 409:
                raise OdpResourceExistsError("Resource already exists") from e
            raise requests.HTTPError(f"HTTP Error - {res.status_code}: {res.text}")

        return ResourceDto(**res.json())

    def update(self, manifest_update: Union[ResourceDto, dict], ref: Union[str, UUID, None] = None) -> ResourceDto:
        """Update a resource from a manifest

        Args:
            manifest_update: Resource manifest or JSON patch
            ref: Optional reference to the resource to update.

        Returns:
            The manifest of the updated resource, populated with the updated fields

        Raises:
            OdpValidationError: Invalid input
            OdpResourceNotFoundError: Resource not found
        """
        if ref:
            if isinstance(ref, UUID):
                params = {"either_id": str(ref)}
            else:
                rg, rt, name = ref.split("/")
                params = {"either_id": name, "kind": f"{rg}/{rt}"}
        else:
            params = {}

        res = self.http_client.patch(self.resource_endpoint, params=params, content=manifest_update)
        try:
            res.raise_for_status()
        except requests.HTTPError as e:
            if res.status_code == 400:
                raise OdpValidationError("Invalid input") from e
            if res.status_code == 404:
                raise OdpResourceNotFoundError("Resource not found") from e
            raise requests.HTTPError(f"HTTP Error - {res.status_code}: {res.text}")

        return ResourceDto(**res.json())

    def delete(self, ref: Union[UUID, str, ResourceDto]):
        """Delete a resource by reference.

        Args:
            ref: Resource reference. If a `ResourceDto` is passed, the reference will be extracted from the metadata.

        Raises:
            OdpResourceNotFoundError: If the resource does not exist
        """
        if isinstance(ref, ResourceDto):
            ref = ref.metadata.uuid or f"{ref.kind}/{ref.metadata.name}"

        res = self.http_client.delete(f"{self.resource_endpoint}/{ref}")
        try:
            res.raise_for_status()
        except requests.HTTPError as e:
            if res.status_code == 404:
                raise OdpResourceNotFoundError(f"Resource not found: {ref}") from e
            raise requests.HTTPError(f"HTTP Error - {res.status_code}: {res.text}")
