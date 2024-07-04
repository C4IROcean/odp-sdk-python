import re
from typing import Any, Dict, Iterable, List, Optional, Tuple, Type, TypeVar, Union
from uuid import UUID

import requests
from odp.dto import DEFAULT_RESOURCE_REGISTRY, ResourceDto, ResourceRegistry, ResourceSpecT, get_resource_spec_type
from pydantic import BaseModel, field_validator

from .exc import OdpResourceExistsError, OdpResourceNotFoundError, OdpValidationError
from .http_client import OdpHttpClient

T = TypeVar("T", bound=ResourceSpecT)


class OdpResourceClient(BaseModel):
    """Client for interacting with ODP resources."""

    http_client: OdpHttpClient
    resource_endpoint: str
    resource_registry: ResourceRegistry = DEFAULT_RESOURCE_REGISTRY

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

    def get(
        self,
        ref: Union[UUID, str],
        tp: Optional[Type[ResourceDto[T]]] = None,
        assert_type: bool = False,
        raise_unknown_kind: bool = False,
    ) -> ResourceDto[T]:
        """Get a resource by reference.

        The reference can be either a UUID or a qualified name.

        Args:
            ref: Resource reference
            tp: Optionally cast the fetched resource to a specific type
            assert_type: Whether to assert that the fetched resource is of the expected type, must be used with `tp`
            raise_unknown_kind: Whether to raise an error if the kind of the resource is not known

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

        if not tp:
            return self.resource_registry.resource_factory(res.json(), raise_unknown_kind)

        return self.resource_registry.resource_factory_cast(tp, res.json(), assert_type, raise_unknown_kind)

    def list(
        self,
        oqs_filter: Optional[Dict[str, Any]] = None,
        cursor: Optional[str] = None,
        tp: Optional[Type[ResourceDto[T]]] = None,
        assert_type: bool = False,
        raise_unknown_kind: bool = False,
    ) -> Iterable[ResourceDto[T]]:
        """List all resources based on the provided filter

        Args:
            oqs_filter: OQS filter
            cursor: Optional cursor for pagination
            tp: Optionally cast the fetched resource to a specific type
            assert_type: Whether to assert that the fetched resource is of the expected type, must be used with `tp`
            raise_unknown_kind: Whether to raise an error if the kind of the resource is not known

        Yields:
            Resources matching the provided filter
        """
        while True:
            page, cursor = self.list_paginated(
                oqs_filter=oqs_filter,
                cursor=cursor,
                tp=tp,
                assert_type=assert_type,
                raise_unknown_kind=raise_unknown_kind,
            )

            yield from page
            if not cursor:
                break

    def list_paginated(
        self,
        oqs_filter: Optional[dict] = None,
        cursor: Optional[str] = None,
        limit: int = 1000,
        tp: Optional[Type[ResourceDto[T]]] = None,
        assert_type: bool = False,
        raise_unknown_kind: bool = False,
    ) -> Tuple[List[ResourceDto[T]], Optional[str]]:
        """List a page of resources based on the provided filter

        Args:
            oqs_filter: OQS filter
            cursor: Cursor for pagination
            limit: Maximum number of resources to return
            tp: Optionally cast the fetched resource to a specific type
            assert_type: Whether to assert that the fetched resource is of the expected type, must be used with `tp`
            raise_unknown_kind: Whether to raise an error if the kind of the resource is not known

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

        if not tp:
            ret = [self.resource_registry.resource_factory(item, raise_unknown_kind) for item in content["results"]]
        else:
            ret = [
                self.resource_registry.resource_factory_cast(tp, item, assert_type, raise_unknown_kind)
                for item in content["results"]
            ]

        return ret, content.get("next")

    def create(
        self,
        manifest: ResourceDto[T],
        assert_type: bool = False,
        raise_unknown_kind: bool = False,
    ) -> ResourceDto[T]:
        """Create a resource from a manifest

        Args:
            manifest: Resource manifest
            assert_type: Whether to assert the type of the object returned by the API
            raise_unknown_kind: Whether to raise an error if the kind of the resource is not known

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

        return self.resource_registry.resource_factory_cast(
            ResourceDto[get_resource_spec_type(manifest)], res.json(), assert_type, raise_unknown_kind
        )

    def update(
        self,
        manifest_update: Union[ResourceDto[T], dict],
        ref: Union[str, UUID, None] = None,
        tp: Optional[Type[ResourceDto[T]]] = None,
        assert_type: bool = False,
        raise_unknown_kind: bool = False,
    ) -> ResourceDto:
        """Update a resource from a manifest

        Args:
            manifest_update: Resource manifest or JSON patch
            ref: Optional reference to the resource to update.
            tp: Optionally cast the fetched resource to a specific type. Can only be used if `ref` is a UUID.
            assert_type: Whether to assert the type of the object returned by the API
            raise_unknown_kind: Whether to raise an error if the kind of the resource is not known

        Returns:
            The manifest of the updated resource, populated with the updated fields

        Raises:
            OdpValidationError: Invalid input
            OdpResourceNotFoundError: Resource not found
        """
        if isinstance(manifest_update, ResourceDto) and tp:
            raise ValueError("Cannot cast the updated resource to a specific type if the manifest is a ResourceDto")
        elif isinstance(manifest_update, ResourceDto):
            tp = ResourceDto[get_resource_spec_type(manifest_update)]

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

        if tp:
            return self.resource_registry.resource_factory_cast(tp, res.json(), assert_type, raise_unknown_kind)
        return self.resource_registry.resource_factory(res.json(), raise_unknown_kind)

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
