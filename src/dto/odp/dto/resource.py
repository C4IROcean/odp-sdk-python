from abc import ABC
from typing import Annotated, Generic, Optional, TypeVar
from uuid import UUID

from pydantic import BaseModel
from pydantic.functional_validators import BeforeValidator

from .metadata import Metadata
from .resource_status import ResourceStatus
from .validators import validate_resource_kind, validate_resource_version


class ResourceSpecABC(BaseModel, ABC):
    """ResourceSpecABC is an abstract base class for resource specification."""


T = TypeVar("T", bound=ResourceSpecABC)


class ResourceDto(BaseModel, Generic[T]):
    """Resource Data Transmission Object (DTO) representing a resource manifest"""

    kind: Annotated[str, BeforeValidator(validate_resource_kind)]
    """kind is the kind of the resource."""

    version: Annotated[str, BeforeValidator(validate_resource_version)]
    """version is the version of the resource."""

    metadata: Metadata
    """metadata is the metadata of the resource."""

    status: Optional[ResourceStatus]
    """status is the status of the resource."""

    spec: T

    @property
    def qualified_name(self) -> str:
        return f"{self.kind}/{self.metadata.name}"

    @property
    def uuid(self) -> UUID:
        return self.metadata.uuid
