from abc import ABC
from typing import Annotated, Generic, Optional, Type, TypeVar, Union, cast
from uuid import UUID

from pydantic import BaseModel
from pydantic.functional_validators import BeforeValidator

from .metadata import Metadata
from .resource_status import ResourceStatus
from .validators import validate_resource_kind, validate_resource_version


class ResourceSpecABC(BaseModel, ABC):
    """ResourceSpecABC is an abstract base class for resource specification."""


ResourceSpecT = Union[dict, ResourceSpecABC]

T = TypeVar("T", bound=ResourceSpecT)


class ResourceDto(BaseModel, Generic[T]):
    """Resource Data Transmission Object (DTO) representing a resource manifest"""

    kind: Annotated[str, BeforeValidator(validate_resource_kind)]
    """kind is the kind of the resource."""

    version: Annotated[str, BeforeValidator(validate_resource_version)]
    """version is the version of the resource."""

    metadata: Metadata
    """metadata is the metadata of the resource."""

    status: Optional[ResourceStatus] = None
    """status is the status of the resource."""

    spec: T

    @classmethod
    @property
    def spec_tp(cls) -> Type[T]:
        tp = cls.model_fields["spec"].annotation
        return cast(Type[T], tp)

    @classmethod
    def is_generic(cls) -> bool:
        return isinstance(cls.spec_tp, dict)

    @property
    def qualified_name(self) -> str:
        return self.get_qualified_name()

    @property
    def uuid(self) -> UUID:
        return self.get_uuid()

    def get_qualified_name(self) -> str:
        """Get the resource qualified name

        The qualified name is the kind and resource name joined by a slash: `{kind}/{metadata.name}`

        Returns:
            Qualified name
        """
        return f"{self.kind}/{self.metadata.name}"

    def get_uuid(self) -> Optional[UUID]:
        """Get the resource UUID

        Returns:
            Resource UUID if it is set, `None` otherwise
        """
        return self.metadata.uuid

    def get_ref(self) -> Union[UUID, str]:
        """Get a valid reference to the resource

        Returns:
            The resource UUID if it is set, the qualified name otherwise
        """
        return self.get_uuid() or self.get_qualified_name()


GenericResourceDto = ResourceDto[dict]
