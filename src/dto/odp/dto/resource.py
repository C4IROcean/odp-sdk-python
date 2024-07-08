from abc import ABC
from typing import Annotated, ClassVar, Generic, Optional, Type, TypeVar, Union, cast
from uuid import UUID

from pydantic import BaseModel
from pydantic.functional_validators import BeforeValidator

from .metadata import Metadata
from .resource_status import ResourceStatus
from .validators import validate_resource_kind, validate_resource_version


class ResourceSpecABC(BaseModel, ABC):
    """ResourceSpecABC is an abstract base class for resource specification."""

    __kind__: ClassVar[str]
    __manifest_version__: ClassVar[str]


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

    def __init__(self, **data):
        spec = data.pop("spec")

        if hasattr(spec, "__kind__") and "kind" not in data:
            data["kind"] = spec.__kind__
        if hasattr(spec, "__manifest_version__") and "version" not in data:
            data["version"] = spec.__manifest_version__

        super().__init__(**data, spec=spec)

    @classmethod
    def is_generic(cls) -> bool:
        return isinstance(get_resource_spec_type(cls), dict)

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


def get_resource_spec_type(cls: Union[Type[ResourceDto[T]], ResourceDto[T]]) -> Type[T]:
    """Get the resource spec type

    Args:
        cls: ResourceDto class or instance

    Returns:
        The resource spec type
    """
    if isinstance(cls, type) and issubclass(cls, ResourceDto):
        tp = cls.model_fields["spec"].annotation
    else:
        tp = type(cls.spec)
    return cast(Type[T], tp)


GenericResourceDto = ResourceDto[dict]
