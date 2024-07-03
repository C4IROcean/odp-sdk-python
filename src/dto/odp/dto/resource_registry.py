from typing import Annotated, Callable, Dict, Type, TypeVar, cast

from pydantic import BaseModel, Field
from pydantic.functional_validators import BeforeValidator

from .resource import ResourceSpecABC
from .validators import validate_resource_kind, validate_resource_version

T = TypeVar("T", bound=ResourceSpecABC)


class ResourceRegistryEntry(BaseModel):
    """ResourceRegistryEntry is a registry entry for a resource."""

    resource_kind: Annotated[str, BeforeValidator(validate_resource_kind)]
    """resource_type is the kind of the resource."""

    resource_version: Annotated[str, BeforeValidator(validate_resource_version)]
    """resource_version is the version of the resource. in the form v<major>(alpha|beta)<minor>"""

    def __hash__(self):
        return hash((self.resource_kind, self.resource_version))


class ResourceRegistry(BaseModel):
    entries: Dict[ResourceRegistryEntry, Type[ResourceSpecABC]] = Field(default_factory=dict)
    """entries is a list of resource registry entries."""

    def add(self, entry: ResourceRegistryEntry, spec: Type[ResourceSpecABC]) -> None:
        """add adds a resource to the registry."""
        if entry in self.entries:
            raise ValueError(f"resource {entry.resource_kind} ({entry.resource_version}) already exists")
        self.entries[entry] = spec

    def get_resource_cls(self, kind: str, version: str) -> Type[ResourceSpecABC]:
        """Returns the resource spec class for the given kind and version.

        Args:
            kind: kind is the kind of the resource.
            version: version is the version of the resource.

        Returns:
            Type[ResourceSpecABC]: the resource spec class.
        """
        entry = ResourceRegistryEntry(resource_kind=kind, resource_version=version)
        try:
            return self.entries[entry]
        except KeyError as e:
            raise KeyError(f"resource {kind} ({version}) not found") from e

    def factory(self, kind: str, version: str, data: dict) -> ResourceSpecABC:
        """factory creates a resource spec object for the given kind and version.

        Args:
            kind: kind is the kind of the resource.
            version: version is the version of the resource.
            data: data is the resource data.

        Returns:
            ResourceSpecABC: the resource spec object.
        """
        cls = self.get_resource_cls(kind, version)
        return cls(**data)

    def factory_cast(self, t: T, kind: str, version: str, data: dict, assert_type: bool = True) -> T:
        """Convenience method to create a resource spec object and cast it to the given type.

        Args:
            t: Type to cast to.
            kind: kind is the kind of the resource.
            version: version is the version of the resource.
            data: data is the resource data.
            assert_type: Whether to assert the type before returning

        Returns:
            T: the resource spec object.
        """
        ret = self.factory(kind, version, data)
        if assert_type and not isinstance(ret, type(t)):
            raise ValueError(f"Expected type {type(t).__name__}, got {type(ret).__name__}")
        return cast(T, self.factory(kind, version, data))


DEFAULT_RESOURCE_REGISTRY = ResourceRegistry()
"""Globally default resource registry."""


def kind(
    resource_group: str, resource_type: str, resource_version: str
) -> Callable[[Type[ResourceSpecABC]], Type[ResourceSpecABC]]:
    """kind is a decorator for resource specification classes to register them in the resource registry.

    Args:
        resource_group: resource_group is the group of the resource.
        resource_type: resource_type is the kind of the resource.
        resource_version: resource_version is the version of the resource. in the form v<major>(alpha|beta)<minor>

    Returns:
        Callable[[Type[ResourceSpecABC]], Type[ResourceSpecABC]]: a decorator function.
    """

    def inner(cls: Type[ResourceSpecABC]) -> Type[ResourceSpecABC]:
        DEFAULT_RESOURCE_REGISTRY.add(
            ResourceRegistryEntry(resource_kind=f"{resource_group}/{resource_type}", resource_version=resource_version),
            cls,
        )

        return cls

    return inner
