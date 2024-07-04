from typing import Annotated, Callable, Dict, Optional, Tuple, Type, TypeVar, cast

from pydantic import BaseModel, Field
from pydantic.functional_validators import BeforeValidator

from .resource import Metadata, ResourceDto, ResourceSpecABC, ResourceSpecT, ResourceStatus, get_resource_spec_type
from .validators import validate_resource_kind, validate_resource_version

T = TypeVar("T", bound=ResourceSpecT)


class ResourceRegistryEntry(BaseModel):
    """ResourceRegistryEntry is a registry entry for a resource."""

    resource_kind: Annotated[str, BeforeValidator(validate_resource_kind)]
    """resource_type is the kind of the resource."""

    resource_version: Annotated[str, BeforeValidator(validate_resource_version)]
    """resource_version is the version of the resource. in the form v<major>(alpha|beta)<minor>"""

    def __hash__(self):
        return hash((self.resource_kind, self.resource_version))


class ResourceRegistry(BaseModel):
    """Registry used to register and lookup resource definitions."""

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

    def factory_cast(self, t: Type[T], kind: str, version: str, data: dict, assert_type: bool = True) -> T:
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
        if assert_type and not isinstance(ret, t):
            raise ValueError(f"Expected type {t.__name__}, got {type(ret).__name__}")
        return cast(T, self.factory(kind, version, data))

    def _resource_factory_prototype(self, manifest: dict) -> Tuple[str, str, Metadata, Optional[ResourceStatus], dict]:
        try:
            kind = manifest["kind"]
            version = manifest["version"]
            metadata = manifest["metadata"]
            status = manifest.get("status")
            spec = manifest["spec"]
        except KeyError as e:
            raise ValueError("Invalid resource manifest") from e

        return (kind, version, Metadata.parse_obj(metadata), ResourceStatus.parse_obj(status) if status else None, spec)

    def resource_factory(self, manifest: dict, raise_unknown: bool = True) -> ResourceDto:
        """Convert a manifest to a ResourceDto object.

        Args:
            manifest: Resource manifest.
            raise_unknown: Whether to raise an exception if the resource kind is unknown.

        Returns:
            Parsed ResourceDto object.
        """
        kind, version, metadata, status, spec_dict = self._resource_factory_prototype(manifest)

        try:
            spec = self.factory(kind, version, spec_dict)
        except KeyError:
            if raise_unknown:
                raise
            spec = spec_dict

        return ResourceDto(kind=kind, version=version, metadata=Metadata.parse_obj(metadata), status=status, spec=spec)

    def resource_factory_cast(
        self, t: Type[ResourceDto[T]], manifest: dict, raise_unknown: bool = True, assert_type: bool = True
    ) -> ResourceDto[T]:
        """Convenience method to create a ResourceDto object and cast it to the given type.

        Args:
            t: Type to cast to.
            manifest: manifest is the resource data.
            raise_unknown: Whether to raise an exception if the resource kind is unknown.
            assert_type: Whether to assert the type before returning
        """
        kind, version, metadata, status, spec_dict = self._resource_factory_prototype(manifest)

        spec_tp = get_resource_spec_type(t)
        try:
            spec = self.factory_cast(spec_tp, kind, version, spec_dict)
        except KeyError:
            if raise_unknown:
                raise
            elif issubclass(spec_tp, ResourceSpecABC):
                spec = spec_tp.parse_obj(spec_dict)
            else:
                spec = spec_dict

        ret = ResourceDto(kind=kind, version=version, metadata=metadata, status=status, spec=spec)
        return cast(ResourceDto[T], ret)


DEFAULT_RESOURCE_REGISTRY = ResourceRegistry()
"""Globally default resource registry."""


def kind(
    resource_group: str,
    resource_type: str,
    resource_version: str,
    registry: ResourceRegistry = DEFAULT_RESOURCE_REGISTRY,
) -> Callable[[Type[ResourceSpecABC]], Type[ResourceSpecABC]]:
    """kind is a decorator for resource specification classes to register them in the resource registry.

    Args:
        resource_group: resource_group is the group of the resource.
        resource_type: resource_type is the kind of the resource.
        resource_version: resource_version is the version of the resource. in the form v<major>(alpha|beta)<minor>
        registry: registry is the resource registry to register the resource in.

    Returns:
        Callable[[Type[ResourceSpecABC]], Type[ResourceSpecABC]]: a decorator function.
    """

    def inner(cls: Type[ResourceSpecABC]) -> Type[ResourceSpecABC]:
        kind = f"{resource_group}/{resource_type}"

        cls.__kind__ = kind
        cls.__manifest_version__ = resource_version

        registry.add(
            ResourceRegistryEntry(resource_kind=kind, resource_version=resource_version),
            cls,
        )

        return cls

    return inner
