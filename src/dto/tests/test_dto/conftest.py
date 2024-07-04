import pytest
from odp.dto import ResourceRegistry, ResourceRegistryEntry

from .utils import MockSpec, SimpleSpec


@pytest.fixture
def empty_resource_registry() -> ResourceRegistry:
    return ResourceRegistry()


@pytest.fixture
def resource_registry(empty_resource_registry: ResourceRegistry) -> ResourceRegistry:
    empty_resource_registry.add(
        ResourceRegistryEntry(
            resource_kind="test.hubocean.io/mock",
            resource_version="v1alpha1",
        ),
        MockSpec,
    )

    empty_resource_registry.add(
        ResourceRegistryEntry(
            resource_kind="test.hubocean.io/simple",
            resource_version="v1alpha1",
        ),
        SimpleSpec,
    )

    return empty_resource_registry
