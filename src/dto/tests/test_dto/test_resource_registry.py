import pytest
from odp.dto.resource import ResourceSpecABC
from odp.dto.resource_registry import ResourceRegistry, ResourceRegistryEntry


class MockSpec(ResourceSpecABC):
    pass


def test_add_get():
    rg = ResourceRegistry()

    rg.add(
        ResourceRegistryEntry(
            resource_kind="test.hubocean.io/mock",
            resource_version="v1alpha1",
        ),
        MockSpec,
    )

    cls = rg.get_resource_cls("test.hubocean.io/mock", "v1alpha1")
    assert cls == MockSpec


def test_get_nonexistent():
    rg = ResourceRegistry()

    with pytest.raises(KeyError):
        rg.get_resource_cls("test.hubocean.io/mock", "v1alpha1")


def test_get_nonexistent_version():
    rg = ResourceRegistry()

    rg.add(
        ResourceRegistryEntry(
            resource_kind="test.hubocean.io/mock",
            resource_version="v1alpha1",
        ),
        MockSpec,
    )

    with pytest.raises(KeyError):
        rg.get_resource_cls("test.hubocean.io/mock", "v1alpha2")


def test_add_duplicate():
    rg = ResourceRegistry()

    rg.add(
        ResourceRegistryEntry(
            resource_kind="test.hubocean.io/mock",
            resource_version="v1alpha1",
        ),
        MockSpec,
    )

    with pytest.raises(ValueError):
        rg.add(
            ResourceRegistryEntry(
                resource_kind="test.hubocean.io/mock",
                resource_version="v1alpha1",
            ),
            MockSpec,
        )
