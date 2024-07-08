from odp.dto import Metadata, ResourceDto

from .utils import TESTS_RESOURCE_REGISTRY, MockSpec, SimpleSpec

MockDto = ResourceDto[MockSpec]


def test_default_test_resource_registry():
    cls = TESTS_RESOURCE_REGISTRY.get_resource_cls("test.hubocean.io/mock", "v1alpha1")
    assert cls == MockSpec

    cls = TESTS_RESOURCE_REGISTRY.get_resource_cls("test.hubocean.io/simple", "v1alpha1")
    assert cls == SimpleSpec


def test_dunders():
    assert MockSpec.__kind__ == "test.hubocean.io/mock"
    assert MockSpec.__manifest_version__ == "v1alpha1"

    assert SimpleSpec.__kind__ == "test.hubocean.io/simple"
    assert SimpleSpec.__manifest_version__ == "v1alpha1"


def test_init_use_registered_kind_and_version():
    # Users should not need to provide the kind and version for a registered resource kind
    s = MockDto(metadata=Metadata(name="foo"), spec=MockSpec())

    assert s.metadata.name == "foo"
    assert isinstance(s.spec, MockSpec)
