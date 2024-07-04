import pytest
from odp.dto.resource import Metadata, ResourceDto
from odp.dto.resource_registry import ResourceRegistry, ResourceRegistryEntry

from .utils import MockSpec, SimpleSpec, UnregisteredSpec


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


def test_get_resource_cls(resource_registry: ResourceRegistry):
    cls = resource_registry.get_resource_cls("test.hubocean.io/mock", "v1alpha1")
    assert cls == MockSpec


def test_get_resource_cls_nonexistent(resource_registry: ResourceRegistry):
    with pytest.raises(KeyError):
        resource_registry.get_resource_cls("test.hubocean.io/mock", "v1alpha2")


def test_factory(resource_registry: ResourceRegistry):
    data = {
        "some_str": "foo",
        "some_int": 42,
    }
    obj = resource_registry.factory("test.hubocean.io/simple", "v1alpha1", data)
    assert isinstance(obj, SimpleSpec)

    assert obj.some_str == data["some_str"]
    assert obj.some_int == data["some_int"]


def test_factory_nonexistent(resource_registry: ResourceRegistry):
    with pytest.raises(KeyError):
        resource_registry.factory(
            "test.hubocean.io/mock",
            "v1alpha2",
            {
                "some_str": "foo",
                "some_int": 42,
            },
        )


def test_factory_bad_schema(resource_registry: ResourceRegistry):
    with pytest.raises(ValueError):
        resource_registry.factory(
            "test.hubocean.io/simple",
            "v1alpha1",
            {
                "some_str": "foo",
            },
        )


def test_factory_validation_error(resource_registry: ResourceRegistry):
    with pytest.raises(ValueError):
        resource_registry.factory(
            "test.hubocean.io/simple",
            "v1alpha1",
            {
                "some_str": "foo",
                "some_int": 0,
            },
        )


def test_factory_cast(resource_registry: ResourceRegistry):
    data = {
        "some_str": "foo",
        "some_int": 42,
    }
    obj = resource_registry.factory_cast(SimpleSpec, "test.hubocean.io/simple", "v1alpha1", data)
    assert isinstance(obj, SimpleSpec)

    assert obj.some_str == data["some_str"]
    assert obj.some_int == data["some_int"]


def test_factory_cast_no_assertion(resource_registry: ResourceRegistry):
    data = {
        "some_str": "foo",
        "some_int": 42,
    }
    obj = resource_registry.factory_cast(SimpleSpec, "test.hubocean.io/simple", "v1alpha1", data, assert_type=False)
    assert isinstance(obj, SimpleSpec)

    assert obj.some_str == data["some_str"]
    assert obj.some_int == data["some_int"]


def test_factory_cast_wrong_type(resource_registry: ResourceRegistry):
    with pytest.raises(ValueError):
        resource_registry.factory_cast(
            MockSpec,
            "test.hubocean.io/simple",
            "v1alpha1",
            {
                "some_str": "foo",
                "some_int": 42,
            },
        )


def test_factory_cast_nonexistent(resource_registry: ResourceRegistry):
    with pytest.raises(KeyError):
        resource_registry.factory_cast(
            SimpleSpec,
            "test.hubocean.io/simple_nonexistent",
            "v1alpha2",
            {
                "some_str": "foo",
                "some_int": 42,
            },
        )


def test_factory_cast_validation_error(resource_registry: ResourceRegistry):
    with pytest.raises(ValueError):
        resource_registry.factory_cast(
            SimpleSpec,
            "test.hubocean.io/simple",
            "v1alpha1",
            {
                "some_str": "foo",
                "some_int": 0,
            },
        )


def test_resource_factory(resource_registry: ResourceRegistry):
    manifest = {
        "kind": "test.hubocean.io/simple",
        "version": "v1alpha1",
        "metadata": {
            "name": "foobar",
        },
        "spec": {
            "some_str": "foo",
            "some_int": 42,
        },
    }

    obj = resource_registry.resource_factory(manifest)
    assert isinstance(obj, ResourceDto)
    assert isinstance(obj.metadata, Metadata)
    assert isinstance(obj.spec, SimpleSpec)
    assert obj.status is None
    assert obj.metadata.name == manifest["metadata"]["name"]
    assert obj.spec.some_str == manifest["spec"]["some_str"]
    assert obj.spec.some_int == manifest["spec"]["some_int"]


def test_resource_factory_nonexistent(resource_registry: ResourceRegistry):
    manifest = {
        "kind": "test.hubocean.io/simple_nonexistent",
        "version": "v1alpha1",
        "metadata": {
            "name": "foobar",
        },
        "spec": {
            "some_str": "foo",
            "some_int": 42,
        },
    }

    with pytest.raises(KeyError):
        resource_registry.resource_factory(manifest)


def test_resource_factory_nonexistent_generic_fallback(resource_registry: ResourceRegistry):
    manifest = {
        "kind": "test.hubocean.io/simple_nonexistent",
        "version": "v1alpha1",
        "metadata": {
            "name": "foobar",
        },
        "spec": {
            "some_str": "foo",
            "some_int": 42,
        },
    }

    obj = resource_registry.resource_factory(manifest, raise_unknown=False)
    assert isinstance(obj, ResourceDto)
    assert isinstance(obj.spec, dict)
    assert obj.metadata.name == manifest["metadata"]["name"]
    assert obj.spec == manifest["spec"]


def test_resource_factory_invalid_manifest(resource_registry: ResourceRegistry):
    manifest = {
        "kind": "test.hubocean.io/simple",
        "version": "v1alpha1",
        "metadata": {
            "name": "foobar",
        },
    }

    with pytest.raises(ValueError):
        resource_registry.resource_factory(manifest)


def test_resource_factory_validation_error(resource_registry: ResourceRegistry):
    manifest = {
        "kind": "test.hubocean.io/simple",
        "version": "v1alpha1",
        "metadata": {
            "name": "foobar",
        },
        "spec": {
            "some_str": "foo",
            "some_int": 0,
        },
    }

    with pytest.raises(ValueError):
        resource_registry.resource_factory(manifest)


def test_resource_factory_cast(resource_registry: ResourceRegistry):
    manifest = {
        "kind": "test.hubocean.io/simple",
        "version": "v1alpha1",
        "metadata": {
            "name": "foobar",
        },
        "spec": {
            "some_str": "foo",
            "some_int": 42,
        },
    }

    obj = resource_registry.resource_factory_cast(ResourceDto[SimpleSpec], manifest)

    assert isinstance(obj, ResourceDto)
    assert isinstance(obj.metadata, Metadata)
    assert isinstance(obj.spec, SimpleSpec)
    assert obj.status is None
    assert obj.metadata.name == manifest["metadata"]["name"]
    assert obj.spec.some_str == manifest["spec"]["some_str"]
    assert obj.spec.some_int == manifest["spec"]["some_int"]


def test_resource_factory_cast_no_assertion(resource_registry: ResourceRegistry):
    manifest = {
        "kind": "test.hubocean.io/simple",
        "version": "v1alpha1",
        "metadata": {
            "name": "foobar",
        },
        "spec": {
            "some_str": "foo",
            "some_int": 42,
        },
    }

    obj = resource_registry.resource_factory_cast(ResourceDto[SimpleSpec], manifest, assert_type=False)

    assert isinstance(obj, ResourceDto)
    assert isinstance(obj.metadata, Metadata)
    assert isinstance(obj.spec, SimpleSpec)
    assert obj.status is None
    assert obj.metadata.name == manifest["metadata"]["name"]
    assert obj.spec.some_str == manifest["spec"]["some_str"]
    assert obj.spec.some_int == manifest["spec"]["some_int"]


def test_resource_factory_cast_wrong_type(resource_registry: ResourceRegistry):
    manifest = {
        "kind": "test.hubocean.io/simple",
        "version": "v1alpha1",
        "metadata": {
            "name": "foobar",
        },
        "spec": {
            "some_str": "foo",
            "some_int": 42,
        },
    }

    with pytest.raises(ValueError):
        resource_registry.resource_factory_cast(ResourceDto[MockSpec], manifest)


def test_resource_factory_cast_nonexistent(resource_registry: ResourceRegistry):
    manifest = {
        "kind": "test.hubocean.io/simple_nonexistent",
        "version": "v1alpha1",
        "metadata": {
            "name": "foobar",
        },
        "spec": {
            "some_str": "foo",
            "some_int": 42,
        },
    }

    with pytest.raises(KeyError):
        resource_registry.resource_factory_cast(ResourceDto[SimpleSpec], manifest)


def test_resource_factory_cast_validation_error(resource_registry: ResourceRegistry):
    manifest = {
        "kind": "test.hubocean.io/simple",
        "version": "v1alpha1",
        "metadata": {
            "name": "foobar",
        },
        "spec": {
            "some_str": "foo",
            "some_int": 0,
        },
    }

    with pytest.raises(ValueError):
        resource_registry.resource_factory_cast(ResourceDto[SimpleSpec], manifest)


def test_resource_factory_cast_nonexistent_generic_fallback(resource_registry: ResourceRegistry):
    manifest = {
        "kind": "test.hubocean.io/simple_nonexistent",
        "version": "v1alpha1",
        "metadata": {
            "name": "foobar",
        },
        "spec": {
            "some_str": "foo",
            "some_int": 42,
        },
    }

    obj = resource_registry.resource_factory_cast(ResourceDto[dict], manifest, raise_unknown=False)

    assert isinstance(obj, ResourceDto)
    assert isinstance(obj.metadata, Metadata)
    assert isinstance(obj.spec, dict)
    assert obj.status is None
    assert obj.metadata.name == manifest["metadata"]["name"]
    assert obj.spec == manifest["spec"]


def test_resource_factory_cast_nonexistent_specific(resource_registry: ResourceRegistry):
    manifest = {
        "kind": "test.hubocean.io/unregistered",
        "version": "v1alpha1",
        "metadata": {"name": "unregistered-kind"},
        "spec": {
            "some_float": 3.14,
            "some_validated_str": "foobar",
        },
    }

    obj = resource_registry.resource_factory_cast(ResourceDto[UnregisteredSpec], manifest, raise_unknown=False)

    assert isinstance(obj, ResourceDto)
    assert isinstance(obj.metadata, Metadata)
    assert isinstance(obj.spec, UnregisteredSpec)
    assert obj.status is None
    assert obj.metadata.name == manifest["metadata"]["name"]
    assert obj.spec.some_float == manifest["spec"]["some_float"]
    assert obj.spec.some_validated_str == manifest["spec"]["some_validated_str"]


def test_resource_factory_cast_nonexistent_specific_validation_error(resource_registry: ResourceRegistry):
    manifest = {
        "kind": "test.hubocean.io/unregistered",
        "version": "v1alpha1",
        "metadata": {"name": "unregistered-kind"},
        "spec": {
            "some_float": 3.14,
            "some_validated_str": "invalid",
        },
    }

    with pytest.raises(ValueError):
        resource_registry.resource_factory_cast(ResourceDto[UnregisteredSpec], manifest, raise_unknown=False)
