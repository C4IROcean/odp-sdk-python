import random
import string
from typing import Tuple
from uuid import UUID

from odp.dto import ObservableDto, ObservableSpec
from odp_sdk.client import OdpClient


def test_observables(odp_client_test_uuid: Tuple[OdpClient, UUID]):
    catalog_client = odp_client_test_uuid[0].catalog

    observable_filter = {"#EQUALS": ["$kind", "catalog.hubocean.io/observable"]}

    for item in catalog_client.list(observable_filter):
        assert isinstance(item.spec, ObservableSpec)

    observable_manifest = ObservableDto(
        **{
            "kind": "catalog.hubocean.io/observable",
            "version": "v1alpha2",
            "metadata": {
                "name": "".join(random.choices(string.ascii_lowercase + string.digits, k=20)),
                "display_name": "Test Observable for time",
                "description": "A test observable for time",
                "labels": {"hubocean.io/test": True, "test_uuid": odp_client_test_uuid[1]},
            },
            "spec": {
                "ref": "catalog.hubocean.io/dataset/test-dataset",
                "observable_class": "catalog.hubocean.io/observableClass/static-geometric-coverage",
                "details": {"value": {"type": "Point", "coordinates": [-73.981200, 40.764950]}, "attribute": "test"},
            },
        }
    )

    observable_manifest = catalog_client.create(observable_manifest)
    assert isinstance(observable_manifest.spec, ObservableSpec)

    fetched_manifest = catalog_client.get(observable_manifest.metadata.uuid)
    assert isinstance(fetched_manifest.spec, ObservableSpec)

    observable_geometry_filter = {
        "#AND": [
            {"#EQUALS": ["$kind", "catalog.hubocean.io/observable"]},
            {
                "#ST_INTERSECTS": [
                    "$spec.details.value",
                    {
                        "type": "Polygon",
                        "coordinates": [
                            [
                                [-73.981200, 40.764950],
                                [-73.980600, 40.764000],
                                [-73.979800, 40.764450],
                                [-73.980400, 40.765400],
                                [-73.981200, 40.764950],
                            ]
                        ],
                    },
                ]
            },
        ]
    }

    for item in catalog_client.list(observable_geometry_filter):
        assert isinstance(item.spec, ObservableSpec)
    assert [observable for observable in catalog_client.list(observable_geometry_filter)] != []

    static_manifest_small = ObservableDto(
        **{
            "kind": "catalog.hubocean.io/observable",
            "version": "v1alpha2",
            "metadata": {
                "name": "".join(random.choices(string.ascii_lowercase + string.digits, k=20)),
                "display_name": "SDK Example Small Value",
                "description": "An observable that emits a small value",
                "labels": {"hubocean.io/test": True, "test_uuid": odp_client_test_uuid[1]},
            },
            "spec": {
                "ref": "catalog.hubocean.io/dataset/test-dataset",
                "observable_class": "catalog.hubocean.io/observableClass/static-observable",
                "details": {"value": 1, "attribute": "test"},
            },
        }
    )

    catalog_client.create(static_manifest_small)

    static_manifest_large = ObservableDto(
        **{
            "kind": "catalog.hubocean.io/observable",
            "version": "v1alpha2",
            "metadata": {
                "name": "".join(random.choices(string.ascii_lowercase + string.digits, k=20)),
                "display_name": "SDK Example Large Value",
                "description": "An observable that emits a large value",
                "labels": {"hubocean.io/test": True, "test_uuid": odp_client_test_uuid[1]},
            },
            "spec": {
                "ref": "catalog.hubocean.io/dataset/test-dataset",
                "observable_class": "catalog.hubocean.io/observableClass/static-observable",
                "details": {"value": 3, "attribute": "test"},
            },
        }
    )

    catalog_client.create(static_manifest_large)

    observable_range_filter = {
        "#AND": [
            {"#WITHIN": ["$spec.observable_class", ["catalog.hubocean.io/observableClass/static-observable"]]},
            {"#GREATER_THAN_OR_EQUALS": ["$spec.details.value", 2]},
        ]
    }

    list_observables = []
    for item in catalog_client.list(observable_range_filter):
        assert isinstance(item.spec, ObservableSpec)
        list_observables.append(item)

    assert list_observables != []
