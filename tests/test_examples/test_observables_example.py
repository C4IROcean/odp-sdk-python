import random
import string
from typing import Tuple
from uuid import UUID

from odp_sdk.client import OdpClient
from odp_sdk.dto import ResourceDto


def test_observables(odp_client_owner: Tuple[OdpClient, UUID]):
    catalog_client = odp_client_owner[0].catalog

    # List observables in the catalog
    observable_filter = {"#EQUALS": ["$kind", "catalog.hubocean.io/observable"]}

    for item in catalog_client.list(observable_filter):
        print(item)

    print("-------")

    # Create a new manifest to add to the catalog
    observable_manifest = ResourceDto(
        **{
            "kind": "catalog.hubocean.io/observable",
            "version": "v1alpha2",
            "metadata": {
                "name": "".join(random.choices(string.ascii_lowercase + string.digits, k=20)),
                "display_name": "Test Observable for time",
                "description": "A test observable for time",
                "labels": {"hubocean.io/test": True},
                "owner": odp_client_owner[1],
            },
            "spec": {
                "ref": "catalog.hubocean.io/dataset/test-dataset",
                "observable_class": "catalog.hubocean.io/observableClass/static-coverage",
                "details": {"value": [0, 1684147082], "attribute": "test"},
            },
        }
    )

    # The observable is created in the catalog.
    observable_manifest = catalog_client.create(observable_manifest)
    assert isinstance(observable_manifest, ResourceDto)

    # Fetch the manifest from the observable using the UUID
    fetched_manifest = catalog_client.get(observable_manifest.metadata.uuid)
    print(fetched_manifest)
    print("-------")
    assert isinstance(fetched_manifest, ResourceDto)

    # An example query to search for observables in certain geometries
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

    # List all observables in the catalog that intersect with the geometry
    for item in catalog_client.list(observable_geometry_filter):
        print(item)
    print("-------")
    assert [observable for observable in catalog_client.list(observable_geometry_filter)] != []

    # Create static observables to filter
    static_manifest_small = ResourceDto(
        **{
            "kind": "catalog.hubocean.io/observable",
            "version": "v1alpha2",
            "metadata": {
                "name": "".join(random.choices(string.ascii_lowercase + string.digits, k=20)),
                "display_name": "SDK Example Small Value",
                "description": "An observable that emits a small value",
                "labels": {"hubocean.io/test": True},
                "owner": odp_client_owner[1],
            },
            "spec": {
                "ref": "catalog.hubocean.io/dataset/test-dataset",
                "observable_class": "catalog.hubocean.io/observableClass/static-observable",
                "details": {"value": 1, "attribute": "test"},
            },
        }
    )

    catalog_client.create(static_manifest_small)

    static_manifest_large = ResourceDto(
        **{
            "kind": "catalog.hubocean.io/observable",
            "version": "v1alpha2",
            "metadata": {
                "name": "".join(random.choices(string.ascii_lowercase + string.digits, k=20)),
                "display_name": "SDK Example Large Value",
                "description": "An observable that emits a large value",
                "labels": {"hubocean.io/test": True},
                "owner": odp_client_owner[1],
            },
            "spec": {
                "ref": "catalog.hubocean.io/dataset/test-dataset",
                "observable_class": "catalog.hubocean.io/observableClass/static-observable",
                "details": {"value": 3, "attribute": "test"},
            },
        }
    )

    catalog_client.create(static_manifest_large)

    # An example query to search for observables in certain range
    observable_range_filter = {
        "#AND": [
            {"#WITHIN": ["$spec.observable_class", ["catalog.hubocean.io/observableClass/static-observable"]]},
            {"#GREATER_THAN_OR_EQUALS": ["$spec.details.value", "2"]},
        ]
    }

    # List all observables in the catalog that intersect with the geometry
    for item in catalog_client.list(observable_range_filter):
        print(item)
    print("-------")
    assert [observable for observable in catalog_client.list(observable_range_filter)] != []
