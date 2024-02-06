from odp_sdk.client import OdpClient
from odp_sdk.dto import ResourceDto

client = OdpClient()

catalog_client = client.catalog

observables = []

# List observables in the catalog
observable_filter = {"selectors": [{"kind": "catalog.hubocean.io/observable"}]}

for item in catalog_client.list(observable_filter):
    print(item)

print("-------")

# Create a new manifest to add to the catalog
observable_manifest = ResourceDto(
    **{
        "kind": "catalog.hubocean.io/observable",
        "version": "v1alpha1",
        "metadata": {
            "name": "sdk-observable-example",
            "display_name": "Test Observable for time",
            "description": "A test observable for time",
            "labels": {"hubocean.io/test": True},
        },
        "spec": {
            "dataset": "catalog.hubocean.io/test-dataset",
            "details": {"value": [0, 1684147082], "cls": "odp.odcat.observable.observable.StaticCoverage"},
        },
    }
)

# The observable is created in the catalog.
observable_manifest = catalog_client.create(observable_manifest)
observables.append(observable_manifest)

# Fetch the manifest from the observable using the UUID
fetched_manifest = catalog_client.get(observable_manifest.metadata.uuid)
print(fetched_manifest)
print("-------")

# An example query to search for observables in certain geometries
observable_geometry_filter = {
    "selectors": [
        {"kind": "catalog.hubocean.io/observable"},
        {
            "geometry": {
                "spec.details.value": {
                    "intersects": {
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
                    }
                }
            }
        },
    ]
}

# List all observables in the catalog that intersect with the geometry
for item in catalog_client.list(observable_geometry_filter):
    print(item)
print("-------")


# Create static observables to filter
static_manifest_small = ResourceDto(
    **{
        "kind": "catalog.hubocean.io/observable",
        "version": "v1alpha1",
        "metadata": {
            "name": "sdk-example-small-value",
            "display_name": "SDK Example Small Value",
            "description": "An observable that emits a small value",
            "labels": {
                "hubocean.io/test": True
            }
        },
        "spec": {
            "dataset": "catalog.hubocean.io/test-dataset",
            "details": {
                "value": 1,
                "cls": "odp.odcat.observable.observable.StaticObservable"
            }
        }
    }
)

small_manifest = catalog_client.create(static_manifest_small)
observables.append(small_manifest)

static_manifest_large = ResourceDto(
    **{
        "kind": "catalog.hubocean.io/observable",
        "version": "v1alpha1",
        "metadata": {
            "name": "sdk-example-large-value",
            "display_name": "SDK Example Large Value",
            "description": "An observable that emits a large value",
            "labels": {
                "hubocean.io/test": True
            }
        },
        "spec": {
            "dataset": "catalog.hubocean.io/test-dataset",
            "details": {
                "value": 3,
                "cls": "odp.odcat.observable.observable.StaticObservable"
            }
        }
    }
)

large_manifest = catalog_client.create(static_manifest_large)
observables.append(large_manifest)


# An example query to search for observables in certain range
observable_range_filter = {
    "oqs": {
        "#AND": [
            {
                "#WITHIN": [
                    "$spec.details.cls",
                    [
                        "odp.odcat.observable.observable.StaticObservable"
                    ]
                ]
            },
            {
                "#GREATER_THAN_OR_EQUALS": [
                    "$spec.details.value",
                    "2"
                ]
            }
        ]
    }
}

# List all observables in the catalog that intersect with the geometry
for item in catalog_client.list(observable_range_filter):
    print(item)
print("-------")

# Clean up
for obs in observables:
    catalog_client.delete(obs)
