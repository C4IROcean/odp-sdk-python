from odp_sdk.client import OdpClient
from odp_sdk.dto import ResourceDto

client = OdpClient()

catalog_client = client.catalog

observables = []

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
            "name": "sdk-observable-example",
            "display_name": "Test Observable for time",
            "description": "A test observable for time",
            "labels": {"hubocean.io/test": True},
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
observables.append(observable_manifest)

# Fetch the manifest from the observable using the UUID
fetched_manifest = catalog_client.get(observable_manifest.metadata.uuid)
print(fetched_manifest)
print("-------")

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


# Create static observables to filter
static_manifest_small = ResourceDto(
    **{
        "kind": "catalog.hubocean.io/observable",
        "version": "v1alpha2",
        "metadata": {
            "name": "sdk-example-small-value",
            "display_name": "SDK Example Small Value",
            "description": "An observable that emits a small value",
            "labels": {"hubocean.io/test": True},
        },
        "spec": {
            "ref": "catalog.hubocean.io/dataset/test-dataset",
            "observable_class": "catalog.hubocean.io/observableClass/static-observable",
            "details": {"value": 1, "attribute": "test"},
        },
    }
)

small_manifest = catalog_client.create(static_manifest_small)
observables.append(small_manifest)

static_manifest_large = ResourceDto(
    **{
        "kind": "catalog.hubocean.io/observable",
        "version": "v1alpha2",
        "metadata": {
            "name": "sdk-example-large-value",
            "display_name": "SDK Example Large Value",
            "description": "An observable that emits a large value",
            "labels": {"hubocean.io/test": True},
        },
        "spec": {
            "ref": "catalog.hubocean.io/dataset/test-dataset",
            "observable_class": "catalog.hubocean.io/observableClass/static-observable",
            "details": {"value": 3, "attribute": "test"},
        },
    }
)

large_manifest = catalog_client.create(static_manifest_large)
observables.append(large_manifest)


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

# Clean up
for obs in observables:
    catalog_client.delete(obs)
