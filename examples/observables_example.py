from odp.client import OdpClient
from odp.dto import Metadata
from odp.dto.catalog import ObservableDto, ObservableSpec

# Instantiate the client without specifying a token provider.
#   The token provider will be set based on the environment.
client = OdpClient()

created_manifests = []

# List observables in the catalog
observable_filter = {"#EQUALS": ["$kind", "catalog.hubocean.io/observable"]}

# If we know the type of the resource we are querying,
#   we can use the `tp` parameter to assert the type of the returned resources.

print("List of observables in the catalog:")

for item in client.catalog.list(observable_filter, tp=ObservableDto, assert_type=True):
    print(item)

# Declare a new observable to be added to the data catalog

print("Creating a sample observable in the catalog")

manifest = ObservableDto(
    metadata=Metadata(
        name=client.personalize_name("sdk-observable-example"),
        display_name="Test Observable for time",
        description="A test observable for time",
        labels={"hubocean.io/test": True},
    ),
    spec=ObservableSpec(
        ref="catalog.hubocean.io/dataset/test-dataset",
        observable_class="catalog.hubocean.io/observableClass/static-coverage",
        details={"value": [0, 1684147082], "attribute": "test"},
    ),
)

# The observable is created in the catalog.
#   The return value is the full manifest of the created observable.
manifest = client.catalog.create(manifest)
created_manifests.append(manifest)

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

print("List of observables in the catalog:")

# List all observables in the catalog that intersect with the geometry
for item in client.catalog.list(observable_geometry_filter):
    print(item)


print("Adding more sample observables in the catalog")

# Create static observables to filter
manifest = ObservableDto(
    metadata=Metadata(
        name=client.personalize_name("sdk-example-small-value"),
        display_name="SDK Example Small Value",
        description="An observable that emits a small value",
        labels={"hubocean.io/test": True},
    ),
    spec=ObservableSpec(
        ref="catalog.hubocean.io/dataset/test-dataset",
        observable_class="catalog.hubocean.io/observableClass/static-observable",
        details={"value": 1, "attribute": "test"},
    ),
)

manifest = client.catalog.create(manifest)
created_manifests.append(manifest)

manifest = ObservableDto(
    metadata=Metadata(
        name=client.personalize_name("sdk-example-large-value"),
        display_name="SDK Example Large Value",
        description="An observable that emits a large value",
        labels={"hubocean.io/test": True},
    ),
    spec=ObservableSpec(
        ref="catalog.hubocean.io/dataset/test-dataset",
        observable_class="catalog.hubocean.io/observableClass/static-observable",
        details={"value": 3, "attribute": "test"},
    ),
)

manifest = client.catalog.create(manifest)
created_manifests.append(manifest)


# An example query to search for observables in certain range
observable_range_filter = {
    "#AND": [
        {"#WITHIN": ["$spec.observable_class", ["catalog.hubocean.io/observableClass/static-observable"]]},
        {"#GREATER_THAN_OR_EQUALS": ["$spec.details.value", "2"]},
    ]
}

print("List of observables in the catalog:")

# List all observables in the catalog that intersect with the geometry
for item in client.catalog.list(observable_range_filter):
    print(item)

print("Cleaning up")

# Clean up
for man in created_manifests:
    client.catalog.delete(man)

print("Done")
