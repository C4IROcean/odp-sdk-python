from odp_sdk.client import OdpClient
from odp_sdk.dto import ResourceDto

client = OdpClient()

catalog_client = client.catalog

# List all resources in the catalog
for item in catalog_client.list():
    print(item)

print("-------")

# Create a new manifest to add to the catalog
manifest = ResourceDto(
    **{
        "kind": "catalog.hubocean.io/dataset",
        "version": "v1alpha3",
        "metadata": {
            "name": "sdk-manifest-creation-example",
        },
        "spec": {
            "storage_controller": "registry.hubocean.io/storageController/storage-tabular",
            "storage_class": "registry.hubocean.io/storageClass/tabular",
            "maintainer": {"contact": "Just Me <raw_client_example@hubocean.earth>"},  # <-- strict syntax here
        },
    }
)

# The dataset is created in the catalog.
manifest = catalog_client.create(manifest)

# Fetch the manifest from the catalog using the UUID
fetched_manifest = catalog_client.get(manifest.metadata.uuid)
print(fetched_manifest)

# Clean up
catalog_client.delete(manifest)
