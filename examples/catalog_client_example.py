from odp.client import OdpClient
from odp.dto import Metadata
from odp.dto.catalog import DatasetDto, DatasetSpec
from odp.dto.common.contact_info import ContactInfo

# Instantiate the client without specifying a token provider.
#   The token provider will be set based on the environment.
client = OdpClient()

print("Datasets in the catalog:")

# List all resources in the catalog
for item in client.catalog.list():
    print(item)

# Declare a dataset manifest to add to the catalog
manifest = DatasetDto(
    metadata=Metadata(
        name=client.personalize_name("sdk-manifest-creation-example"),
    ),
    spec=DatasetSpec(
        storage_controller="registry.hubocean.io/storageController/storage-tabular",
        storage_class="registry.hubocean.io/storageClass/tabular",
        maintainer=ContactInfo(
            contact="User McUsername <user.mcusername@emailprovider.com>",
            organization="Organization Name",
        ),
    ),
)

# The dataset is created in the catalog.
manifest = client.catalog.create(manifest)

# Fetch the manifest from the catalog using the UUID
print("Fetching the manifest from the catalog using the UUID")

fetched_manifest = client.catalog.get(manifest.metadata.uuid)
print(fetched_manifest)

# Clean up
print("Cleaning up")

client.catalog.delete(manifest)

print("Done")
