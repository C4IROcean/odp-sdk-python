from odp_sdk.client import OdpClient
from odp_sdk.dto import ResourceDto
from odp_sdk.dto.file_dto import FileMetadataDto

# export ODP_ACCESS_TOKEN=Bearer thisismyaccesstoken <-- Omit this to run interactive auth

client = OdpClient()

my_dataset = ResourceDto(
    **{
        "kind": "catalog.hubocean.io/dataset",
        "version": "v1alpha3",
        "metadata": {
            "name": "seahorses",
        },
        "spec": {
            "storage_controller": "registry.hubocean.io/storageController/storage-raw-cdffs",
            "storage_class": "registry.hubocean.io/storageClass/raw",
            "maintainer": {"contact": "Just Me <raw_client_example@hubocean.earth>"},  # <-- strict syntax here
        },
    }
)

# The dataset is created in the catalog.
my_dataset = client.catalog.create(my_dataset)

# Creating and uploading a file.
file_dto = client.raw.create_file(
    resource_dto=my_dataset,
    file_metadata_dto=FileMetadataDto(**{"name": "test.txt", "mime_type": "text/plain"}),
    contents=b"Hello, World!",
)
print("-------FILES IN DATASET--------")

for file in client.raw.list(my_dataset):
    print(file)

# Download file
client.raw.download_file(my_dataset, file_dto, "test.txt")


# Clean up
client.raw.delete_file(my_dataset, file_dto)

client.catalog.delete(my_dataset)
