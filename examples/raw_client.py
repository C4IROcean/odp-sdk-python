from odp_sdk.client import OdpClient
from odp_sdk.dto import ResourceDto
from odp_sdk.dto.file_dto import FileMetadataDto

# export ODP_ACCESS_TOKEN=Bearer thisismyaccesstoken

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

# client.catalog.delete(my_dataset)

# The dataset is created in the catalog.
my_dataset = client.catalog.create(my_dataset)

# The resource_dto is updated with the uuid etc
print(my_dataset)


file_dto = client.raw.create_file(
    resource_dto=my_dataset,
    file_meta=FileMetadataDto(**{"name": "test_csv.csv", "mime_type": "text/csv"}),
    contents=b"This is a test file",
    overwrite=True,
)

print("---------------")

print(file_dto)

client.catalog.delete(my_dataset)
