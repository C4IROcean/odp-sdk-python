from odp.client import OdpClient
from odp.client.dto.file_dto import FileMetadataDto
from odp.client.exc import OdpFileAlreadyExistsError, OdpResourceExistsError
from odp.dto import Metadata
from odp.dto.catalog import DatasetDto, DatasetSpec
from odp.dto.common.contact_info import ContactInfo

# Instantiate the client without specifying a token provider.
# The token provider will be set based on the environment.
client = OdpClient()

# Declare a dataset manifest to add to the catalog

print("Creating sample dataset")

dataset = DatasetDto(
    metadata=Metadata(
        name=client.personalize_name("sdk-raw-example"),
        display_name="SDK Raw Example",
        description="A test dataset for raw data",
        labels={"hubocean.io/test": True},
    ),
    spec=DatasetSpec(
        storage_controller="registry.hubocean.io/storageController/storage-raw-cdffs",
        storage_class="registry.hubocean.io/storageClass/raw",
        maintainer=ContactInfo(
            contact="User McUsername <user.mcusername@emailprovider.com>",
            organization="Organization Name",
        ),
    ),
)

# The dataset is created in the catalog.
try:
    dataset = client.catalog.create(dataset)
    print("Resource created successfully:", dataset)
except OdpResourceExistsError:
    print("Dataset already exists. Getting existing dataset")
    dataset = client.catalog.get("catalog.hubocean.io/dataset/" + dataset.metadata.name)
    print(dataset)

# Creating and uploading an existing file.
path_to_file = "test.txt"
file_metadata_dto = None
file_dto = None
try:
    with open(path_to_file, "rb") as data:
        file_metadata_dto = FileMetadataDto(
            name=data.name,
            mime_type="text/plain",  # Update mime type of the file
        )
        file_dto = client.raw.create_file(
            resource_dto=dataset,
            file_metadata_dto=file_metadata_dto,
            contents=data.read(),
        )
except OdpFileAlreadyExistsError:
    print("File already exists. Getting metadata of existing file")
    file_dto = client.raw.get_file_metadata(dataset, file_metadata_dto)

print("List of files in the dataset:")

for file in client.raw.list(dataset):
    print(file)

# Download file
print("Downloading the file:")

client.raw.download_file(dataset, file_dto, "test.txt")

# Clean up
print("Cleaning up")

client.raw.delete_file(dataset, file_dto)
client.catalog.delete(dataset)

print("Done")
