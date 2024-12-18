from odp.client import OdpClient
from odp.client.dto.file_dto import FileMetadataDto
from odp.dto import Metadata
from odp.dto.catalog import DataCollectionDto, DataCollectionSpec, DatasetDto, DatasetSpec
from odp.dto.common.contact_info import ContactInfo
from odp.dto.common.license import License

# Instantiate the client without specifying a token provider.
# The token provider will be set based on the environment.
client = OdpClient()

data_collection_name = "collection-manifest-example"

collection = DataCollectionDto(
    metadata=Metadata(
        name=data_collection_name,
        display_name="collection-example",
        description="A test data collection",
    ),
    spec=DataCollectionSpec(
        published_by=ContactInfo(
            contact="User McUsername <user.mcusername@emailprovider.com>",
            organisation="Organisation Name",
        ),
        published_date="2019-06-19T06:00:00",
        website="https://hubocean.earth",
        license=License(
            name="proprietary",
            full_text="This is a very strict legal text describing the data license.",
            href="www.wikipedia.org",
        ),
        tags=[],
    ),
)

collection = client.catalog.create(collection)
print("Collection was created")

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
        data_collection=f"catalog.hubocean.io/dataCollection/{data_collection_name}",
        storage_controller="registry.hubocean.io/storageController/storage-raw-cdffs",
        storage_class="registry.hubocean.io/storageClass/raw",
        maintainer=ContactInfo(
            contact="User McUsername <user.mcusername@emailprovider.com>",
            organisation="Organisation Name",
        ),
    ),
)

# The dataset is created in the catalog.
dataset = client.catalog.create(dataset)

# Creating and uploading a file.
file_dto = client.raw.create_file(
    resource_dto=dataset,
    file_metadata_dto=FileMetadataDto(
        name="test.txt",
        mime_type="text/plain",
    ),
    contents=b"Hello, World!",
)

print("List of files in the dataset:")

for file in client.raw.list(dataset):
    print(file)

# Download file
print("Downloading the file")

client.raw.download_file(dataset, file_dto, "test.txt")

# Clean up
print("Cleaning up")

client.raw.delete_file(dataset, file_dto)
client.catalog.delete(dataset)
client.catalog.delete(collection)

print("Done")
