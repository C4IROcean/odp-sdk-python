from odp.client import OdpClient
from odp.client.dto.table_spec import TableSpec
from odp.client.exc import OdpResourceNotFoundError
from odp.dto import Metadata
from odp.dto.catalog import DatasetDto, DatasetSpec
from odp.dto.common.contact_info import ContactInfo

# Instantiate the client without specifying a token provider.
#   The token provider will be set based on the environment.
client = OdpClient()

# Declare a dataset manifest to add to the catalog

print("Creating sample dataset")

dataset = DatasetDto(
    metadata=Metadata(
        name=client.personalize_name("sdk-tabular-example"),
        display_name="SDK Tabular Example",
        description="A test dataset for tabular data",
        labels={"hubocean.io/test": True},
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
dataset = client.catalog.create(dataset)

# Create a table spec to create the schema in tabular client
print("Creating table spec")

mt_table_spec = client.tabular.create_schema(
    resource_dto=dataset, table_spec=TableSpec(table_schema={"Data": {"type": "string"}})
)

# Insert data into the table
test_data = [{"Data": "Test"}, {"Data": "Test1"}]
print(f"Inserting {len(test_data)} rows into the table")

client.tabular.write(resource_dto=dataset, data=test_data)

# Query the data as a list
print("Querying data from the table as a list")
our_data = client.tabular.select_as_list(dataset)

print("Data query result:")
print(f"{our_data}\n")

# To update the data filters must be declared
update_filters = {"#EQUALS": ["$Data", "Test"]}
new_data = [{"Data": "Test Updated"}]

print("Updating data in the table")
client.tabular.update(
    resource_dto=dataset,
    data=new_data,
    filter_query=update_filters,
)

result = client.tabular.select_as_list(dataset)

print(f"Data read back:\n{result}")  # noqa: E231

# Delete the data with another filter
delete_filters = {"#EQUALS": ["$Data", "Test1"]}
print("Deleting data in the table")

client.tabular.delete(resource_dto=dataset, filter_query=delete_filters)
result = client.tabular.select_as_list(dataset)

print(f"Data read back:\n{result}")  # noqa: E231

# Clean up

print("Cleaning up")

# Delete the schema
client.tabular.delete_schema(dataset, delete_data=True)

# Reading the schema of a dataset without a schema will result in an error
try:
    client.tabular.get_schema(dataset)
except OdpResourceNotFoundError as e:
    print("Schema not found error since it is deleted")
    print(e)

print("Deleting dataset")

# Delete the dataset
client.catalog.delete(dataset)

print("Done")
