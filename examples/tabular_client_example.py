from odp_sdk.client import OdpClient
from odp_sdk.dto import ResourceDto
from odp_sdk.dto.table_spec import TableSpec
from odp_sdk.exc import OdpResourceNotFoundError

# export ODP_ACCESS_TOKEN=Bearer thisismyaccesstoken <-- Omit this to run interactive auth

client = OdpClient()

# Create a new manifest to add to the catalog
my_dataset = ResourceDto(
    **{
        "kind": "catalog.hubocean.io/dataset",
        "version": "v1alpha3",
        "metadata": {
            "name": "sdk-tabular-example",
        },
        "spec": {
            "storage_controller": "registry.hubocean.io/storageController/storage-tabular",
            "storage_class": "registry.hubocean.io/storageClass/tabular",
            "maintainer": {"contact": "Just Me <raw_client_example@hubocean.earth>"},  # <-- strict syntax here
        },
    }
)

# The dataset is created in the catalog.
print("Creating dataset")
my_dataset = client.catalog.create(my_dataset)
print("Dataset created successfully")
print(f"{my_dataset}\n")

# Create a table spec to create the schema in tabular client
print("Creating table spec")
table_schema = {"Data": {"type": "string"}}
my_table_spec = TableSpec(table_schema=table_schema)

mt_table_spec = client.tabular.create_schema(resource_dto=my_dataset, table_spec=my_table_spec)
print("Table spec created successfully")
print(f"{mt_table_spec}\n")

# Insert data into the table
test_data = [{"Data": "Test"}, {"Data": "Test1"}]
print("Inserting data into the table")
client.tabular.write(resource_dto=my_dataset, data=test_data)

# Query the data as a list
print("Querying data from the table as a list")
our_data = client.tabular.select_as_list(my_dataset)

print("-------DATA IN DATASET--------")
print(f"{our_data}\n")

# Query the data as a stream
print("Querying data from the table as a stream")
our_data = client.tabular.select_as_stream(my_dataset)

print("-------DATA IN DATASET--------")
print(f"{[data for data in our_data]}\n")

# To update the data filters must be declared
update_filters = {"#EQUALS": ["$Data", "Test"]}
new_data = [{"Data": "Test Updated"}]
print("Updating data in the table")
client.tabular.update(
    resource_dto=my_dataset,
    data=test_data,
    filter_query=update_filters,
)

result = client.tabular.select_as_list(my_dataset)

print("-------UPDATED DATA IN DATASET--------")
print(f"{result}\n")

# Delete the data with another filter
delete_filters = {"#EQUALS": ["$Data", "Test1"]}
print("Deleting data in the table")
client.tabular.delete(resource_dto=my_dataset, filter_query=delete_filters)
result = client.tabular.select_as_list(my_dataset)

print("-------DATA IN DATASET AFTER DELETION--------")
print(f"{result}\n")
# Delete the schema
client.tabular.delete_schema(my_dataset)

try:
    client.tabular.get_schema(my_dataset)
except OdpResourceNotFoundError as e:
    print("Schema not found error since it is deleted:")
    print(e)


# Delete the dataset
client.catalog.delete(my_dataset)
print("Dataset deleted successfully")
