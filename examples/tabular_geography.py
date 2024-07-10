from odp.client import OdpClient
from odp.client.dto.table_spec import TableSpec
from odp.client.dto.tabular_store import TablePartitioningSpec
from odp.dto import Metadata
from odp.dto.catalog import DatasetDto, DatasetSpec
from odp.dto.common.contact_info import ContactInfo

client = OdpClient()

# Create a new manifest to add to the catalog
dataset = DatasetDto(
    metadata=Metadata(
        name=client.personalize_name("st_within_example"),
        display_name="ST_WITHIN Example",
        description="A test dataset for ST_WITHIN query",
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

print("Dataset created successfully")

table_schema = {"name": {"type": "string"}, "location": {"type": "geometry"}}
partitioning = [TablePartitioningSpec(columns=["location"], transformer_name="geohash", args=[2])]

my_table_spec = TableSpec(table_schema=table_schema, partitioning=partitioning)

client.tabular.create_schema(
    resource_dto=dataset, table_spec=TableSpec(table_schema=table_schema, partitioning=partitioning)
)

print("Table spec created successfully")

data = [
    {"name": "Oslo", "location": {"type": "Point", "coordinates": [10.74609, 59.91273]}},
    {"name": "New York", "location": {"type": "Point", "coordinates": [-74.005974, 40.712776]}},
    {"name": "Los Angeles", "location": {"type": "Point", "coordinates": [-118.243683, 34.052235]}},
    {"name": "London", "location": {"type": "Point", "coordinates": [-0.127758, 51.507351]}},
    {"name": "Tokyo", "location": {"type": "Point", "coordinates": [139.691711, 35.689487]}},
    {"name": "Paris", "location": {"type": "Point", "coordinates": [2.352222, 48.856613]}},
    {"name": "Berlin", "location": {"type": "Point", "coordinates": [13.404954, 52.520008]}},
    {"name": "Moscow", "location": {"type": "Point", "coordinates": [37.617298, 55.755825]}},
    {"name": "Beijing", "location": {"type": "Point", "coordinates": [116.407394, 39.904202]}},
    {"name": "Mexico City", "location": {"type": "Point", "coordinates": [-99.133209, 19.432608]}},
    {"name": "São Paulo", "location": {"type": "Point", "coordinates": [-46.633308, -23.55052]}},
    {"name": "Buenos Aires", "location": {"type": "Point", "coordinates": [-58.381592, -34.603722]}},
    {"name": "New Delhi", "location": {"type": "Point", "coordinates": [77.209023, 28.613939]}},
    {"name": "Sydney", "location": {"type": "Point", "coordinates": [151.209296, -33.86882]}},
    {"name": "San Francisco", "location": {"type": "Point", "coordinates": [-122.419418, 37.774929]}},
    {"name": "Johannesburg", "location": {"type": "Point", "coordinates": [28.047305, -26.204103]}},
    {"name": "Chicago", "location": {"type": "Point", "coordinates": [-87.629799, 41.878113]}},
    {"name": "Melbourne", "location": {"type": "Point", "coordinates": [144.963058, -37.813628]}},
    {"name": "Edinburgh", "location": {"type": "Point", "coordinates": [-3.188267, 55.953251]}},
    {"name": "Stockholm", "location": {"type": "Point", "coordinates": [18.068581, 59.329323]}},
    {"name": "Ottawa", "location": {"type": "Point", "coordinates": [-75.697193, 45.42153]}},
    {"name": "Hong Kong", "location": {"type": "Point", "coordinates": [114.109497, 22.396428]}},
    {"name": "Jakarta", "location": {"type": "Point", "coordinates": [106.845599, -6.208763]}},
    {"name": "Cairo", "location": {"type": "Point", "coordinates": [31.235712, 30.04442]}},
    {"name": "Budapest", "location": {"type": "Point", "coordinates": [19.040236, 47.497913]}},
    {"name": "Christchurch", "location": {"type": "Point", "coordinates": [172.636225, -43.532054]}},
    {"name": "Manila", "location": {"type": "Point", "coordinates": [120.98422, 14.599512]}},
    {"name": "Bangkok", "location": {"type": "Point", "coordinates": [100.501765, 13.756331]}},
    {"name": "Rome", "location": {"type": "Point", "coordinates": [12.496366, 41.902783]}},
    {"name": "Shanghai", "location": {"type": "Point", "coordinates": [121.473702, 31.23039]}},
    {"name": "Rio de Janeiro", "location": {"type": "Point", "coordinates": [-43.172897, -22.906847]}},
    {"name": "Madrid", "location": {"type": "Point", "coordinates": [-3.70379, 40.416775]}},
    {"name": "Nairobi", "location": {"type": "Point", "coordinates": [36.821946, -1.292066]}},
    {"name": "Toronto", "location": {"type": "Point", "coordinates": [-79.383186, 43.653225]}},
    {"name": "Fortaleza", "location": {"type": "Point", "coordinates": [-38.526669, -3.731862]}},
    {"name": "Tehran", "location": {"type": "Point", "coordinates": [51.388973, 35.6895]}},
    {"name": "Brasília", "location": {"type": "Point", "coordinates": [-47.882166, -15.794229]}},
    {"name": "Bogotá", "location": {"type": "Point", "coordinates": [-74.072092, 4.710989]}},
]

print(f"Inserting {len(data)} rows into the table")
client.tabular.write(resource_dto=dataset, data=data)
print("Data inserted and partitioned")

print("Querying for cities in Europe")
europe_list = client.tabular.select_as_list(
    resource_dto=dataset,
    filter_query={
        "#ST_WITHIN": [
            "$location",  # <- Name of column to perform geographic query against.
            {
                "type": "Polygon",  # This is a rough polygon encompassing Europe.
                "coordinates": [
                    [
                        [37.02028908997249, 70.9411520317463],
                        [-24.834125592956013, 70.9411520317463],
                        [-24.834125592956013, 35.753296916825306],
                        [37.02028908997249, 35.753296916825306],
                        [37.02028908997249, 70.9411520317463],
                    ]
                ],
            },
        ]
    },
)

print("Cities in Europe:")
for city in europe_list:
    print(city.get("name"))

# Clean up
print("Cleaning up")

client.tabular.delete_schema(dataset)
client.catalog.delete(dataset)

print("Done")
