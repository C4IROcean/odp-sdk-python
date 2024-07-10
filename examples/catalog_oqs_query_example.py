from odp.client import OdpClient
from odp.dto.catalog import DataCollectionDto

# Instantiate the client without specifying a token provider.
#   The token provider will be set based on the environment.
client = OdpClient()

# List all resources matching a given query

oqs_filter = {
    "#EQUALS": [  # EQUALS is used here to compare to values
        "$kind",  # The first value is the kind from the metadata, prefaced with a dollarsign.
        "catalog.hubocean.io/dataCollection",  # And this is the value to compare with
    ]
}

print("Listing all data collections:")

for item in client.catalog.list(oqs_filter):
    print(item)

# If we know the type of the resource we are querying,
#   we can use the `tp` parameter to assert the type of the returned resources.

print("Listing all data collections:")

for item in client.catalog.list(oqs_filter, tp=DataCollectionDto, assert_type=True):
    print(item)
