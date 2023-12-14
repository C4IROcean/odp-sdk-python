from odp_sdk.client import OdpClient

client = OdpClient()

catalog_client = client.catalog

for item in catalog_client.list():
    print(item)
