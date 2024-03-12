from odp_sdk.client import OdpClient

client = OdpClient()

# Filter collections

oqs_filter = {
  "oqs": { # When querying to the catalog, the oqs filter is passed inside an object named oqs.
    "#EQUALS": [ #EQUALS is used here to compare to values
      "$kind", #The first value is the kind from the metadata, prefaced with a dollarsign.
      "catalog.hubocean.io/dataCollection" # And this is the value to compare with
    ]
  }
}


for item in client.catalog.list(oqs_filter):
    print(item)