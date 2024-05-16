from odp_sdk.client import OdpClient


def test_catalog_oqs_query(odp_client: OdpClient):
    # Filter collections

    oqs_filter = {
        "#EQUALS": [  # EQUALS is used here to compare to values
            "$kind",  # The first value is the kind from the metadata, prefaced with a dollarsign.
            "catalog.hubocean.io/dataCollection",  # And this is the value to compare with
        ]
    }

    for item in odp_client.catalog.list(oqs_filter):
        print(item)

    assert odp_client.catalog.list(oqs_filter) != []
