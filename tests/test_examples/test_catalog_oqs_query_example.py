from odp.client import OdpClient
from odp.dto import ResourceDto


def test_catalog_oqs_query(odp_client: OdpClient):
    oqs_filter = {
        "#EQUALS": [
            "$kind",
            "catalog.hubocean.io/dataCollection",
        ]
    }

    for item in odp_client.catalog.list(oqs_filter):
        assert isinstance(item, ResourceDto)

    assert odp_client.catalog.list(oqs_filter) != []
