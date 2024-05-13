import unittest

from odp_sdk.auth import get_default_token_provider
from odp_sdk.client import OdpClient


class TestCatalogOqsQueryExample(unittest.TestCase):
    def test_catalog_oqs_query(self):
        get_default_token_provider()
        client = OdpClient()

        # Filter collections

        oqs_filter = {
            "#EQUALS": [  # EQUALS is used here to compare to values
                "$kind",  # The first value is the kind from the metadata, prefaced with a dollarsign.
                "catalog.hubocean.io/dataCollection",  # And this is the value to compare with
            ]
        }

        for item in client.catalog.list(oqs_filter):
            print(item)

        self.assertTrue(client.catalog.list(oqs_filter))
