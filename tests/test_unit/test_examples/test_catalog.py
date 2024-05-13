import random
import string
import unittest

from odp_sdk.auth import get_default_token_provider
from odp_sdk.client import OdpClient
from odp_sdk.dto import ResourceDto


class TestCatalogMethods(unittest.TestCase):
    def test_get_catalog(self):
        get_default_token_provider()
        client = OdpClient()

        resource_manifest = client.catalog.get(
            "catalog.hubocean.io/dataCollection/1e3401d4-9630-40cd-a9cf-d875cb310449-glodap"
        )

        self.assertTrue(resource_manifest)

        same_resource_manifest = client.catalog.get(resource_manifest.metadata.uuid)

        self.assertTrue(same_resource_manifest)

    def test_list_catalog(self):
        get_default_token_provider()
        client = OdpClient()

        oqs_filter = {
            "#EQUALS": [  # EQUALS is used here to compare to values
                "$kind",  # The first value is the kind from the metadata, prefaced with a dollarsign.
                "catalog.hubocean.io/dataCollection",  # And this is the value to compare with
            ]
        }

        self.assertTrue(client.catalog.list(oqs_filter=oqs_filter))

    def test_create_catalog(self):
        get_default_token_provider()
        client = OdpClient()

        # ResourceDto
        manifest = ResourceDto(
            **{
                "kind": "catalog.hubocean.io/dataset",
                "version": "v1alpha3",
                "metadata": {
                    "name": "".join(random.choices(string.ascii_lowercase + string.digits, k=20)),
                },
                "spec": {
                    "storage_controller": "registry.hubocean.io/storageController/storage-tabular",
                    "storage_class": "registry.hubocean.io/storageClass/tabular",
                    "maintainer": {"contact": "Just Me <raw_client_example@hubocean.earth>"},  # <-- strict syntax here
                },
            }
        )

        self.assertTrue(client.catalog.create(manifest))


if __name__ == "__main__":
    unittest.main()
