import random
import string

import pytest

from odp_sdk.client import OdpClient
from odp_sdk.dto import ResourceDto
from odp_sdk.resource_client import OdpResourceClient
from tests.fixtures import azure_token_provider


@pytest.mark.usefixtures("azure_token_provider")
def test_catalog_client():
    client = OdpClient()

    catalog_client = client.catalog
    assert isinstance(catalog_client, OdpResourceClient)

    # List all resources in the catalog
    for item in catalog_client.list():
        print(item)

    print("-------")

    # Create a new manifest to add to the catalog
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

    # The dataset is created in the catalog.
    manifest = catalog_client.create(manifest)
    assert isinstance(manifest, ResourceDto)

    # Fetch the manifest from the catalog using the UUID
    fetched_manifest = catalog_client.get(manifest.metadata.uuid)
    print(fetched_manifest)
    assert isinstance(fetched_manifest, ResourceDto)

    # Clean up
    catalog_client.delete(manifest)
