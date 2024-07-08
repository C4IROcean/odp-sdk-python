import random
import string
from typing import Tuple
from uuid import UUID

from odp.dto import DatasetDto, DatasetSpec, ResourceDto
from odp_sdk.client import OdpClient
from odp_sdk.resource_client import OdpResourceClient


def test_catalog_client(odp_client_test_uuid: Tuple[OdpClient, UUID]):
    catalog_client = odp_client_test_uuid[0].catalog
    assert isinstance(catalog_client, OdpResourceClient)

    for item in catalog_client.list():
        assert isinstance(item, ResourceDto)

    manifest = DatasetDto(
        **{
            "kind": "catalog.hubocean.io/dataset",
            "version": "v1alpha3",
            "metadata": {
                "name": "".join(random.choices(string.ascii_lowercase + string.digits, k=20)),
                "labels": {"test_uuid": odp_client_test_uuid[1]},
            },
            "spec": {
                "storage_controller": "registry.hubocean.io/storageController/storage-tabular",
                "storage_class": "registry.hubocean.io/storageClass/tabular",
                "maintainer": {"contact": "Just Me <raw_client_example@hubocean.earth>"},  # <-- strict syntax here
            },
        }
    )

    manifest = catalog_client.create(manifest)
    assert isinstance(manifest.spec, DatasetSpec)

    fetched_manifest = catalog_client.get(manifest.metadata.uuid, tp=DatasetDto)
    assert isinstance(fetched_manifest.spec, DatasetSpec)
