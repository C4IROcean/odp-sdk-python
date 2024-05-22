import random
import string
from typing import Tuple
from uuid import UUID

from odp_sdk.client import OdpClient
from odp_sdk.dto import ResourceDto


def test_get_catalog(odp_client: OdpClient):
    resource_manifest = odp_client.catalog.get(
        "catalog.hubocean.io/dataCollection/1e3401d4-9630-40cd-a9cf-d875cb310449-glodap"
    )

    assert isinstance(resource_manifest, ResourceDto)

    same_resource_manifest = odp_client.catalog.get(resource_manifest.metadata.uuid)

    assert isinstance(same_resource_manifest, ResourceDto)


def test_list_catalog(odp_client: OdpClient):
    oqs_filter = {
        "#EQUALS": [  # EQUALS is used here to compare to values
            "$kind",  # The first value is the kind from the metadata, prefaced with a dollarsign.
            "catalog.hubocean.io/dataCollection",  # And this is the value to compare with
        ]
    }

    list_elements = [data_collection for data_collection in odp_client.catalog.list(oqs_filter=oqs_filter)]
    assert list_elements != []


def test_create_catalog(odp_client_owner: Tuple[OdpClient, UUID]):
    # ResourceDto
    manifest = ResourceDto(
        **{
            "kind": "catalog.hubocean.io/dataset",
            "version": "v1alpha3",
            "metadata": {
                "name": "".join(random.choices(string.ascii_lowercase + string.digits, k=20)),
                "owner": odp_client_owner[1],
            },
            "spec": {
                "storage_controller": "registry.hubocean.io/storageController/storage-tabular",
                "storage_class": "registry.hubocean.io/storageClass/tabular",
                "maintainer": {"contact": "Just Me <raw_client_example@hubocean.earth>"},  # <-- strict syntax here
            },
        }
    )

    assert isinstance(odp_client_owner[0].catalog.create(manifest), ResourceDto)