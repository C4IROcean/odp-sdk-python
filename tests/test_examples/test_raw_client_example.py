import os
import random
import string
from typing import Tuple
from uuid import UUID

from odp_sdk.client import OdpClient
from odp_sdk.dto import ResourceDto
from odp_sdk.dto.file_dto import FileMetadataDto


def test_raw_client(odp_client_test_uuid: Tuple[OdpClient, UUID]):
    my_dataset = ResourceDto(
        **{
            "kind": "catalog.hubocean.io/dataset",
            "version": "v1alpha3",
            "metadata": {
                "name": "".join(random.choices(string.ascii_lowercase + string.digits, k=20)),
                "labels": {"test_uuid": odp_client_test_uuid[1]},
            },
            "spec": {
                "storage_controller": "registry.hubocean.io/storageController/storage-raw-cdffs",
                "storage_class": "registry.hubocean.io/storageClass/raw",
                "maintainer": {"contact": "Just Me <raw_client_example@hubocean.earth>"},  # <-- strict syntax here
            },
        }
    )

    my_dataset = odp_client_test_uuid[0].catalog.create(my_dataset)
    assert isinstance(my_dataset, ResourceDto)

    file_dto = odp_client_test_uuid[0].raw.create_file(
        resource_dto=my_dataset,
        file_metadata_dto=FileMetadataDto(**{"name": "test.txt", "mime_type": "text/plain"}),
        contents=b"Hello, World!",
    )

    for file in odp_client_test_uuid[0].raw.list(my_dataset):
        assert isinstance(file, FileMetadataDto)
    assert odp_client_test_uuid[0].raw.list(my_dataset) != []

    odp_client_test_uuid[0].raw.download_file(my_dataset, file_dto, "test.txt")
    assert os.path.exists("test.txt")
