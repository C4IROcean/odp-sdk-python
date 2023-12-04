import pytest
import responses
from uuid import uuid4

from odp_sdk.dto.dataset_dto import DatasetDto
from odp_sdk.dto.resource_dto import MetadataDto
from odp_sdk.exc import OdpFileNotFoundError
from odp_sdk.raw_storage_client import OdpRawStorageClient, FileMetadata

@pytest.fixture()
def raw_storage_client(http_client) -> OdpRawStorageClient:
    return OdpRawStorageClient(http_client=http_client, raw_storage_endpoint="/data")

def test_get_file_metadata_success(raw_storage_client):
    dataset_uuid = uuid4()
    dataset_name = "test_dataset"
    dataset_kind = "catalog.hubocean.io/dataset"
    dataset_version = "v1alpha3"
    dataset_spec = {
        "storage_class": "registry.hubocean.io/storageClass/raw",
        "maintainer": {
            "contact": "HUB Ocean <info@oceandata.earth>",
            "organisation": "HUB Ocean"
        },
        "documentation": [
            "https://oceandata.earth"
        ],
        "tags": ["test", "hubocean"]
    }
    file_ref = "/home/hubocean/file.zip"
    file_metadata = {
        "name": "file.zip",
        "mime_type": "application/zip",
    }

    metadata = MetadataDto(name=dataset_name, uuid=dataset_uuid)

    dataset_dto = DatasetDto(spec=dataset_spec,
                             kind=dataset_kind,
                             version=dataset_version,
                             metadata=metadata)

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            f"{raw_storage_client.raw_storage_url}/{dataset_uuid}/{file_ref}/metadata",
            json=file_metadata,
            status=200,
            content_type="application/json",
        )

        result = raw_storage_client.get_file_metadata(dataset_dto, file_ref)

        assert result.name == file_metadata["name"]
        assert result.mime_type == file_metadata["mime_type"]

def test_get_file_metadata_not_found(raw_storage_client):
    dataset_uuid = uuid4()
    dataset_name = "test_dataset"
    dataset_kind = "catalog.hubocean.io/dataset"
    dataset_version = "v1alpha3"
    dataset_spec = {
        "storage_class": "registry.hubocean.io/storageClass/raw",
        "maintainer": {
            "contact": "HUB Ocean <info@oceandata.earth>",
            "organisation": "HUB Ocean"
        },
        "documentation": [
            "https://oceandata.earth"
        ],
        "tags": ["test", "hubocean"]
    }
    file_metadata = {
        "name": "file.zip",
        "mime_type": "application/zip",
    }

    file_ref = "non_existent_file_ref"
    metadata = MetadataDto(name=dataset_name, uuid=dataset_uuid)

    dataset_dto = DatasetDto(spec=dataset_spec,
                             kind=dataset_kind,
                             version=dataset_version,
                             metadata=metadata)

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            f"{raw_storage_client.raw_storage_url}/{dataset_uuid}/{file_ref}/metadata",
            status=404,
        )

        with pytest.raises(OdpFileNotFoundError):
            raw_storage_client.get_file_metadata(dataset_dto, file_ref)
