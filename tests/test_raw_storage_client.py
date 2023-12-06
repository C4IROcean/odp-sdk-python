import json
from uuid import uuid4

import pytest
import responses

from odp_sdk.dto.dataset_dto import DatasetDto, DatasetSpec
from odp_sdk.dto.resource_dto import MetadataDto
from odp_sdk.exc import OdpFileNotFoundError
from odp_sdk.raw_storage_client import FileMetadata, OdpRawStorageClient


@pytest.fixture()
def raw_storage_client(http_client) -> OdpRawStorageClient:
    return OdpRawStorageClient(http_client=http_client, raw_storage_endpoint="/data")


def test_get_file_metadata_success(raw_storage_client):
    dataset_uuid = uuid4()
    dataset_name = "test_dataset"
    dataset_kind = "catalog.hubocean.io/dataset"
    dataset_version = "v1alpha3"

    dataset_spec = DatasetSpec(
        storage_class="registry.hubocean.io/storageClass/raw",
        maintainer={"organization": "HUB Ocean"},
        documentation=["https://oceandata.earth"],
        tags=["test", "hubocean"],
    )

    file_ref = "/home/hubocean/file.zip"

    file_metadata_name = "file.zip"
    file_metadata_mime_type = "application/zip"

    dataset_dto = DatasetDto(
        spec=dataset_spec,
        kind=dataset_kind,
        version=dataset_version,
        metadata=MetadataDto(name=dataset_name, uuid=dataset_uuid),
    )

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            f"{raw_storage_client.raw_storage_url}/{dataset_uuid}/{file_ref}/metadata",
            body=FileMetadata(
                name=file_metadata_name,
                mime_type=file_metadata_mime_type,
            ).model_dump_json(),
            status=200,
            content_type="application/json",
        )

        result = raw_storage_client.get_file_metadata(dataset_dto, file_ref)

        assert result.name == file_metadata_name
        assert result.mime_type == file_metadata_mime_type


def test_get_file_metadata_not_found(raw_storage_client):
    dataset_uuid = uuid4()
    dataset_name = "test_dataset"
    dataset_kind = "catalog.hubocean.io/dataset"
    dataset_version = "v1alpha3"
    dataset_spec = {
        "storage_class": "registry.hubocean.io/storageClass/raw",
        "maintainer": {"contact": "HUB Ocean <info@oceandata.earth>", "organisation": "HUB Ocean"},
        "documentation": ["https://oceandata.earth"],
        "tags": ["test", "hubocean"],
    }

    file_ref = "non_existent_file_ref"
    metadata = MetadataDto(name=dataset_name, uuid=dataset_uuid)

    dataset_dto = DatasetDto(spec=dataset_spec, kind=dataset_kind, version=dataset_version, metadata=metadata)

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            f"{raw_storage_client.raw_storage_url}/{dataset_uuid}/{file_ref}/metadata",
            status=404,
        )

        with pytest.raises(OdpFileNotFoundError):
            raw_storage_client.get_file_metadata(dataset_dto, file_ref)


def test_list_files_success(raw_storage_client):
    dataset_uuid = uuid4()
    dataset_name = "test_dataset"
    dataset_kind = "catalog.hubocean.io/dataset"
    dataset_version = "v1alpha3"
    dataset_spec = {
        "storage_class": "registry.hubocean.io/storageClass/raw",
        "maintainer": {"contact": "HUB Ocean", "organisation": "HUB Ocean"},
        "documentation": ["https://oceandata.earth"],
        "tags": ["test", "hubocean"],
    }

    metadata = MetadataDto(name=dataset_name, uuid=dataset_uuid)

    dataset_dto = DatasetDto(spec=dataset_spec, kind=dataset_kind, version=dataset_version, metadata=metadata)

    file_metadata_name = "file.zip"
    file_metadata_mime_type = "application/zip"
    file_metadata = FileMetadata(
        name=file_metadata_name,
        mime_type=file_metadata_mime_type,
    )

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            f"{raw_storage_client.raw_storage_url}/{dataset_uuid}/list",
            json=[json.loads(file_metadata.model_dump_json())],
            status=200,
            content_type="application/json",
        )

        result = raw_storage_client.list_files(dataset_dto)

        assert result[0].name == file_metadata_name
        assert result[0].mime_type == file_metadata_mime_type
