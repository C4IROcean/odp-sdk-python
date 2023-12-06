import json
from uuid import uuid4

import pytest
import responses

from odp_sdk.dto.file_dto import FileMetadataDto
from odp_sdk.dto.resource_dto import MetadataDto, ResourceDto
from odp_sdk.exc import OdpFileNotFoundError
from odp_sdk.raw_storage_client import OdpRawStorageClient


@pytest.fixture()
def raw_storage_client(http_client) -> OdpRawStorageClient:
    return OdpRawStorageClient(http_client=http_client, raw_storage_endpoint="/data")


def test_get_file_metadata_success(raw_storage_client):
    uuid = uuid4()
    name = "test_dataset"
    kind = "catalog.hubocean.io/dataset"
    version = "v1alpha3"

    file_ref = "/home/hubocean/file.zip"

    file_metadata_name = "file.zip"
    file_metadata_mime_type = "application/zip"

    resource_dto = ResourceDto(
        kind=kind,
        version=version,
        metadata=MetadataDto(name=name, uuid=uuid),
        spec=dict(
            storage_class="registry.hubocean.io/storageClass/raw",
            maintainer={"organization": "HUB Ocean"},
            documentation=["https://oceandata.earth"],
            tags=["test", "hubocean"],
        ),
    )

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            f"{raw_storage_client.raw_storage_url}/{uuid}/{file_ref}/metadata",
            body=FileMetadataDto(
                name=file_metadata_name,
                mime_type=file_metadata_mime_type,
            ).model_dump_json(),
            status=200,
            content_type="application/json",
        )

        result = raw_storage_client.get_file_metadata(resource_dto, file_ref)

        assert result.name == file_metadata_name
        assert result.mime_type == file_metadata_mime_type


def test_get_file_metadata_not_found(raw_storage_client):
    uuid = uuid4()
    name = "test_dataset"
    kind = "catalog.hubocean.io/dataset"
    version = "v1alpha3"

    file_ref = "non_existent_file_ref"

    resource_dto = ResourceDto(
        kind=kind,
        version=version,
        metadata=MetadataDto(name=name, uuid=uuid),
        spec=dict(
            storage_class="registry.hubocean.io/storageClass/raw",
            maintainer={"organization": "HUB Ocean"},
            documentation=["https://oceandata.earth"],
            tags=["test", "hubocean"],
        ),
    )

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            f"{raw_storage_client.raw_storage_url}/{uuid}/{file_ref}/metadata",
            status=404,
        )

        with pytest.raises(OdpFileNotFoundError):
            raw_storage_client.get_file_metadata(resource_dto, file_ref)


def test_list_files_success(raw_storage_client):
    uuid = uuid4()
    name = "test_dataset"
    kind = "catalog.hubocean.io/dataset"
    version = "v1alpha3"

    resource_dto = ResourceDto(
        kind=kind,
        version=version,
        metadata=MetadataDto(name=name, uuid=uuid),
        spec=dict(
            storage_class="registry.hubocean.io/storageClass/raw",
            maintainer={"organization": "HUB Ocean"},
            documentation=["https://oceandata.earth"],
            tags=["test", "hubocean"],
        ),
    )

    file_metadata_name = "file.zip"
    file_metadata_mime_type = "application/zip"
    file_metadata = FileMetadataDto(
        name=file_metadata_name,
        mime_type=file_metadata_mime_type,
    )

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            f"{raw_storage_client.raw_storage_url}/{uuid}/list",
            json=[json.loads(file_metadata.model_dump_json())],
            status=200,
            content_type="application/json",
        )

        result = raw_storage_client.list_files(resource_dto)

        assert result[0].name == file_metadata_name
        assert result[0].mime_type == file_metadata_mime_type
