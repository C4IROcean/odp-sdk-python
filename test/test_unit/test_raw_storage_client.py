import json

import pytest
import responses

from odp_sdk.dto.file_dto import FileMetadataDto
from odp_sdk.exc import OdpFileNotFoundError
from odp_sdk.raw_storage_client import OdpRawStorageClient


@pytest.fixture()
def raw_storage_client(http_client) -> OdpRawStorageClient:
    return OdpRawStorageClient(http_client=http_client, raw_storage_endpoint="/data")


def test_get_file_metadata_success(raw_storage_client, common_resource_dto):
    file_ref = "/home/hubocean/file.zip"

    file_metadata_name = "file.zip"
    file_metadata_mime_type = "application/zip"

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            f"{raw_storage_client.raw_storage_url}/{common_resource_dto.metadata.uuid}/{file_ref}/metadata",
            body=FileMetadataDto(
                name=file_metadata_name,
                mime_type=file_metadata_mime_type,
            ).model_dump_json(),
            status=200,
            content_type="application/json",
        )

        result = raw_storage_client.get_file_metadata(common_resource_dto, file_ref)

        assert result.name == file_metadata_name
        assert result.mime_type == file_metadata_mime_type


def test_get_file_metadata_not_found(raw_storage_client, common_resource_dto):
    file_ref = "non_existent_file_ref"

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            f"{raw_storage_client.raw_storage_url}/{common_resource_dto.metadata.uuid}/{file_ref}/metadata",
            status=404,
        )

        with pytest.raises(OdpFileNotFoundError):
            raw_storage_client.get_file_metadata(common_resource_dto, file_ref)


def test_list_files_success(raw_storage_client, common_resource_dto):
    file_metadata_name = "file.zip"
    file_metadata_mime_type = "application/zip"
    file_metadata = FileMetadataDto(
        name=file_metadata_name,
        mime_type=file_metadata_mime_type,
    )

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            f"{raw_storage_client.raw_storage_url}/{common_resource_dto.metadata.uuid}/list",
            json=[json.loads(file_metadata.model_dump_json())],
            status=200,
            content_type="application/json",
        )

        result = raw_storage_client.list_files(common_resource_dto)

        assert result[0].name == file_metadata_name
        assert result[0].mime_type == file_metadata_mime_type
