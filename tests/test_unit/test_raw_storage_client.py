import json

import pytest
import responses

from odp_sdk.dto.file_dto import FileMetadataDto
from odp_sdk.exc import OdpFileNotFoundError
from odp_sdk.raw_storage_client import OdpRawStorageClient


@pytest.fixture()
def raw_storage_client(http_client) -> OdpRawStorageClient:
    return OdpRawStorageClient(http_client=http_client, raw_storage_endpoint="/data")


def test_get_file_metadata_success(raw_storage_client, raw_resource_dto):
    file_meta = FileMetadataDto(
        file_metadata_name="file.zip", file_metadata_mime_type="application/zip"
    )

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            f"{raw_storage_client.raw_storage_url}/{raw_resource_dto.metadata.uuid}/{file_meta.name}/metadata",
            body=file_meta.model_dump_json(),
            status=200,
            content_type="application/json",
        )

        result = raw_storage_client.get_file_metadata(raw_resource_dto, file_meta)

        assert result.name == file_meta.name
        assert result.mime_type == file_meta.mime_type


def test_get_file_metadata_not_found(raw_storage_client, raw_resource_dto):
    file_meta = FileMetadataDto(
        file_metadata_name="file.zip", file_metadata_mime_type="application/zip"
    )

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            f"{raw_storage_client.raw_storage_url}/{raw_resource_dto.metadata.uuid}/{file_meta.name}/metadata",
            status=404,
        )

        with pytest.raises(OdpFileNotFoundError):
            raw_storage_client.get_file_metadata(raw_resource_dto, file_meta)


def test_list_files_success(raw_storage_client, raw_resource_dto):
    file_metadata = FileMetadataDto(
        name="file.zip",
        mime_type="application/zip",
    )

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            f"{raw_storage_client.raw_storage_url}/{raw_resource_dto.metadata.uuid}/list",
            json={
                "results": [json.loads(file_metadata.model_dump_json())],
                "next": None,
                "num_results": 1,
            },
            status=200,
            content_type="application/json",
        )

        metadata_filter = {"name": file_metadata.name}

        result = raw_storage_client.list(
            raw_resource_dto, metadata_filter=metadata_filter
        )

        first_item = next(iter(result))

        assert first_item.name == file_metadata.name
        assert first_item.mime_type == file_metadata.mime_type


def test_create_file_success(raw_storage_client, raw_resource_dto):
    file_metadata = FileMetadataDto(
        name="new_file.txt",
        mime_type="text/plain",
    )

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            f"{raw_storage_client.raw_storage_url}/{raw_resource_dto.metadata.uuid}",
            status=200,
            json=json.loads(file_metadata.model_dump_json()),
            content_type="application/json",
        )

        rsps.add(
            responses.GET,
            f"{raw_storage_client.raw_storage_url}/{raw_resource_dto.metadata.uuid}/{file_metadata.name}/metadata",
            json=json.loads(file_metadata.model_dump_json()),
            status=200,
            content_type="application/json",
        )

        result = raw_storage_client.create_file(
            raw_resource_dto, file_metadata_dto=file_metadata, contents=None
        )

        assert result.name == file_metadata.name
        assert result.mime_type == "text/plain"


def test_download_file_save(raw_storage_client, raw_resource_dto, tmp_path):
    file_data = b"Sample file content"
    save_path = tmp_path / "downloaded_file.txt"

    file_metadata = FileMetadataDto(name="test_file.txt", mime_type="text/plain")

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            f"{raw_storage_client.raw_storage_url}/{raw_resource_dto.metadata.uuid}/{file_metadata.name}",
            body=file_data,
            status=200,
        )

        raw_storage_client.download_file(
            raw_resource_dto, file_metadata, save_path=str(save_path)
        )

        with open(save_path, "rb") as file:
            saved_data = file.read()

    assert saved_data == file_data


def test_delete_file_not_found(raw_storage_client, raw_resource_dto):
    file_metadata = FileMetadataDto(name="test_file.txt", mime_type="text/plain")

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.DELETE,
            f"{raw_storage_client.raw_storage_url}/{raw_resource_dto.metadata.uuid}/{file_metadata.name}",
            status=404,  # Assuming status code 404 indicates file not found
        )

        with pytest.raises(OdpFileNotFoundError):
            raw_storage_client.delete_file(raw_resource_dto, file_metadata)
