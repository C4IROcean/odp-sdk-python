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

    file_meta = FileMetadataDto(file_metadata_name="file.zip", file_metadata_mime_type="application/zip")

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            f"{raw_storage_client.raw_storage_url}/{common_resource_dto.metadata.uuid}/{file_ref}/metadata",
            body=file_meta.model_dump_json(),
            status=200,
            content_type="application/json",
        )

        result = raw_storage_client.get_file_metadata(common_resource_dto, file_ref)

        assert result.name == file_meta.name
        assert result.mime_type == file_meta.mime_type


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
            json={"results": [json.loads(file_metadata.model_dump_json())], "next": None, "num_results": 1},
            status=200,
            content_type="application/json",
        )

        metadata_filter = {"name": file_metadata_name}

        result = raw_storage_client.list(common_resource_dto, metadata_filter=metadata_filter)

        first_item = next(iter(result))

        assert first_item.name == file_metadata_name
        assert first_item.mime_type == file_metadata_mime_type


def test_create_file_success(raw_storage_client, common_resource_dto):
    file_name = "new_file.txt"
    file_metadata = FileMetadataDto(
        name=file_name,
        mime_type="text/plain",
    )

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            f"{raw_storage_client.raw_storage_url}/{common_resource_dto.metadata.uuid}/{file_metadata.name}",
            status=200,
            json=json.loads(file_metadata.model_dump_json()),
            content_type="application/json",
        )

        rsps.add(
            responses.GET,
            f"{raw_storage_client.raw_storage_url}/{common_resource_dto.metadata.uuid}/{file_name}/metadata",
            json=json.loads(file_metadata.model_dump_json()),
            status=200,
            content_type="application/json",
        )

        result = raw_storage_client.create_file(common_resource_dto, file_meta=file_metadata, contents=None)

        assert result.name == file_name
        assert result.mime_type == "text/plain"


def test_download_file_save(raw_storage_client, common_resource_dto, tmp_path):
    file_data = b"Sample file content"
    save_path = tmp_path / "downloaded_file.txt"

    file_meta_data = FileMetadataDto(name="test_file.txt", mime_type="text/plain")

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            f"{raw_storage_client.raw_storage_url}/{common_resource_dto.metadata.uuid}/{file_meta_data.name}",
            body=file_data,
            status=200,
        )

        raw_storage_client.download_file(common_resource_dto, file_meta_data, save_path=str(save_path))

        with open(save_path, "rb") as file:
            saved_data = file.read()

    assert saved_data == file_data


def test_delete_file_success(raw_storage_client, common_resource_dto):
    filename = "test_file.txt"

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.DELETE,
            f"{raw_storage_client.raw_storage_url}/{common_resource_dto.metadata.uuid}/{filename}",
            status=200,  # Assuming status code 200 indicates successful deletion
        )

        result = raw_storage_client.delete_file(common_resource_dto, filename)

    assert result is True


def test_delete_file_not_found(raw_storage_client, common_resource_dto):
    filename = "test_file.txt"

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.DELETE,
            f"{raw_storage_client.raw_storage_url}/{common_resource_dto.metadata.uuid}/{filename}",
            status=404,  # Assuming status code 404 indicates file not found
        )

        with pytest.raises(OdpFileNotFoundError):
            raw_storage_client.delete_file(common_resource_dto, filename)
