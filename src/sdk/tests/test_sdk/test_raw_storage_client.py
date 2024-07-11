import json
import uuid
from datetime import datetime
from pathlib import Path

import pytest
import responses
from odp.client.dto.file_dto import FileMetadataDto
from odp.client.exc import OdpFileNotFoundError
from odp.client.http_client import OdpHttpClient
from odp.client.raw_storage_client import OdpRawStorageClient
from odp.dto import DatasetDto


@pytest.fixture()
def raw_storage_client(http_client: OdpHttpClient) -> OdpRawStorageClient:
    return OdpRawStorageClient(http_client=http_client, raw_storage_endpoint="/data")


def test_get_file_metadata_success(
    raw_storage_client: OdpRawStorageClient, raw_resource_dto: DatasetDto, request_mock: responses.RequestsMock
):
    rand_uuid = uuid.uuid4()
    time_now = datetime.now()
    file_meta = FileMetadataDto(
        name="file.zip",
        mime_type="application/zip",
        dataset=rand_uuid,
        metadata={"name": "sdk-raw-example"},
        geo_location="Somewhere",
        size_bytes=123456789,
        checksum="asdf",
        created_time=time_now,
        modified_time=time_now,
        deleted_time=time_now,
    )

    request_mock.add(
        responses.GET,
        f"{raw_storage_client.raw_storage_url}/{raw_resource_dto.metadata.uuid}/{file_meta.name}/metadata",
        body=file_meta.model_dump_json(),
        status=200,
        content_type="application/json",
    )

    result = raw_storage_client.get_file_metadata(raw_resource_dto, file_meta)

    assert result.name == "file.zip"
    assert result.mime_type == "application/zip"
    assert result.dataset == rand_uuid
    assert result.metadata == {"name": "sdk-raw-example"}
    assert result.geo_location == "Somewhere"
    assert result.size_bytes == 123456789
    assert result.checksum == "asdf"
    assert result.created_time == time_now
    assert result.modified_time == time_now
    assert result.deleted_time == time_now


def test_get_file_metadata_not_found(
    raw_storage_client: OdpRawStorageClient,
    raw_resource_dto: DatasetDto,
    request_mock: responses.RequestsMock,
):
    file_meta = FileMetadataDto(name="file.zip", mime_type="application/zip")

    request_mock.add(
        responses.GET,
        f"{raw_storage_client.raw_storage_url}/{raw_resource_dto.metadata.uuid}/{file_meta.name}/metadata",
        status=404,
    )

    with pytest.raises(OdpFileNotFoundError):
        raw_storage_client.get_file_metadata(raw_resource_dto, file_meta)


def test_list_files_success(
    raw_storage_client: OdpRawStorageClient,
    raw_resource_dto: DatasetDto,
    request_mock: responses.RequestsMock,
):
    file_metadata = FileMetadataDto(name="file.zip", mime_type="application/zip")

    request_mock.add(
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

    result = raw_storage_client.list(raw_resource_dto, metadata_filter=metadata_filter)

    first_item = next(iter(result))

    assert first_item.name == file_metadata.name
    assert first_item.mime_type == file_metadata.mime_type


def test_create_file_success(
    raw_storage_client: OdpRawStorageClient,
    raw_resource_dto: DatasetDto,
    request_mock: responses.RequestsMock,
):
    file_metadata = FileMetadataDto(
        name="new_file.txt",
        mime_type="text/plain",
    )

    request_mock.add(
        responses.POST,
        f"{raw_storage_client.raw_storage_url}/{raw_resource_dto.metadata.uuid}",
        status=200,
        json=json.loads(file_metadata.model_dump_json()),
        content_type="application/json",
    )

    request_mock.add(
        responses.GET,
        f"{raw_storage_client.raw_storage_url}/{raw_resource_dto.metadata.uuid}/{file_metadata.name}/metadata",
        json=json.loads(file_metadata.model_dump_json()),
        status=200,
        content_type="application/json",
    )

    result = raw_storage_client.create_file(raw_resource_dto, file_metadata_dto=file_metadata, contents=None)

    assert result.name == file_metadata.name
    assert result.mime_type == "text/plain"


def test_download_file_save(
    raw_storage_client: OdpRawStorageClient,
    raw_resource_dto: DatasetDto,
    tmp_path: Path,
    request_mock: responses.RequestsMock,
):
    file_data = b"Sample file content"
    save_path = tmp_path / "downloaded_file.txt"

    file_metadata = FileMetadataDto(name="test_file.txt", mime_type="text/plain")

    request_mock.add(
        responses.GET,
        f"{raw_storage_client.raw_storage_url}/{raw_resource_dto.metadata.uuid}/{file_metadata.name}",
        body=file_data,
        status=200,
    )

    raw_storage_client.download_file(raw_resource_dto, file_metadata, save_path=str(save_path))

    with open(save_path, "rb") as file:
        saved_data = file.read()

    assert saved_data == file_data


def test_delete_file_not_found(
    raw_storage_client: OdpRawStorageClient,
    raw_resource_dto: DatasetDto,
    request_mock: responses.RequestsMock,
):
    file_metadata = FileMetadataDto(name="test_file.txt", mime_type="text/plain")

    request_mock.add(
        responses.DELETE,
        f"{raw_storage_client.raw_storage_url}/{raw_resource_dto.metadata.uuid}/{file_metadata.name}",
        status=404,  # Assuming status code 404 indicates file not found
    )

    with pytest.raises(OdpFileNotFoundError):
        raw_storage_client.delete_file(raw_resource_dto, file_metadata)
