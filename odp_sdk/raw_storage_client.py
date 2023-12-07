from io import BytesIO
from typing import List, Optional

import requests
from pydantic import BaseModel

from odp_sdk.dto import ResourceDto
from odp_sdk.dto.file_dto import FileMetadataDto
from odp_sdk.dto.pagination_dto import PaginationDto
from odp_sdk.exc import OdpFileNotFoundError
from odp_sdk.http_client import OdpHttpClient


class OdpRawStorageClient(BaseModel):
    http_client: OdpHttpClient
    raw_storage_endpoint: str = "/data"

    @property
    def raw_storage_url(self) -> str:
        """The URL of the raw storage endpoint, including the base URL.

        Returns:
            The raw storage URL
        """
        return f"{self.http_client.base_url}{self.raw_storage_endpoint}"

    def get_file_metadata(self, resource_dto: ResourceDto, file_name: str) -> FileMetadataDto:
        """
        Get file metadata by reference.

        Args:
            resource_dto: Dataset manifest
            file_name: File name in dataset to get metadata for

        Returns:
            The metadata of the file corresponding to the reference

        Raises:
            OdpFileNotFoundError: If the file does not exist
        """

        if resource_dto.metadata.uuid:
            url = f"{self.raw_storage_url}/{resource_dto.metadata.uuid}/{file_name}/metadata"
        else:
            url = (
                f"{self.raw_storage_url}/catalog.hubocean.io/dataset/{resource_dto.metadata.name}/{file_name}/metadata"
            )

        response = self.http_client.get(url)
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if response.status_code == 404:
                raise OdpFileNotFoundError(f"File not found: {file_name}") from e

        return FileMetadataDto(**response.json())

    def list_files(self, resource_dto: ResourceDto, **kwargs) -> List[FileMetadataDto]:
        """
        List all files in a dataset.

        Args:
            resource_dto: Dataset manifest
            **kwargs: Keyword arguments for filtering

        Returns:
            List of files in the dataset
        """
        if resource_dto.metadata.uuid:
            url = f"{self.raw_storage_url}/{resource_dto.metadata.uuid}/list"
        else:
            url = f"{self.raw_storage_url}/catalog.hubocean.io/dataset/{resource_dto.metadata.name}/list"

        file_filter_body = FileMetadataDto(**kwargs)

        response = self.http_client.post(url, content=file_filter_body)
        response.raise_for_status()

        return PaginationDto(**response.json()).results

    def upload_file(
        self, resource_dto: ResourceDto, file_meta_dto: FileMetadataDto, contents: bytes | BytesIO
    ) -> FileMetadataDto:
        """
        Upload a file.

        Args:
            resource_dto: Dataset manifest
            file_meta_dto: File metadata
            contents: File contents

        Returns:
            The metadata of the uploaded file
        """
        if resource_dto.metadata.uuid:
            url = f"{self.raw_storage_url}/{resource_dto.metadata.uuid}/{file_meta_dto.ref}"
        else:
            url = f"{self.raw_storage_url}/catalog.hubocean.io/dataset/{resource_dto.metadata.name}/{file_meta_dto.ref}"

        if isinstance(contents, bytes):
            contents = BytesIO(contents)

        # Ensure contents is in the correct format (bytes)
        if isinstance(contents, BytesIO):
            contents = contents.read()

        headers = {"Content-Type": "application/octet-stream"}

        response = self.http_client.post(url, headers=headers, content=contents)
        response.raise_for_status()

        return self.get_file_metadata(resource_dto, file_meta_dto.name)

    def create_file(
        self,
        resource_dto: ResourceDto,
        file_meta_dto: FileMetadataDto,
        contents: Optional[bytes | BytesIO],
        overwrite: bool = False,
    ) -> FileMetadataDto:
        """
        Create a new file.

        Args:
            resource_dto: Dataset manifest
            file_meta_dto: File metadata
            contents: File contents
            overwrite: Overwrite if the file already exists

        Returns:
            The metadata of the newly created file
        """
        if resource_dto.metadata.uuid:
            url = f"{self.raw_storage_url}/{resource_dto.metadata.uuid}/{file_meta_dto.name}"
        else:
            url = (
                f"{self.raw_storage_url}/catalog.hubocean.io/dataset/{resource_dto.metadata.name}/{file_meta_dto.name}"
            )

        response = self.http_client.post(url, content=file_meta_dto.model_dump_json())
        response.raise_for_status()

        file_meta = FileMetadataDto(**response.json())

        if contents:
            return self.update_file(file_meta, contents, overwrite)

        return self.get_file_metadata(resource_dto, file_meta_dto.name)
