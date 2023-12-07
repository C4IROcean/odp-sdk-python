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

    def get_file_metadata(self, resource_dto: ResourceDto, filename: str) -> FileMetadataDto:
        """
        Get file metadata by reference.

        Args:
            resource_dto: Dataset manifest
            filename: File name in dataset to get metadata for

        Returns:
            The metadata of the file corresponding to the reference

        Raises:
            OdpFileNotFoundError: If the file does not exist
        """
        url = f"{self.raw_storage_url}/catalog.hubocean.io/dataset/{resource_dto.metadata.name}/{filename}/metadata"

        if resource_dto.metadata.uuid:
            url = f"{self.raw_storage_url}/{resource_dto.metadata.uuid}/{filename}/metadata"

        response = self.http_client.get(url)
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if response.status_code == 404:
                raise OdpFileNotFoundError(f"File not found: {filename}") from e

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
        url = f"{self.raw_storage_url}/catalog.hubocean.io/dataset/{resource_dto.metadata.name}/list"

        if resource_dto.metadata.uuid:
            url = f"{self.raw_storage_url}/{resource_dto.metadata.uuid}/list"

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
        url = f"{self.raw_storage_url}/catalog.hubocean.io/dataset/{resource_dto.metadata.name}/{file_meta_dto.ref}"

        if resource_dto.metadata.uuid:
            url = f"{self.raw_storage_url}/{resource_dto.metadata.uuid}/{file_meta_dto.ref}"

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
        url = f"{self.raw_storage_url}/catalog.hubocean.io/dataset/{resource_dto.metadata.name}/{file_meta_dto.name}"

        if resource_dto.metadata.uuid:
            url = f"{self.raw_storage_url}/{resource_dto.metadata.uuid}/{file_meta_dto.name}"

        response = self.http_client.post(url, content=file_meta_dto.model_dump_json())
        response.raise_for_status()

        file_meta = FileMetadataDto(**response.json())

        if contents:
            return self.update_file(file_meta, contents, overwrite)

        return self.get_file_metadata(resource_dto, file_meta_dto.name)

    def download_file(self, resource_dto: ResourceDto, file: [FileMetadataDto, str], save_path: str = None):
        """
        Download a file.

        Args:
            resource_dto: Dataset manifest
            file: File metadata or file name
        """

        filename = file.name

        if isinstance(file, str):
            filename = file

        url = f"{self.raw_storage_url}/catalog.hubocean.io/dataset/{resource_dto.metadata.name}/{filename}"

        if resource_dto.metadata.uuid:
            url = f"{self.raw_storage_url}/{resource_dto.metadata.uuid}/{filename}"

        response = self.http_client.get(url)
        response.raise_for_status()

        if save_path:
            with open(save_path, "wb") as file:
                file.write(response.content)
        else:
            return response.content

    def delete_file(self, resource_dto: ResourceDto, filename: str) -> bool:
        """
        Delete a file.

        Args:
            resource_dto: Dataset manifest
            filename: File name in dataset to delete

        Returns:
            True if the file was deleted, False otherwise
        """
        url = f"{self.raw_storage_url}/catalog.hubocean.io/dataset/{resource_dto.metadata.name}/{filename}"

        if resource_dto.metadata.uuid:
            url = f"{self.raw_storage_url}/{resource_dto.metadata.uuid}/{filename}"

        response = self.http_client.delete(url)

        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if response.status_code == 404:
                raise OdpFileNotFoundError(f"File not found: {filename}") from e

        return True
