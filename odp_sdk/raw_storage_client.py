from typing import List

import requests
from pydantic import BaseModel

from odp_sdk.dto import ResourceDto
from odp_sdk.dto.file_dto import FileMetadataDto
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

    def get_file_metadata(self, resource_dto: ResourceDto, file_ref: str) -> FileMetadataDto:
        """
        Get file metadata by reference.

        Args:
            resource_dto: ResourceDto
            file_ref: File reference

        Returns:
            The metadata of the file corresponding to the reference

        Raises:
            OdpFileNotFoundError: If the file does not exist
        """
        url = f"{self.raw_storage_url}/{resource_dto.metadata.uuid}/{file_ref}/metadata"

        response = self.http_client.get(url)
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if response.status_code == 404:
                raise OdpFileNotFoundError(f"File not found: {file_ref}") from e

        # Assuming the response returns JSON that matches the FileMetadata schema
        return FileMetadataDto(**response.json())

    def list_files(self, resource_dto: ResourceDto) -> List[FileMetadataDto]:
        """
        List all files in a dataset.

        Args:
            resource_dto: ResourceDto

        Returns:
            List of files in the dataset
        """
        url = f"{self.raw_storage_url}/{resource_dto.metadata.uuid}/list"

        response = self.http_client.post(url)
        response.raise_for_status()

        return [FileMetadataDto(**file_metadata) for file_metadata in response.json()]
