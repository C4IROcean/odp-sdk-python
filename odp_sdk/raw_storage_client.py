from io import BytesIO
from typing import Optional, Dict, List, Literal, Union
from uuid import UUID

import requests
from pydantic import BaseModel

from odp_sdk.dto.dataset_dto import DatasetDto
from odp_sdk.exc import OdpFileNotFoundError
from odp_sdk.http_client import OdpHttpClient


class FileMetadata(BaseModel):
    """File Metadata Model."""

    external_id: Optional[str] = None
    name: Optional[str] = None
    source: Optional[str] = None
    mime_type: Optional[str] = None
    metadata: Optional[Dict[str, str]] = None
    directory: Optional[str] = None
    asset_ids: Optional[List[int]] = None
    data_set_id: Optional[int] = None
    labels: Optional[List[str]] = None
    geo_location: Optional[str] = None
    source_created_time: Optional[int] = None
    source_modified_time: Optional[int] = None
    security_categories: Optional[List[int]] = None
    id: Optional[int] = None
    uploaded: Optional[bool] = None
    uploaded_time: Optional[int] = None
    created_time: Optional[int] = None
    last_updated_time: Optional[int] = None


class OdpRawStorageClient(BaseModel):
    http_client: OdpHttpClient
    raw_storage_endpoint: str

    @property
    def raw_storage_url(self) -> str:
        """The URL of the raw storage endpoint, including the base URL.

        Returns:
            The raw storage URL
        """
        return f"{self.http_client.base_url}{self.raw_storage_endpoint}"

    def get_file_metadata(self, dataset_dto: DatasetDto, file_ref: str) -> FileMetadata:
        """
        Get file metadata by reference.

        Args:
            dataset_dto: DatasetDto
            file_ref: File reference

        Returns:
            The metadata of the file corresponding to the reference

        Raises:
            OdpFileNotFoundError: If the file does not exist
        """
        url = f"{self.raw_storage_url}/{dataset_dto.metadata.uuid}/{file_ref}/metadata"

        response = self.http_client.get(url)
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if response.status_code == 404:
                raise OdpFileNotFoundError(f"File not found: {file_ref}") from e

        # Assuming the response returns JSON that matches the FileMetadata schema
        return FileMetadata(**response.json())

    def list_files(self, dataset_dto: DatasetDto) -> List[FileMetadata]:
        """
        List all files in a dataset.

        Args:
            dataset_dto: DatasetDto

        Returns:
            List of files in the dataset
        """
        url = f"{self.raw_storage_url}/{dataset_dto.metadata.uuid}/list"

        response = self.http_client.post(url)
        response.raise_for_status()

        return [FileMetadata(**file_metadata) for file_metadata in response.json()]

