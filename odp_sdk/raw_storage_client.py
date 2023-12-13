from io import BytesIO
from typing import Iterable, List, Optional
from uuid import UUID

import requests
from pydantic import BaseModel

from odp_sdk.dto import ResourceDto
from odp_sdk.dto.file_dto import FileMetadataDto
from odp_sdk.exc import OdpFileNotFoundError, OdpValidationError
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

    def _construct_url(self, dataset_reference, endpoint: str = "") -> str:
        kind = "/catalog.hubocean.io/dataset"
        if isinstance(dataset_reference, UUID):
            return f"{self.raw_storage_url}/{dataset_reference}{endpoint}"
        elif isinstance(dataset_reference, ResourceDto) and dataset_reference.metadata.uuid:
            return f"{self.raw_storage_url}/{dataset_reference.metadata.uuid}{endpoint}"
        elif isinstance(dataset_reference, ResourceDto):
            return f"{self.raw_storage_url}{kind}/{dataset_reference.metadata.name}{endpoint}"
        else:
            return f"{self.raw_storage_url}{kind}/{dataset_reference}{endpoint}"

    def get_file_metadata(self, dataset_reference: str | ResourceDto | UUID, filename: str) -> FileMetadataDto:
        """
        Get file metadata by reference.

        Args:
            dataset_reference: Dataset manifest or name of dataset or UUID of dataset
            filename: File name in dataset to get metadata for

        Returns:
            The metadata of the file corresponding to the reference

        Raises:
            OdpFileNotFoundError: If the file does not exist
        """

        url = self._construct_url(dataset_reference, endpoint=f"/{filename}/metadata")

        response = self.http_client.get(url)
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if response.status_code == 404:
                raise OdpFileNotFoundError(f"File not found: {filename}") from e
            raise  # Unhandled error

        return FileMetadataDto(**response.json())

    def list(
        self, dataset_reference: str | ResourceDto | UUID, metadata_filter: dict[str, any]
    ) -> Iterable[FileMetadataDto]:
        """
        List all files in a dataset.

        Args:
            dataset_reference: Dataset manifest or name of dataset or UUID of dataset
            metadata_filter: List filter

        Returns:
            List of files in the dataset
        """

        metadata_filter = FileMetadataDto(**metadata_filter)

        while True:
            page, cursor = self.list_paginated(dataset_reference, metadata_filter=metadata_filter)
            yield from page
            if not cursor:
                break

    def list_paginated(
        self,
        dataset_reference: str | ResourceDto | UUID,
        metadata_filter: FileMetadataDto,
        cursor: Optional[str] = None,
        limit: int = 1000,
    ) -> tuple[List[FileMetadataDto], str]:
        """
        List page

        Args:
            dataset_reference: Dataset reference
            metadata_filter: List filter
            cursor: Optional cursor for pagination
            limit: Optional limit for pagination

        Returns:
            Page of return values
        """

        url = self._construct_url(dataset_reference, endpoint="/list")
        params = {}

        if cursor:
            params["page"] = cursor
        if limit:
            params["limit"] = limit

        response = self.http_client.post(url, params=params, content=metadata_filter.model_dump_json())

        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if response.status_code == 401:
                raise OdpValidationError("API argument error") from e
            raise  # Unhandled error

        content = response.json()
        return [FileMetadataDto(**item) for item in content["results"]], content.get("next")

    def upload_file(
        self, dataset_reference: str | ResourceDto | UUID, file_meta_dto: FileMetadataDto, contents: bytes | BytesIO
    ) -> FileMetadataDto:
        """
        Upload data to a file.

        Args:
            dataset_reference: Dataset manifest or name of dataset or UUID of dataset
            file_meta_dto: File metadata
            contents: File contents

        Returns:
            The metadata of the uploaded file
        """
        filename = file_meta_dto.ref
        url = self._construct_url(dataset_reference, endpoint=f"/{filename}")

        if isinstance(contents, bytes):
            contents = BytesIO(contents)

        # Ensure contents is in the correct format (bytes)
        if isinstance(contents, BytesIO):
            contents = contents.read()

        headers = {"Content-Type": "application/octet-stream"}

        response = self.http_client.post(url, headers=headers, content=contents)
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if response.status_code == 404:
                raise OdpFileNotFoundError(f"File not found: {filename}") from e

        return self.get_file_metadata(dataset_reference, file_meta_dto.name)

    def create_file(
        self,
        dataset_reference: str | ResourceDto | UUID,
        file_meta: Optional[dict[str, any] | FileMetadataDto] = None,
        filename: str = None,
        mime_type: str = None,
        contents: Optional[bytes | BytesIO] = None,
        overwrite: bool = False,
    ) -> FileMetadataDto:
        """
        Create a new file.

        Args:
            dataset_reference: Dataset manifest or name of dataset or UUID of dataset
            file_meta: File metadata
            filename: Optional way to specify the filename
            mime_type: Optional way to specify the MIME type,
            contents: File contents
            overwrite: Overwrite if the file already exists

        Returns:
            The metadata of the newly created file
        """
        if isinstance(file_meta, FileMetadataDto):
            file_meta_dto = file_meta
        elif isinstance(file_meta, dict):
            file_meta_dto = FileMetadataDto(**file_meta)
        elif filename and mime_type:
            file_meta_dto = FileMetadataDto(filename=filename, mime_type=mime_type)
        else:
            raise ValueError("You must provide either 'file_meta' or both 'filename' and 'mime_type'")

        url = self._construct_url(dataset_reference, endpoint=f"/{file_meta_dto.name}")
        response = self.http_client.post(url, content=file_meta_dto.model_dump_json(exclude_unset=True))

        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if response.status_code == 401:
                raise OdpValidationError("API argument error") from e
            raise  # Unhandled error

        file_meta = FileMetadataDto(**response.json())

        if contents:
            return self.update_file(file_meta, contents, overwrite)

        return self.get_file_metadata(dataset_reference, file_meta_dto.name)

    def download_file(
        self, dataset_reference: str | ResourceDto | UUID, file: FileMetadataDto | str, save_path: str = None
    ):
        """
        Download a file.

        Args:
            dataset_reference: Dataset manifest or name of dataset or UUID of dataset
            file: File metadata or file name
            save_path: File path to save the downloaded file to
        """

        filename = file.name

        if isinstance(file, str):
            filename = file

        url = self._construct_url(dataset_reference, endpoint=f"/{filename}")

        response = self.http_client.get(url)
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if response.status_code == 404:
                raise OdpFileNotFoundError(f"File not found: {filename}") from e

        if save_path:
            with open(save_path, "wb") as file:
                file.write(response.content)
        else:
            return response.content

    def delete_file(self, dataset_reference: str | ResourceDto | UUID, filename: str) -> bool:
        """
        Delete a file.

        Args:
            dataset_reference: Dataset manifest or name of dataset or UUID of dataset
            filename: File name in dataset to delete

        Returns:
            True if the file was deleted, False otherwise
        """
        url = self._construct_url(dataset_reference, endpoint=f"/{filename}")

        response = self.http_client.delete(url)

        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if response.status_code == 404:
                raise OdpFileNotFoundError(f"File not found: {filename}") from e

        return True
