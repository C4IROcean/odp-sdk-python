from io import BytesIO
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import requests
from odp_sdk.dto import ResourceDto
from odp_sdk.dto.file_dto import FileMetadataDto
from odp_sdk.exc import OdpFileAlreadyExistsError, OdpFileNotFoundError, OdpValidationError
from odp_sdk.http_client import OdpHttpClient
from pydantic import BaseModel


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

    def _construct_url(self, resource_dto: ResourceDto, endpoint: str = "") -> str:
        if resource_dto.metadata.uuid:
            return f"{self.raw_storage_url}/{resource_dto.metadata.uuid}{endpoint}"
        else:
            return f"{self.raw_storage_url}/catalog.hubocean.io/dataset/{resource_dto.metadata.name}{endpoint}"

    def get_file_metadata(self, resource_dto: ResourceDto, file_metadata_dto: FileMetadataDto) -> FileMetadataDto:
        """Get file metadata by reference.

        Args:
            resource_dto: Dataset manifest
            file_metadata_dto: File metadata to retrieve

        Returns:
            The metadata of the file corresponding to the reference
                requests.HTTPError(f"HTTP Error - {response.status_code}: {response.text}")

        Raises:
            OdpFileNotFoundError: If the file does not exist
        """

        url = self._construct_url(resource_dto, endpoint=f"/{file_metadata_dto.name}/metadata")

        response = self.http_client.get(url)
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if response.status_code == 404:
                raise OdpFileNotFoundError(f"File not found: {file_metadata_dto.name}") from e
            raise requests.HTTPError(f"HTTP Error - {response.status_code}: {response.text}")

        return FileMetadataDto(**response.json())

    def list(
        self, resource_dto: ResourceDto, metadata_filter: Optional[Dict[str, Any]] = None
    ) -> Iterable[FileMetadataDto]:
        """List all files in a dataset.

        Args:
            resource_dto: Dataset manifest
            metadata_filter: List filter

        Returns:
            List of files in the dataset
        """

        while True:
            page, cursor = self.list_paginated(resource_dto, metadata_filter=metadata_filter)
            yield from page
            if not cursor:
                break

    def list_paginated(
        self,
        resource_dto: ResourceDto,
        metadata_filter: Optional[Dict[str, Any]] = None,
        cursor: Optional[str] = None,
        limit: int = 1000,
    ) -> Tuple[List[FileMetadataDto], str]:
        """List page

        Args:
            resource_dto: Dataset manifest
            metadata_filter: List filter
            cursor: Optional cursor for pagination
            limit: Optional limit for pagination

        Returns:
            Page of return values
        """

        url = self._construct_url(resource_dto, endpoint="/list")
        params = {}

        if cursor:
            params["page"] = cursor
        if limit:
            params["limit"] = limit

        response = self.http_client.post(url, params=params, content=metadata_filter)

        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if response.status_code == 401:
                raise OdpValidationError("API argument error") from e
            raise requests.HTTPError(f"HTTP Error - {response.status_code}: {response.text}")

        content = response.json()
        return [FileMetadataDto(**item) for item in content["results"]], content.get("next")

    def upload_file(
        self,
        resource_dto: ResourceDto,
        file_metadata_dto: FileMetadataDto,
        contents: Union[bytes, BytesIO],
        overwrite: bool = False,
    ) -> FileMetadataDto:
        """Upload data to a file.

        Args:
            resource_dto: Dataset manifest
            file_metadata_dto: File metadata
            contents: File contents

        Returns:
            The metadata of the uploaded file
        """
        filename = file_metadata_dto.name
        url = self._construct_url(resource_dto, endpoint=f"/{filename}")

        if isinstance(contents, bytes):
            contents = BytesIO(contents)

        # Ensure contents is in the correct format (bytes)
        if isinstance(contents, BytesIO):
            contents = contents.read()

        headers = {"Content-Type": "application/octet-stream"}

        response = self.http_client.patch(url, params={"overwrite": overwrite}, headers=headers, content=contents)
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if response.status_code == 404:
                raise OdpFileNotFoundError(f"File not found: {filename}") from e
            raise requests.HTTPError(f"HTTP Error - {response.status_code}: {response.text}")

        return self.get_file_metadata(resource_dto, file_metadata_dto)

    def create_file(
        self,
        resource_dto: ResourceDto,
        file_metadata_dto: FileMetadataDto,
        contents: Union[bytes, BytesIO, None] = None,
    ) -> FileMetadataDto:
        """Create a new file.

        Args:
            resource_dto: Dataset manifest
            file_metadata_dto: File metadata
            contents: File contents

        Returns:
            The metadata of the newly created file
        """

        url = self._construct_url(resource_dto)
        headers = {"Content-Type": "application/json"}
        response = self.http_client.post(
            url,
            headers=headers,
            content=file_metadata_dto.model_dump_json(exclude_unset=True),
        )

        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if response.status_code == 401:
                raise OdpValidationError("API argument error") from e
            elif response.status_code == 409:
                raise OdpFileAlreadyExistsError(f"File already exists: {file_metadata_dto.name}")
            raise requests.HTTPError(f"HTTP Error - {response.status_code}: {response.text}")

        file_meta = FileMetadataDto(**response.json())

        if contents:
            return self.upload_file(resource_dto, file_meta, contents)

        return self.get_file_metadata(resource_dto, file_meta)

    def download_file(
        self,
        resource_dto: ResourceDto,
        file_metadata_dto: FileMetadataDto,
        save_path: Optional[str] = None,
    ):
        """Download a file.

        Args:
            resource_dto: Dataset manifest
            file_metadata_dto: File metadata of file
            save_path: File path to save the downloaded file to
        """
        url = self._construct_url(resource_dto, endpoint=f"/{file_metadata_dto.name}")

        response = self.http_client.get(url)
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if response.status_code == 404:
                raise OdpFileNotFoundError(f"File not found: {file_metadata_dto.name}") from e
            raise requests.HTTPError(f"HTTP Error - {response.status_code}: {response.text}")

        if save_path:
            with open(save_path, "wb") as file:
                file.write(response.content)
        else:
            return response.content

    def delete_file(self, resource_dto: ResourceDto, file_metadata_dto: FileMetadataDto):
        """Delete a file. Raises exception if any issues.

        Args:
            resource_dto: Dataset manifest
            file_metadata_dto: File metadata og file to delete.

        Returns:
            `True` if the file was deleted, `False` otherwise
        """
        url = self._construct_url(resource_dto, endpoint=f"/{file_metadata_dto.name}")

        response = self.http_client.delete(url)

        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if response.status_code == 404:
                raise OdpFileNotFoundError(f"File not found: {file_metadata_dto.name}") from e

            raise requests.HTTPError(f"HTTP Error - {response.status_code}: {response.text}")
