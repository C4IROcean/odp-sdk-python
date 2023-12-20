import math
import re
from typing import Dict, Iterable, Iterator, List, Optional
from uuid import UUID

import requests
from pandas import DataFrame
from pydantic import BaseModel, field_validator

from odp_sdk.dto import ResourceDto
from odp_sdk.dto.table_spec import StageDataPoints, TableSpec
from odp_sdk.dto.tabular_store import PaginatedSelectResultSet, TableStage
from odp_sdk.exc import OdpResourceExistsError, OdpResourceNotFoundError
from odp_sdk.http_client import OdpHttpClient


class OdpTabularStorageClient(BaseModel):
    http_client: OdpHttpClient
    tabular_storage_endpoint: str = "/data"
    write_chunk_size_limit: int = 10000
    select_chunk_size_limit: int = 10000

    def __init__(self, **data):
        super().__init__(**data)

    @field_validator("tabular_storage_endpoint")
    def _endpoint_validator(cls, v: str):
        m = re.match(r"^/\w+(?<!/)", v)
        if not m:
            raise ValueError(f"Invalid endpoint: {v}")
        return v

    @property
    def tabular_storage_url(self) -> str:
        """
        The URL of the tabular storage endpoint, including the base URL.

        Returns:
            The tabular storage URL
        """
        return f"{self.http_client.base_url}{self.tabular_storage_endpoint}"

    def _get_schema_url(self, resource_dto: ResourceDto):
        if resource_dto.metadata.uuid:
            return f"{self.tabular_storage_url}/{resource_dto.metadata.uuid}/schema"
        else:
            return f"{self.tabular_storage_url}/catalog.hubocean.io/dataset/{resource_dto.metadata.name}/schema"

    def create_schema(self, resource_dto: ResourceDto, table_spec: TableSpec) -> TableSpec:
        """
        Create Schema

        Args:
            resource_dto: Dataset manifest
            table_spec: Specifications of the schema that is being created

        Returns:
            Specifications of the schema that is being created

        Raises
            OdpResourceExistsError: If the schema already exists with the same identifier
        """

        url = self._get_schema_url(resource_dto)
        response = self.http_client.post(url, content=table_spec.model_dump_json())

        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if response.status_code == 409:
                raise OdpResourceExistsError("Schema with identifier already exists") from e
            raise

        return TableSpec(**response.json())

    def get_schema(self, resource_dto: ResourceDto) -> TableSpec:
        """
        Get schema

        Args:
            resource_dto: Dataset manifest

        Returns:
            Specifications of the schema that is being queried

        Raises
            OdpResourceNotFoundError: If the schema cannot be found
        """

        url = self._get_schema_url(resource_dto)
        response = self.http_client.get(url)

        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if response.status_code == 404:
                raise OdpResourceNotFoundError("Schema not found") from e
            raise

        return TableSpec(**response.json())

    def delete_schema(self, resource_dto: ResourceDto, delete_data=False):
        """
        Delete schema

        Args:
            resource_dto: Dataset manifest
            delete_data: Bool to specify whether the data should be deleted as well

        Raises
            OdpResourceNotFoundError: If the schema cannot be found
        """

        url = self._get_schema_url(resource_dto)

        query_params = dict()
        query_params["delete_data"] = delete_data

        response = self.http_client.delete(url, params=query_params)

        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if response.status_code == 404:
                raise OdpResourceNotFoundError("Schema not found") from e
            raise

    def _get_stage_url(self, resource_dto: ResourceDto):
        if resource_dto.metadata.uuid:
            return f"{self.tabular_storage_url}/{resource_dto.metadata.uuid}/stage"
        else:
            return f"{self.tabular_storage_url}/catalog.hubocean.io/dataset/{resource_dto.metadata.name}/stage"

    def create_stage_request(self, resource_dto: ResourceDto) -> TableStage:
        """
        Create Stage

        Args:
            resource_dto: Dataset manifest

        Returns:
            Specifications of the stage that is being created

        Raises
            Stage with the specified identifier already exists
        """

        url = self._get_stage_url(resource_dto)

        stage_data = StageDataPoints(action="create", stage_id=None)

        response = self.http_client.post(url, content=stage_data)

        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if response.status_code == 409:
                raise OdpResourceExistsError("Stage with identifier already exists") from e
            raise

        return TableStage(**response.json())

    def commit_stage_request(self, table_stage: TableStage):
        """
        Commit Stage

        Args:
            table_stage: Stage specifications for the stage that is to be committed
        """

        url = f"{self.tabular_storage_url}/{table_stage.stage_id}/stage"

        stage_data = StageDataPoints(action="commit", stage_id=table_stage.stage_id)

        response = self.http_client.post(url, content=stage_data)
        response.raise_for_status()

    def get_stage_request(self, resource_dto: ResourceDto, table_stage_identifier: UUID | TableStage) -> TableStage:
        """
        Get Stage

        Args:
            resource_dto: Dataset manifest
            table_stage_identifier: Identifier for the stage

        Returns:
            Stage that is queried for

        Raises
            OdpResourceNotFoundError: If the schema cannot be found
        """

        url = self._get_stage_url(resource_dto)

        if isinstance(table_stage_identifier, TableStage):
            url = f"{url}/{table_stage_identifier.stage_id}"
        else:
            url = f"{url}/{table_stage_identifier}"

        response = self.http_client.get(url)

        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if response.status_code == 400:  # This is coming as 400 for both 400 and 404 from ODP
                raise OdpResourceNotFoundError("Schema not found") from e
            raise

        return TableStage(**response.json())

    def list_stage_request(self, resource_dto: ResourceDto) -> List[TableStage]:
        """
        List Stages for a dataset

        Args:
            resource_dto: Dataset manifest

        Returns:
            Stages that are related to the dataset

        Raises
            OdpResourceNotFoundError: If the schema cannot be found
        """

        url = self._get_stage_url(resource_dto)

        response = self.http_client.get(url)

        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if response.status_code == 400:  # This is coming as 400 for both 400 and 404 from ODP
                raise OdpResourceNotFoundError("Schema not found") from e
            raise

        return [TableStage(**table_stage) for table_stage in response.json()]

    def delete_stage_request(self, resource_dto: ResourceDto, table_stage: TableStage, force_delete=False):
        """
        Delete Stage

        Args:
            resource_dto: Dataset manifest
            table_stage: Table stage that wants to be deleted
            force_delete: Bool to specify whether the data should be force deleted

        Raises
            OdpResourceNotFoundError: If the schema cannot be found
        """

        url = f"{self._get_stage_url(resource_dto)}/{table_stage.stage_id}"

        query_params = dict()
        query_params["force_delete"] = force_delete

        response = self.http_client.delete(url, params=query_params)

        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if response.status_code == 400:  # This is coming as 400 for both 400 and 404 from ODP
                raise OdpResourceNotFoundError("Schema not found") from e
            raise

    def _get_crud_url(self, resource_dto: ResourceDto):
        if resource_dto.metadata.uuid:
            return f"{self.tabular_storage_url}/{resource_dto.metadata.uuid}"
        else:
            return f"{self.tabular_storage_url}/catalog.hubocean.io/dataset/{resource_dto.metadata.name}"

    def select_as_stream(
        self, resource_dto: ResourceDto, filter_query: Optional[dict], limit: Optional[int] = None
    ) -> Iterable[dict]:
        """
        Select data from dataset

        Args:
            resource_dto: Dataset manifest
            filter_query: Filter query in OQS format
            limit: limit for the number of rows returned

        Returns:
            Data that is queried as a stream

        Raises
            OdpResourceNotFoundError: If the schema cannot be found
        """

        row_iterator = self._select_stream(resource_dto, filter_query, limit)

        yield from row_iterator

    def select_as_list(
        self, resource_dto: ResourceDto, filter_query: Optional[dict], limit: Optional[int] = None
    ) -> list[dict]:
        """
        Select data from dataset

        Args:
            resource_dto: Dataset manifest
            filter_query: Filter query in OQS format
            limit: limit for the number of rows returned

        Returns:
            Data that is queried as a list

        Raises
            OdpResourceNotFoundError: If the schema cannot be found
        """

        row_iterator = self._select_stream(resource_dto, filter_query, limit)

        return list(row_iterator)

    def _select_stream(
        self,
        resource_dto: ResourceDto,
        filter_query: Optional[dict],
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
    ) -> Iterator[dict]:
        """
        Helper method to get data in chunks and to compile them
        """
        if not limit:
            limit = math.inf

        page_size = min(self.select_chunk_size_limit, limit)

        while True:
            # Make sure the page size is a multiple of 1000 per API specs
            page_size = (page_size * 1000) // 1000
            rows, cursor = self._select_page(resource_dto, filter_query, page_size, cursor)
            yield from rows

            # Calculate remaining limit
            limit -= page_size
            # If limit is reached or no more data yield the result
            if limit == 0 or cursor is None:
                break
            # If limit not reached if limit is less than the page_size set new page size to not overflow the limit
            elif limit < page_size:
                page_size = limit

    def _select_page(
        self,
        resource_dto: ResourceDto,
        filter_query: Optional[dict],
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
    ) -> tuple[list[dict], Optional[str]]:
        """
        Method to query a specific page from the data
        """

        url = f"{self._get_crud_url(resource_dto)}/list"

        query_parameters = dict()
        if limit:
            query_parameters["limit"] = limit
        if cursor:
            query_parameters["cursor"] = cursor

        headers = dict()
        headers["X-ODP-CHUNKED-ENCODING"] = "True"

        if filter_query:
            response = self.http_client.post(url, content=filter_query, params=query_parameters, headers=headers)
        else:
            response = self.http_client.post(url, params=query_parameters, headers=headers)

        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if response.status_code == 404:
                raise OdpResourceNotFoundError("Resource not found") from e
            raise

        result = PaginatedSelectResultSet(**response.json())

        return result.data, result.next

    def select_as_dataframe(self, resource_dto: ResourceDto, filter_query: Optional[dict]) -> DataFrame:
        """
        Select data from dataset as a DataFrame

        Args:
            resource_dto: Dataset manifest
            filter_query: Filter query in OQS format

        Returns:
            Data that is queried in DataFrame format

        Raises
            OdpResourceNotFoundError: If the schema cannot be found
        """

        data = self.select_as_list(resource_dto, filter_query)

        return DataFrame(data)

    def write(self, resource_dto: ResourceDto, data: List[Dict], table_stage: Optional[TableStage]):
        """
        Write data to dataset

        Args:
            resource_dto: Dataset manifest
            data: Data to ingest
            table_stage: Stage specifications for the stage to ingest

        Raises
            OdpResourceNotFoundError: If the schema cannot be found
        """

        url = self._get_crud_url(resource_dto)

        while len(data) > self.write_chunk_size_limit:
            data_chunk = data[: self.write_chunk_size_limit]
            data = data[self.write_chunk_size_limit :]
            self._write_limited_size(url, data_chunk, table_stage)

        self._write_limited_size(url, data, table_stage)

    def _write_limited_size(self, url: str, data: List[Dict], table_stage: Optional[TableStage]):
        if len(data) < 1:
            return

        body = dict()
        if table_stage:
            body["stage_id"] = table_stage.stage_id
        body["data"] = data

        response = self.http_client.post(url, content=body)

        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if response.status_code == 404:
                raise OdpResourceNotFoundError("Resource not found") from e
            raise

    def write_dataframe(self, resource_dto: ResourceDto, data: DataFrame, table_stage: Optional[TableStage]):
        """
        Write data to dataset in DataFrame format

        Args:
            resource_dto: Dataset manifest
            data: Data to ingest in DataFrame format
            table_stage: Stage specifications for the stage to ingest

        Raises
            OdpResourceNotFoundError: If the schema cannot be found
        """

        data_list = data.values.tolist()

        self.write(resource_dto, data_list, table_stage)

    def delete(self, resource_dto: ResourceDto, filter_query: Optional[dict]):
        """
        Delete data from dataset

        Args:
            resource_dto: Dataset manifest
            filter_query: Filter query in OQS format

        Raises
            OdpResourceNotFoundError: If the schema cannot be found
        """

        url = f"{self._get_crud_url(resource_dto)}/delete"

        if filter_query:
            response = self.http_client.post(url, content=filter_query)
        else:
            response = self.http_client.post(url)

        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if response.status_code == 404:
                raise OdpResourceNotFoundError("Resource not found") from e
            raise

    def update(self, resource_dto: ResourceDto, filter_query: Optional[dict], data: List[Dict]):
        """
        Update data from dataset

        Args:
            resource_dto: Dataset manifest
            filter_query: Filter query in OQS format
            data: data to update

        Raises
            OdpResourceNotFoundError: If the schema cannot be found
        """

        url = f"{self._get_crud_url(resource_dto)}/list"

        body = dict()
        body["update_filters"] = filter_query
        body["data"] = data

        response = self.http_client.patch(url, content=body)

        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if response.status_code == 404:
                raise OdpResourceNotFoundError("Resource not found") from e
            raise

    def update_dataframe(self, resource_dto: ResourceDto, filter_query: Optional[dict], data: DataFrame):
        """
        Update data from dataset in DataFrame format

        Args:
            resource_dto: Dataset manifest
            filter_query: Filter query in OQS format
            data: Data to update in DataFrame format

        Raises
            OdpResourceNotFoundError: If the schema cannot be found
        """

        data_list = data.values.tolist()

        self.update(resource_dto, filter_query, data_list)
