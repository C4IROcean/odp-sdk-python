import re
from typing import Dict, Iterable, Iterator, List, Optional
from uuid import UUID

import requests
from pydantic import BaseModel, ValidationError, field_validator
from requests import JSONDecodeError

from odp_sdk.dto import ResourceDto
from odp_sdk.dto.table_spec import StageDataPoints, TableSpec
from odp_sdk.dto.tabular_store import PaginatedSelectResultSet, TableStage
from odp_sdk.exc import OdpResourceExistsError, OdpResourceNotFoundError
from odp_sdk.http_client import OdpHttpClient
from odp_sdk.utils import convert_geometry
from odp_sdk.utils.ndjson import NdJsonParser

try:
    from pandas import DataFrame
except ImportError:
    print("Pandas not installed. DataFrame support will not be available.")


class OdpTabularStorageClient(BaseModel):
    http_client: OdpHttpClient
    """HTTP-request client"""

    tabular_storage_endpoint: str = "/data"
    """Endpoint appended to base URL in order to interact with the ODP data-APIs"""

    pagination_size: int = 10_000
    """List-limit when paginating"""

    @field_validator("tabular_storage_endpoint")
    def _endpoint_validator(cls, v: str):
        m = re.match(r"^/\w+(?<!/)", v)
        if not m:
            raise ValueError(f"Invalid endpoint: {v}")
        return v

    def tabular_endpoint(self, dataset: ResourceDto, *path: str) -> str:
        ret = f"{self.http_client.base_url}{self.tabular_storage_endpoint}/{dataset.get_ref()}"
        return "/".join([ret, *path])

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

        response = self.http_client.post(self.tabular_endpoint(resource_dto, "schema"), content=table_spec)

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

        response = self.http_client.get(self.tabular_endpoint(resource_dto, "schema"))

        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if response.status_code == 404:
                raise OdpResourceNotFoundError("Schema not found") from e
            raise

        return TableSpec.parse_raw(response.text)

    def delete_schema(self, resource_dto: ResourceDto, delete_data=False):
        """
        Delete schema

        Args:
            resource_dto: Dataset manifest
            delete_data: Bool to specify whether the data should be deleted as well

        Raises
            OdpResourceNotFoundError: If the schema cannot be found
        """

        response = self.http_client.delete(
            self.tabular_endpoint(resource_dto, "schema"), params={"delete_data": delete_data}
        )

        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if response.status_code == 404:
                raise OdpResourceNotFoundError("Schema not found") from e
            raise

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
        stage_data = StageDataPoints(action="create", stage_id=None)
        response = self.http_client.post(self.tabular_endpoint(resource_dto, "stage"), content=stage_data)

        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if response.status_code == 409:
                raise OdpResourceExistsError("Stage with identifier already exists") from e
            raise

        return TableStage(**response.json())

    def commit_stage_request(self, resource_dto: ResourceDto, table_stage: TableStage):
        """
        Commit Stage

        Args:
            resource_dto: Dataset manifest
            table_stage: Stage specifications for the stage that is to be committed
        """
        stage_data = StageDataPoints(action="commit", stage_id=table_stage.stage_id)

        response = self.http_client.post(self.tabular_endpoint(resource_dto, "stage"), content=stage_data)
        response.raise_for_status()

    def get_stage_request(self, resource_dto: ResourceDto, stage: UUID | TableStage) -> TableStage:
        """
        Get Stage

        Args:
            resource_dto: Dataset manifest
            stage: Identifier for the stage

        Returns:
            Stage that is queried for

        Raises
            OdpResourceNotFoundError: If the schema cannot be found
        """

        stage = str(stage.stage_id if isinstance(stage, TableStage) else stage)
        response = self.http_client.get(self.tabular_endpoint(resource_dto, "stage", stage))

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
        response = self.http_client.get(self.tabular_endpoint(resource_dto, "stage"))

        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if response.status_code == 400:  # This is coming as 400 for both 400 and 404 from ODP
                raise OdpResourceNotFoundError("Schema not found") from e
            raise

        return [TableStage.parse_obj(table_stage) for table_stage in response.json()]

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

        response = self.http_client.delete(
            self.tabular_endpoint(resource_dto, "stage", str(table_stage.stage_id)),
            params={"force_delete": force_delete},
        )

        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if response.status_code == 400:  # This is coming as 400 for both 400 and 404 from ODP
                raise OdpResourceNotFoundError("Schema not found") from e
            raise

    def select_as_stream(
        self, resource_dto: ResourceDto, filter_query: Optional[dict] = None, limit: Optional[int] = None
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
        self,
        resource_dto: ResourceDto,
        filter_query: Optional[dict] = None,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
    ) -> list[dict]:
        """
        Select data from dataset

        Args:
            resource_dto: Dataset manifest
            filter_query: Filter query in OQS format
            limit: limit for the number of rows returned
            cursor: pointer to next list page

        Returns:
            Data that is queried as a list

        Raises
            OdpResourceNotFoundError: If the schema cannot be found
        """

        row_iterator = self._select_stream(resource_dto, filter_query, limit, cursor)

        if limit:
            return list(row_iterator)
        else:
            return list(row_iterator)[:-1]

    def _select_stream(
        self,
        resource_dto: ResourceDto,
        filter_query: Optional[dict] = None,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
    ) -> Iterator[dict]:
        """
        Helper method to get data in chunks and to compile them
        """
        # Make sure the page size is a multiple of 1000 per API specs
        if limit:
            limit = (limit // 1000) * 1000
        rows, cursor = self._select_page(resource_dto, filter_query, limit, cursor)
        yield from rows

    def _select_page(
        self,
        resource_dto: ResourceDto,
        filter_query: Optional[dict] = None,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
        result_geometry: Optional[str] = "geojson",
    ) -> tuple[list[dict], Optional[str]]:
        """
        Method to query a specific page from the data
        """
        query_parameters = dict()
        if limit:
            query_parameters["limit"] = limit
        if cursor:
            query_parameters["cursor"] = cursor

        response = self.http_client.post(
            self.tabular_endpoint(resource_dto, "list"), params=query_parameters, content=filter_query
        )

        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if response.status_code == 404:
                raise OdpResourceNotFoundError("Resource not found") from e
            raise

        try:
            if response.headers.get("Content-Type") == "application/json":
                result = PaginatedSelectResultSet(**response.json())
            elif response.headers.get("Content-Type") == "application/x-ndjson":
                data = list(iter(NdJsonParser(response.text)))
                data = convert_geometry(data, result_geometry)

                result = PaginatedSelectResultSet(data=data)
            else:
                raise ValueError("Invalid response content type")
        except (JSONDecodeError, ValidationError, ValueError) as e:
            print(f"Error decoding response: {e}")
            result = PaginatedSelectResultSet(data=[])

        return result.data, result.next

    def select_as_dataframe(self, resource_dto: ResourceDto, filter_query: Optional[dict] = None) -> DataFrame:
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

    def write(self, resource_dto: ResourceDto, data: List[Dict], table_stage: Optional[TableStage] = None):
        """
        Write data to dataset

        Args:
            resource_dto: Dataset manifest
            data: Data to ingest
            table_stage: Stage specifications for the stage to ingest

        Raises
            OdpResourceNotFoundError: If the schema cannot be found
        """

        self._write_limited_size(self.tabular_endpoint(resource_dto), data, table_stage)

    def _write_limited_size(self, url: str, data: List[Dict], table_stage: Optional[TableStage] = None):
        if len(data) < 1:
            return

        body = dict()
        if table_stage:
            body["stage_id"] = str(table_stage.stage_id)
        body["data"] = data

        response = self.http_client.post(url, content=body)

        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if response.status_code == 404:
                raise OdpResourceNotFoundError("Resource not found") from e
            raise requests.HTTPError(f"HTTP Error - {response.status_code}: {response.text}")

    def write_dataframe(self, resource_dto: ResourceDto, data: DataFrame, table_stage: Optional[TableStage] = None):
        """
        Write data to dataset in DataFrame format

        Args:
            resource_dto: Dataset manifest
            data: Data to ingest in DataFrame format
            table_stage: Stage specifications for the stage to ingest

        Raises
            OdpResourceNotFoundError: If the schema cannot be found
        """

        data_list = data.to_dict(orient="records")

        self.write(resource_dto, data_list, table_stage)

    def delete(self, resource_dto: ResourceDto, filter_query: Optional[dict] = None):
        """
        Delete data from dataset

        Args:
            resource_dto: Dataset manifest
            filter_query: Filter query in OQS format

        Raises
            OdpResourceNotFoundError: If the schema cannot be found
        """
        response = self.http_client.post(self.tabular_endpoint(resource_dto, "delete"), content=filter_query)

        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if response.status_code == 404:
                raise OdpResourceNotFoundError("Resource not found") from e
            raise

    def update(self, resource_dto: ResourceDto, data: List[Dict], filter_query: dict):
        """
        Update data from dataset

        Args:
            resource_dto: Dataset manifest
            filter_query: Filter query in OQS format
            data: data to update

        Raises
            OdpResourceNotFoundError: If the schema cannot be found
        """
        response = self.http_client.patch(
            self.tabular_endpoint(resource_dto), content={"update_filters": filter_query, "data": data}
        )

        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if response.status_code == 404:
                raise OdpResourceNotFoundError("Resource not found") from e
            raise

    def update_dataframe(self, resource_dto: ResourceDto, data: DataFrame, filter_query: Optional[dict] = None):
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
