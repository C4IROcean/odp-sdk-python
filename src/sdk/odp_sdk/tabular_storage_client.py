import re
from typing import Dict, Iterable, List, Optional, Tuple, Union
from uuid import UUID
from warnings import warn

import requests
from odp_sdk.dto import ResourceDto
from odp_sdk.dto.table_spec import StageDataPoints, TableSpec
from odp_sdk.dto.tabular_store import TableStage
from odp_sdk.exc import OdpResourceExistsError, OdpResourceNotFoundError
from odp_sdk.http_client import OdpHttpClient
from odp_sdk.utils import convert_geometry
from odp_sdk.utils.ndjson import NdJsonParser
from pydantic import BaseModel, field_validator

try:
    from pandas import DataFrame
except ImportError:
    DataFrame = ImportError
    warn("Pandas not installed. DataFrame support will not be available.")


class OdpTabularStorageClient(BaseModel):
    http_client: OdpHttpClient
    """HTTP-request client"""

    tabular_storage_endpoint: str = "/data"
    """Endpoint appended to base URL in order to interact with the ODP data-APIs"""

    pagination_size: int = 10_000
    """List-limit when paginating"""

    chunked_encoding_page_size: int = 1_000_000
    """List-limit when using chunked encoding"""

    paginate_by_default: bool = False
    """Whether to paginate by default"""

    page_size_align_factor: int = 1000
    """Page size alignment factor for pagination - ie. page size must be a multiple of this value"""

    @field_validator("tabular_storage_endpoint")
    def _endpoint_validator(cls, v: str):
        m = re.match(r"^/\w+(?<!/)", v)
        if not m:
            raise ValueError(f"Invalid endpoint: {v}")
        return v

    def tabular_endpoint(self, dataset: ResourceDto, *path: str) -> str:
        """Get actual tabular endpoint given a dataset

        Args:
            dataset: Dataset to be used
            path: Path-components to be added to the endpoint

        Returns:
            Tabular endpoint given `dataset`
        """
        ret = f"{self.http_client.base_url}{self.tabular_storage_endpoint}/{dataset.get_ref()}"
        return "/".join([ret, *path])

    def create_schema(self, resource_dto: ResourceDto, table_spec: TableSpec) -> TableSpec:
        """Create Schema

        Args:
            resource_dto: Dataset manifest
            table_spec: Specifications of the schema that is being created

        Returns:
            Specifications of the schema that is being created

        Raises:
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
        """Get schema

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

    def delete_schema(self, resource_dto: ResourceDto, delete_data: bool = False):
        """Delete schema

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
        """Create Stage

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
        """Commit Stage

        Args:
            resource_dto: Dataset manifest
            table_stage: Stage specifications for the stage that is to be committed
        """
        stage_data = StageDataPoints(action="commit", stage_id=table_stage.stage_id)

        response = self.http_client.post(self.tabular_endpoint(resource_dto, "stage"), content=stage_data)
        response.raise_for_status()

    def get_stage_request(self, resource_dto: ResourceDto, stage: Union[UUID, TableStage]) -> TableStage:
        """Get Stage

        Args:
            resource_dto: Dataset manifest
            stage: Identifier for the stage

        Returns:
            Stage that is queried for

        Raises
            OdpResourceNotFoundError: If the schema cannot be found
        """
        stage = stage.stage_id if isinstance(stage, TableStage) else stage
        response = self.http_client.get(self.tabular_endpoint(resource_dto, "stage", str(stage)))

        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if response.status_code == 400:  # This is coming as 400 for both 400 and 404 from ODP
                raise OdpResourceNotFoundError("Schema not found") from e
            raise

        return TableStage(**response.json())

    def list_stage_request(self, resource_dto: ResourceDto) -> List[TableStage]:
        """List Stages for a dataset

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
        """Delete Stage

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

    def select(
        self,
        resource_dto: ResourceDto,
        filter_query: Optional[dict] = None,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
        paginate: Optional[bool] = None,
    ) -> Iterable[dict]:
        """Read data from tabular API

        Args:
            resource_dto: Dataset manifest
            filter_query: Filter query in OQS format
            limit: Limit for the number of rows returned
            cursor: Pagination token for the next page
            paginate: Whether or not to paginate the response or use chunked encoding

        Yields:
            Each row of the data
        """

        paginate = paginate if paginate is not None else self.paginate_by_default

        if paginate is True:
            use_limit = self.pagination_size
        else:
            use_limit = self.chunked_encoding_page_size

        if limit:
            use_limit = min(use_limit, limit)

        if use_limit % self.page_size_align_factor:
            # Round up to the nearest multiple of the alignment factor
            use_limit = (use_limit // self.page_size_align_factor + 1) * self.page_size_align_factor

        num_rows = 0

        while True:
            rows = self._select_page(resource_dto, filter_query, use_limit, cursor)
            for row, is_meta in rows:
                if is_meta:
                    cursor = row.get("@@next")
                    continue

                yield row
                num_rows += 1
                if limit and num_rows >= limit:
                    return

            if not cursor:
                break

    def select_as_stream(
        self,
        resource_dto: ResourceDto,
        filter_query: Optional[dict] = None,
        limit: Optional[int] = None,
    ) -> Iterable[dict]:
        """Select data from dataset

        Args:
            resource_dto: Dataset manifest
            filter_query: Filter query in OQS format
            limit: limit for the number of rows returned

        Returns:
            Data that is queried as a stream

        Raises
            OdpResourceNotFoundError: If the schema cannot be found
        """
        yield from self.select(resource_dto, filter_query, limit=limit)

    def select_as_list(
        self,
        resource_dto: ResourceDto,
        filter_query: Optional[dict] = None,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
    ) -> list[dict]:
        """Select data from dataset

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

        return list(self.select(resource_dto, filter_query, limit, cursor))

    def _select_page(
        self,
        resource_dto: ResourceDto,
        filter_query: Optional[dict] = None,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
        result_geometry: Optional[str] = "geojson",
    ) -> Iterable[Tuple[dict, bool]]:
        """Method to query a specific page from the data"""
        query_parameters = {}
        if limit:
            query_parameters["limit"] = limit
        if cursor:
            query_parameters["cursor"] = cursor

        response = self.http_client.post(
            self.tabular_endpoint(resource_dto, "list"),
            params=query_parameters,
            content=filter_query,
            headers={"Accept": "application/x-ndjson"},
            stream=True,
        )

        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if response.status_code == 404:
                raise OdpResourceNotFoundError("Resource not found") from e
            raise

        try:
            dataset_schema = self.get_schema(resource_dto)
        except OdpResourceNotFoundError:
            print(f"Schema not found for resource {resource_dto.metadata.name}: geometry conversion skipped")
            dataset_schema = None

        for row in NdJsonParser(fp=response.iter_content(chunk_size=None)):
            if len(row) == 1 and next(iter(row.keys())).startswith("@@"):
                is_meta = True
            else:
                is_meta = False
            if dataset_schema:
                row = self._convert_geometry_with_schema(dataset_schema, row, result_geometry)
            yield row, is_meta

    @staticmethod
    def _convert_geometry_with_schema(dataset_schema: TableSpec, row: dict, result_geometry: str) -> dict:
        geometry_cols = [
            column
            for column, column_data in dataset_schema.table_schema.items()
            if column_data.get("type") == "geometry"
        ]
        for col in geometry_cols:
            try:
                row[col] = convert_geometry(row[col], result_geometry)
            except KeyError:
                continue
        return row

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

        self.update(resource_dto, data_list, filter_query)
