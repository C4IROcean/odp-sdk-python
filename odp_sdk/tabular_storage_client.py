import re
from typing import Dict, List, Optional
from uuid import UUID

import requests
from pydantic import BaseModel, PrivateAttr, field_validator

from odp_sdk.dto import ResourceDto
from odp_sdk.dto.table_spec import StageDataPoints, TableSpec
from odp_sdk.dto.tabular_store import TableStage
from odp_sdk.exc import OdpResourceExistsError, OdpResourceNotFoundError
from odp_sdk.http_client import OdpHttpClient


class OdpTabularStorageController(BaseModel):
    _http_client: OdpHttpClient = PrivateAttr(default_factory=OdpHttpClient)
    tabular_storage_endpoint: str = "/data"

    @field_validator('tabular_storage_endpoint')
    def endpoint_validator(self, v: str):
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
        return f"{self._http_client.base_url}{self.tabular_storage_endpoint}"

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

        if resource_dto.metadata.uuid:
            url = f"{self.tabular_storage_url}/{resource_dto.metadata.uuid}/schema"
        else:
            url = f"{self.tabular_storage_url}/catalog.hubocean.io/dataset/{resource_dto.metadata.name}/schema"

        response = self._http_client.post(url, content=table_spec)

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

        if resource_dto.metadata.uuid:
            url = f"{self.tabular_storage_url}/{resource_dto.metadata.uuid}/schema"
        else:
            url = f"{self.tabular_storage_url}/catalog.hubocean.io/dataset/{resource_dto.metadata.name}/schema"

        response = self._http_client.get(url)

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

        if resource_dto.metadata.uuid:
            url = f"{self.tabular_storage_url}/{resource_dto.metadata.uuid}/schema"
        else:
            url = f"{self.tabular_storage_url}/catalog.hubocean.io/dataset/{resource_dto.metadata.name}/schema"

        query_params = dict()
        query_params["delete_data"] = delete_data

        response = self._http_client.delete(url, params=query_params)

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

        if resource_dto.metadata.uuid:
            url = f"{self.tabular_storage_url}/{resource_dto.metadata.uuid}/stage"
        else:
            url = f"{self.tabular_storage_url}/catalog.hubocean.io/dataset/{resource_dto.metadata.name}/stage"

        stage_data = StageDataPoints(action="create")

        response = self._http_client.post(url, content=stage_data)

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

        response = self._http_client.post(url, content=stage_data)
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

        if resource_dto.metadata.uuid:
            url = f"{self.tabular_storage_url}/{resource_dto.metadata.uuid}/stage"
        else:
            url = f"{self.tabular_storage_url}/catalog.hubocean.io/dataset/{resource_dto.metadata.name}/stage"

        if table_stage_identifier.stage_id:
            url = f"{url}/{table_stage_identifier.stage_id}"
        else:
            url = f"{url}/{table_stage_identifier}"

        response = self._http_client.get(url)

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

        if resource_dto.metadata.uuid:
            url = f"{self.tabular_storage_url}/{resource_dto.metadata.uuid}/stage"
        else:
            url = f"{self.tabular_storage_url}/catalog.hubocean.io/dataset/{resource_dto.metadata.name}/stage"

        response = self._http_client.get(url)

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

        if resource_dto.metadata.uuid:
            url = f"{self.tabular_storage_url}/{resource_dto.metadata.uuid}/stage/{table_stage.stage_id}"
        else:
            url = (
                f"{self.tabular_storage_url}/catalog.hubocean.io/dataset/{resource_dto.metadata.name}/stage"
                f"/{table_stage.stage_id}"
            )

        query_params = dict()
        query_params["force_delete"] = force_delete

        response = self._http_client.delete(url, params=query_params)

        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if response.status_code == 400:  # This is coming as 400 for both 400 and 404 from ODP
                raise OdpResourceNotFoundError("Schema not found") from e
            raise

    def select(self, resource_dto: ResourceDto, filter_query: Optional[dict]) -> List[Dict]:
        """
        Select data from dataset

        Args:
            resource_dto: Dataset manifest
            filter_query: filter query in OQS format

        Returns:
            Data that is queried

        Raises
            OdpResourceNotFoundError: If the schema cannot be found
        """

        if resource_dto.metadata.uuid:
            url = f"{self.tabular_storage_url}/{resource_dto.metadata.uuid}/list"
        else:
            url = f"{self.tabular_storage_url}/catalog.hubocean.io/dataset/{resource_dto.metadata.name}/list"

        if filter_query:
            response = self._http_client.post(url, content=filter_query)
        else:
            response = self._http_client.post(url)

        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if response.status_code == 404:
                raise OdpResourceNotFoundError("Resource not found") from e
            raise

        return [row for row in response.json()]

    def write(self, resource_dto: ResourceDto, data: List[Dict], table_stage: Optional[TableStage]):
        """
        Write data to dataset

        Args:
            resource_dto: Dataset manifest
            data: data to ingest
            table_stage: Stage specifications for the stage to ingest

        Raises
            OdpResourceNotFoundError: If the schema cannot be found
        """

        if resource_dto.metadata.uuid:
            url = f"{self.tabular_storage_url}/{resource_dto.metadata.uuid}"
        else:
            url = f"{self.tabular_storage_url}/catalog.hubocean.io/dataset/{resource_dto.metadata.name}"

        body = dict()
        if table_stage:
            body["stage_id"] = table_stage.stage_id
        body["data"] = data

        response = self._http_client.post(url, content=body)

        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if response.status_code == 404:
                raise OdpResourceNotFoundError("Resource not found") from e
            raise

    def delete(self, resource_dto: ResourceDto, filter_query: Optional[dict]):
        """
        Delete data from dataset

        Args:
            resource_dto: Dataset manifest
            filter_query: filter query in OQS format

        Raises
            OdpResourceNotFoundError: If the schema cannot be found
        """

        if resource_dto.metadata.uuid:
            url = f"{self.tabular_storage_url}/{resource_dto.metadata.uuid}/delete"
        else:
            url = f"{self.tabular_storage_url}/catalog.hubocean.io/dataset/{resource_dto.metadata.name}/delete"

        if filter_query:
            response = self._http_client.post(url, content=filter_query)
        else:
            response = self._http_client.post(url)

        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if response.status_code == 404:
                raise OdpResourceNotFoundError("Resource not found") from e
            raise

    def update(self, resource_dto: ResourceDto, data: List[Dict]):
        """
        Update data from dataset

        Args:
            resource_dto: Dataset manifest
            data: data to update

        Raises
            OdpResourceNotFoundError: If the schema cannot be found
        """

        if resource_dto.metadata.uuid:
            url = f"{self.tabular_storage_url}/{resource_dto.metadata.uuid}/list"
        else:
            url = f"{self.tabular_storage_url}/catalog.hubocean.io/dataset/{resource_dto.metadata.name}/list"

        body = dict()
        body["data"] = data

        response = self._http_client.patch(url, content=body)

        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if response.status_code == 404:
                raise OdpResourceNotFoundError("Resource not found") from e
            raise
