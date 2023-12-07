from typing import List, Dict, Optional
from uuid import UUID

from pydantic import BaseModel, PrivateAttr

from odp_sdk.dto import ResourceDto
from odp_sdk.dto.table_spec import TableSpec
from odp_sdk.dto.tabular_store import TableStage
from odp_sdk.http_client import OdpHttpClient
from odp_sdk.oqs.oqs_base import OqsBasePredicate


class OdpTabularStorageController(BaseModel):
    _http_client: OdpHttpClient = PrivateAttr(default_factory=OdpHttpClient)
    tabular_storage_endpoint: str = "/data"

    @property
    def tabular_storage_url(self) -> str:
        """The URL of the tabular storage endpoint, including the base URL.
        Returns:
            The raw storage URL
        """
        return f"{self._http_client.base_url}{self.tabular_storage_endpoint}"

    def create_schema(self, resource_dto: ResourceDto, table_spec: TableSpec) -> TableSpec:
        raise NotImplementedError()

    def get_schema(self, resorce_dto: ResourceDto) -> TableSpec:
        raise NotImplementedError()

    def delete_schema(self, resource_dto: ResourceDto):
        raise NotImplementedError()

    def create_stage_request(self, resource_dto: ResourceDto) -> TableStage:
        raise NotImplementedError()

    def commit_stage_request(self, table_stage: TableStage):
        raise NotImplementedError()

    def get_stage_request(self, resource_dto: ResourceDto, table_stage_identifier: UUID | TableStage) -> TableStage:
        raise NotImplementedError()

    def list_stage_request(self, resource_dto: ResourceDto) -> List[TableStage]:
        raise NotImplementedError()

    def delete_stage_request(self, resource_dto: ResourceDto, table_stage: TableStage):
        raise NotImplementedError()

    def select(self, resource_dto: ResourceDto, filter_query: Optional[OqsBasePredicate]) -> List[Dict]:
        raise NotImplementedError()

    def write(self, resource_dto: ResourceDto, data: List[Dict], table_stage: Optional[TableStage]):
        raise NotImplementedError()

    def delete(self, resource_dto: ResourceDto, filter_query: Optional[OqsBasePredicate]):
        raise NotImplementedError()

    def update(self, resource_dto: ResourceDto, filter_query: Optional[OqsBasePredicate], data: List[Dict]):
        raise NotImplementedError()
