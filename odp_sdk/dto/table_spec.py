from typing import Optional, List, Literal
from uuid import UUID

from pydantic import BaseModel, root_validator

from odp_sdk.dto.schema import Schema
from odp_sdk.dto.tabular_store import TablePartitioningSpec


class TableSpec(BaseModel):
    table_schema: Schema
    partitioning: Optional[List[TablePartitioningSpec]] = None


class StageDataPoints(BaseModel):
    """Model for update data point endpoint."""

    action: Literal["create", "commit"]
    stage_id: Optional[UUID]

    @root_validator
    def _validate_action(cls, values):
        if values.get("action") == "create" and values.get("stage_id"):
            raise ValueError("stage id cannot be issued with create action")
        elif values.get("action") == "commit" and not values.get("stage_id"):
            raise ValueError("stage id must be issued with commit action")

        return values
