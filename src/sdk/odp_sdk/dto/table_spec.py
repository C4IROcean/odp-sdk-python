from typing import List, Literal, Optional
from uuid import UUID

from odp_sdk.dto.tabular_store import TablePartitioningSpec
from pydantic import BaseModel, model_validator


class TableSpec(BaseModel):
    table_schema: dict
    partitioning: Optional[List[TablePartitioningSpec]] = None


class StageDataPoints(BaseModel):
    """Model for update data point endpoint."""

    action: Literal["create", "commit"]
    stage_id: Optional[UUID]

    @model_validator(mode="before")
    def _validate_action(cls, values):
        if values.get("action") == "create" and values.get("stage_id"):
            raise ValueError("stage id cannot be issued with create action")
        elif values.get("action") == "commit" and not values.get("stage_id"):
            raise ValueError("stage id must be issued with commit action")

        return values
