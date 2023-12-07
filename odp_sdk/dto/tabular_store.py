from datetime import datetime, timedelta
from typing import List, Optional, Union, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel

from odp_sdk.dto.exception_dto import ExceptionDto
from odp_sdk.exc import OpenTableStageInvalidAction


class TablePartitioningSpec(BaseModel):
    columns: List[str]
    transformer_name: str
    args: Optional[List[Union[int, float, str]]] = None

    def serialize(self) -> bytes:
        return self.json().encode("utf-8")


class TableStage(BaseModel):
    stage_id: UUID
    status: Literal["active", "commit", "commit-failed", "delete"]
    created_time: datetime
    expiry_time: datetime
    updated_time: Optional[datetime] = None

    error: Optional[str] = None
    error_info: Optional[ExceptionDto] = None

    def commit(self, inplace: bool = True) -> "TableStage":
        if self.status not in {"active", "commit-failed"}:
            raise OpenTableStageInvalidAction(f"Cannot commit stage with status '{self.status}'")

        stage = self if inplace else self.copy()

        stage.status = "commit"
        stage.updated_time = datetime.now()

        return stage

    def soft_delete(self, force: bool = False, inplace: bool = True) -> "TableStage":
        if self.status == "commit" and not force:
            raise OpenTableStageInvalidAction(f"Cannot delete stage {self.stage_id} with status '{self.status}'")

        stage = self if inplace else self.copy()

        stage.status = "delete"
        stage.updated_time = datetime.now()

        return stage

    def indicate_error(
        self, exception: Exception, error_info: Optional[ExceptionDto] = None, inplace: bool = True
    ) -> "TableStage":
        stage = self if inplace else self.copy()

        stage.status = "commit-failed"
        stage.updated_time = datetime.now()
        stage.error = str(exception)

        if error_info:
            stage.error_info = error_info

        return stage

    def serialize(self) -> bytes:
        return self.json(exclude_unset=True, exclude_none=True).encode("utf-8")

    @classmethod
    def generate(cls, expiry_time: timedelta) -> "TableStage":
        now = datetime.now()

        return cls(stage_id=uuid4(), status="active", created_time=now, expiry_time=now + expiry_time)

    def dict(self, **kwargs) -> "DictStrAny":
        exclude_unset = kwargs.pop("exclude_unset", True)
        return super().dict(exclude_unset=exclude_unset, **kwargs)

