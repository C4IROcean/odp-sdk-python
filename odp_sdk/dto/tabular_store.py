from datetime import datetime, timedelta
from typing import List, Literal, Optional, Union
from uuid import UUID, uuid4

from pydantic import BaseModel


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
    error_info: Optional[dict] = None

    def serialize(self) -> bytes:
        return self.json(exclude_unset=True, exclude_none=True).encode("utf-8")

    @classmethod
    def generate(cls, expiry_time: timedelta) -> "TableStage":
        now = datetime.now()

        return cls(stage_id=uuid4(), status="active", created_time=now, expiry_time=now + expiry_time)

    def dict(self, **kwargs) -> "DictStrAny":  # noqa: F821
        exclude_unset = kwargs.pop("exclude_unset", True)
        return super().dict(exclude_unset=exclude_unset, **kwargs)
