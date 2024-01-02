from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field, model_validator


class MetadataDto(BaseModel):
    """Resource manifest metadata"""

    name: Optional[str] = None
    """Resource name. Must consist of alphanumeric characters, dashes or underscores and must start
    with an alphanumeric character"""

    display_name: Optional[str] = None
    """Human-friendly name"""

    description: Optional[str] = None
    """Resource description"""

    uuid: Optional[UUID] = None
    """System-assigned unique identifier"""

    labels: dict = Field(default_factory=dict)
    """Resource labels"""

    owner: Optional[UUID] = None

    @model_validator(mode="before")
    def _validate_action(cls, values):
        if not values.get("name") and not values.get("uuid"):
            raise ValueError("name or uuid must be set")

        return values


class ResourceStatusDto(BaseModel):
    num_updates: int
    """Number of time the manifest has been updated"""

    created_time: datetime
    """Created timestamp"""

    created_by: UUID
    """UUID of `PolicyConsumerDto` that created the resource"""

    updated_time: datetime
    """Last updated timestamp"""

    updated_by: UUID
    """UUID of `PolicyConsumerDto` that updated the resource"""

    deleted_time: Optional[datetime] = None
    """Deleted timestamp - used for soft-delete"""

    deleted_by: Optional[UUID] = None
    """UUID of `PolicyConsumerDto` that deleted the resource"""

    class Config:
        extra = "allow"


class ResourceDto(BaseModel):
    """Resource manifest base class"""

    kind: Optional[str] = None
    """Resource kind"""

    version: Optional[str] = None
    """Resource version"""

    metadata: MetadataDto
    """Resource metadata"""

    status: Optional[ResourceStatusDto] = None
    """Resource status"""

    spec: Optional[dict] = None
    """Resource spec"""
