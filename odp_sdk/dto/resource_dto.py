from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field


class MetadataDto(BaseModel):
    """Resource manifest metadata"""

    name: str
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

    kind: str
    """Resource kind"""

    version: str
    """Resource version"""

    metadata: MetadataDto
    """Resource metadata"""

    status: Optional[ResourceStatusDto] = None
    """Resource status"""

    spec: dict
    """Resource spec"""
