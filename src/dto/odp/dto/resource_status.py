from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field


class ResourceStatus(BaseModel):
    """Resource status model"""

    num_updates: int = Field(default=0, ge=0)
    """Number of time the manifest has been updated"""

    created_time: datetime
    """Created timestamp"""

    created_by: UUID
    """UUID of user that created the resource"""

    updated_time: datetime
    """Last updated timestamp"""

    updated_by: UUID
    """UUID of user that updated the resource"""

    deleted_time: Optional[datetime] = None
    """Deleted timestamp - used for soft-delete"""

    deleted_by: Optional[UUID] = None
    """UUID of user that deleted the resource"""
