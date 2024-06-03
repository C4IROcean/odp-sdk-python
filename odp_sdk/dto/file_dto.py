from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID

from pydantic import BaseModel


class FileMetadataDto(BaseModel):
    """File Metadata Model."""

    name: Optional[Any] = None
    mime_type: Optional[Any] = None
    dataset: Optional[UUID] = None
    metadata: Optional[Dict[Any, Any]] = None
    geo_location: Optional[Any] = None
    size_bytes: Optional[int] = None
    checksum: Optional[str] = None
    created_time: Optional[datetime] = None
    modified_time: Optional[datetime] = None
    deleted_time: Optional[datetime] = None
