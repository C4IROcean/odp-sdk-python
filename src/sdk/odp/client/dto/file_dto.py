from datetime import datetime
from typing import Any, Dict, Optional, Union
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


class FileMetadataDto(BaseModel):
    """File Metadata Model."""

    name: str
    mime_type: Optional[str] = None
    dataset: Optional[UUID] = None
    metadata: Dict[str, Union[bool, int, str]] = Field(default_factory=dict)
    geo_location: Optional[Any] = None
    size_bytes: Optional[int] = None
    checksum: Optional[str] = None
    created_time: Optional[datetime] = None
    modified_time: Optional[datetime] = None
    deleted_time: Optional[datetime] = None

    @field_validator("name")
    def lstrip_name(cls, v):
        if v.startswith("/"):
            raise ValueError("name cannot start with '/'. Absolute paths are not allowed.")
        return v
