from typing import Dict, List, Optional

from pydantic import BaseModel


class FileMetadataDto(BaseModel):
    """File Metadata Model."""

    external_id: Optional[str] = None
    name: Optional[str] = None
    source: Optional[str] = None
    mime_type: Optional[str] = None
    metadata: Optional[Dict[str, str]] = None
    directory: Optional[str] = None
    asset_ids: Optional[List[int]] = None
    data_set_id: Optional[int] = None
    labels: Optional[List[str]] = None
    geo_location: Optional[str] = None
    source_created_time: Optional[int] = None
    source_modified_time: Optional[int] = None
    security_categories: Optional[List[int]] = None
    id: Optional[int] = None
    uploaded: Optional[bool] = None
    uploaded_time: Optional[int] = None
    created_time: Optional[int | str] = None
    last_updated_time: Optional[int] = None
