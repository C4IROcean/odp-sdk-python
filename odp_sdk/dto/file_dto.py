from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class FileMetadataDto(BaseModel):

    """File Metadata Model."""

    external_id: Optional[Any] = None
    name: Optional[Any] = None
    source: Optional[Any] = None
    mime_type: Optional[Any] = None
    metadata: Optional[Dict[Any, Any]] = None
    directory: Optional[Any] = None
    asset_ids: Optional[List[Any]] = None
    data_set_id: Optional[Any] = None
    labels: Optional[List[Any]] = None
    geo_location: Optional[Any] = None
    source_created_time: Optional[Any] = None
    source_modified_time: Optional[Any] = None
    security_categories: Optional[List[Any]] = None
    id: Optional[Any] = None
    uploaded: Optional[Any] = None
    uploaded_time: Optional[Any] = None
    created_time: Optional[Any] = None
    last_updated_time: Optional[Any] = None
