from typing import Dict, List, Optional

from pydantic import BaseModel


class FileMetadataDto(BaseModel):
    """File Metadata Model."""

    external_id: Optional[any] = None
    name: Optional[str] = None
    source: Optional[str] = None
    mime_type: Optional[str] = None
    metadata: Optional[Dict[str, str]] = None
    directory: Optional[any] = None
    asset_ids: Optional[List[any]] = None
    data_set_id: Optional[any] = None
    labels: Optional[List[any]] = None
    geo_location: Optional[any] = None
    source_created_time: Optional[any] = None
    source_modified_time: Optional[any] = None
    security_categories: Optional[List[any]] = None
    id: Optional[any] = None
    uploaded: Optional[any] = None
    uploaded_time: Optional[any] = None
    created_time: Optional[any] = None
    last_updated_time: Optional[any] = None
