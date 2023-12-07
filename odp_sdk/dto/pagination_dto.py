from typing import List, Optional

from pydantic import BaseModel

from odp_sdk.dto.file_dto import FileMetadataDto


class PaginationDto(BaseModel):
    """Pagination Model."""

    results: List[FileMetadataDto] = []
    next: Optional[str] = None
    num_results: int
