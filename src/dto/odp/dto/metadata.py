from typing import Dict, Optional
from uuid import UUID

from pydantic import BaseModel, Field


class Metadata(BaseModel):
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

    labels: Dict = Field(default_factory=dict)
    """Resource labels"""

    owner: Optional[UUID] = None
    """Owner of the resource"""
