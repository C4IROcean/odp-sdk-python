from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel

from ..common.contact_info import ContactInfo
from ..common.license import License
from ..resource import ResourceSpecABC
from ..resource_registry import kind
from ._rg import CATALOG_RESOURCE_GROUP


class Distribution(BaseModel):
    """Distribution information"""

    published_by: ContactInfo
    """Publisher information"""

    published_date: datetime
    """Date of first published"""

    website: str
    """Distribution website"""

    license: Optional[License] = None
    """Dataset license information"""


@kind(CATALOG_RESOURCE_GROUP, "dataCollection", "v1alpha1")
class DataCollectionSpec(ResourceSpecABC):
    """Data collection specification model"""

    distribution: Optional[Distribution] = None
    """Information on how the dataset was distributed"""

    tags: set[str]
    """Tags for the dataset"""

    facets: Optional[dict[str, Any]] = None
    """Facets for the dataset"""
