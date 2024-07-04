from typing import Any, Dict, List, Optional, Set

from pydantic import BaseModel, Field

from ..common.contact_info import ContactInfo
from ..resource import ResourceSpecABC
from ..resource_registry import kind
from ._rg import CATALOG_RESOURCE_GROUP


class Citation(BaseModel):
    """Citation information"""

    cite_as: Optional[str] = None
    """Directions on how to cite the dataset"""

    doi: Optional[str] = None


class Attribute(BaseModel):
    """Dataset attribute"""

    name: str
    """Attribute name. This can be a column name in a table, a dimension in an array, etc."""

    description: Optional[str] = None
    """Attribute description"""

    traits: list[str]
    """List of traits. Traits are used to describe the attribute in more detail.

    Traits are based on Microsoft Common Data Model (CDM) traits. See the [CDM documentation]
    (https://learn.microsoft.com/en-us/common-data-model/sdk/trait-concepts-and-use-cases#what-are-traits)
    for more information.
    """


@kind(CATALOG_RESOURCE_GROUP, "dataset", "v1alpha3")
class DatasetSpec(ResourceSpecABC):
    """Dataset specification model"""

    storage_class: str
    """Storage class qualified name"""

    storage_controller: Optional[str] = None
    """Storage controller qualified name"""

    data_collection: Optional[str] = None
    """Data collection qualified name"""

    maintainer: ContactInfo
    """Active maintainer information"""

    citation: Optional[Citation] = None
    """Citation information"""

    documentation: List[str] = Field(default_factory=list)
    """Links to any relevant documentation for the dataset"""

    facets: Optional[Dict[str, Any]] = None
    """Facets for the dataset"""

    tags: Set[str] = Field(default_factory=set)
    """Tags for the dataset"""
