from typing import List, Optional, Set

from pydantic import BaseModel

from odp_sdk.dto import ResourceDto


class CitationDto(BaseModel):
    cite_as: str
    """Directions on how to cite the dataset"""

    doi: str
    """Digital Object Identifier ([DOI](doi.org))"""


class AttributeDto(BaseModel):
    """Dataset attributes"""

    name: str
    """Attribute name"""

    description: Optional[str] = None
    """Attribute description"""

    traits: List[str]
    """List of traits"""


class DatasetSpec(BaseModel):
    storage_class: str
    """Storage class qualified name"""

    storage_controller: Optional[str] = None
    """Storage controller qualified name"""

    data_collection: Optional[str] = None
    """Data collection qualified name"""

    maintainer: dict
    """Active maintainer information"""

    citation: Optional[CitationDto] = None
    """Citation information"""

    documentation: List[str]
    """Links to any relevant documentation"""

    attributes: List[AttributeDto] = None
    """Dataset attributes"""

    facets: Optional[str] = None

    tags: Set[str]


class DatasetDto(ResourceDto):
    _kind: str = "catalog.hubocean.io/dataset"
    _version: str = "v1alpha3"

    spec: DatasetSpec
