from typing import Any, Dict

from ..resource import ResourceSpecABC
from ..resource_registry import kind
from ._rg import CATALOG_RESOURCE_GROUP


@kind(CATALOG_RESOURCE_GROUP, "observable", "v1alpha2")
class ObservableSpec(ResourceSpecABC):
    observable_class: str
    """Observable class"""

    ref: str
    """Qualified name of the associated dataset or data collection"""

    details: Dict[str, Any]
    """Full observable object"""
