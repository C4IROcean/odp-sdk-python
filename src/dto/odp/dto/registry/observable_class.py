from typing import Any, Dict

from ..resource import ResourceSpecABC


class ObservableClassSpec(ResourceSpecABC):
    """Observable class specification model"""

    observable_schema: Dict[str, Any]
    """JSON schema for the observable class"""
