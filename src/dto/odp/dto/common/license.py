from typing import Optional

from pydantic import BaseModel


class License(BaseModel):
    """Data license information"""

    name: str
    """License name. Can be set to `Proprietary` for proprietary licenses"""

    href: Optional[str] = None
    """HREF to license text"""

    full_text: Optional[str] = None
    """Full license text"""
