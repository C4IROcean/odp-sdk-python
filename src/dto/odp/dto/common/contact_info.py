from typing import Optional

from pydantic import BaseModel


class ContactInfo(BaseModel):
    """Contact information for a user"""

    contact: str
    """Contact in the form `Firstname Lastname <email>`"""

    organization: Optional[str] = None
    """Organization name"""
