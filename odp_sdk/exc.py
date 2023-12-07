"""This module contains the set of ODP SDK exceptions."""
from typing import Any


class OdpError(Exception):
    """Base class for exceptions in this module."""


class OdpAuthError(OdpError):
    """Exception raised for authentication errors."""


class OdpUnauthorizedError(OdpError):
    """Exception raised for unauthorized requests."""


class OdpForbiddenError(OdpError):
    """Exception raised for forbidden requests."""


class OdpTokenValidationError(OdpError):
    """Exception raised for invalid tokens."""


class OdpResourceNotFoundError(OdpError):
    """Exception raised when a resource is not found."""


class OdpResourceExistsError(OdpError):
    """Exception raised when a resource already exists."""


class OdpValidationError(OdpError):
    """Exception raised when a resource is not found."""


class OpenTableStageInvalidAction(OdpError):
    """Exception when table is getting deleted and it has active sessions."""


class OqsBaseException(Exception):
    """Base exception for OQS"""


class OqsEvaluationError(OqsBaseException):
    """Error evaluating OQS"""

    def __int__(self, msg: str, fallback_value: Any = None):
        super().__init__(msg)

    @property
    def fallback_value(self):
        return self.args[1]
