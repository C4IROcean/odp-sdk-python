"""This module contains the set of ODP SDK exceptions."""


class OdpError(Exception):
    """Base class for exceptions in this module."""


class OdpAuthError(OdpError):
    """Exception raised for authentication errors."""


class OdpUnauthorizedError(OdpError):
    """Exception raised for unauthorized requests."""


class OdpTokenValidationError(OdpError):
    """Exception raised for invalid tokens."""
