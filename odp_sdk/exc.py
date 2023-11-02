"""This module contains the set of ODP SDK exceptions."""


class OdpError(Exception):
    """Base class for exceptions in this module."""


class OdpUnauthorizedError(OdpError):
    """Exception raised for unauthorized requests.

    Attributes:
        message -- explanation of the error
    """
