"""Authentication handling in the form of token-providers"""
from abc import ABC, abstractmethod
from typing import List

import requests
from pydantic import BaseModel, SecretStr
from requests.auth import AuthBase


class TokenProvider(AuthBase, ABC):
    """Base class for token providers"""

    @abstractmethod
    def get_token(self) -> str:
        """Returns the token to be used for authentication

        Returns:
            str: The token to be used for authentication

        Raises:

        """

    def __call__(self, r: requests.PreparedRequest) -> requests.PreparedRequest:
        r.headers["Authorization"] = self.get_token()
        return r


class OdpWorkspaceTokenProvider(TokenProvider):
    """Token provider for ODP workspaces"""

    def get_token(self) -> str:
        res = requests.post("http://localhost:8000/access_token")
        res.raise_for_status()

        return "Bearer " + res.json()["token"]


class HardcodedTokenProvider(TokenProvider):
    """Token provider for hardcoded tokens"""

    def __init__(self, token: str):
        self._token = token

    def get_token(self) -> str:
        return self._token


class Oauth2TokenProvider(TokenProvider, BaseModel):
    authority: SecretStr
    client_id: SecretStr
    client_secret: SecretStr
    audience: str

    scope: List[str]

    def get_token(self) -> str:
        raise NotImplementedError("InteractiveTokenProvider not implemented yet")


class InteractiveTokenProvider(TokenProvider):
    """Token provider for interactive token input

    This token provider is intended to be used for interactive sessions, where the user is prompted for
    login credentials by a web browser. The token is then retrieved from the browser and used for authentication.
    """

    def get_token(self) -> str:
        raise NotImplementedError("InteractiveTokenProvider not implemented yet")
