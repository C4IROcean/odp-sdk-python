"""Authentication handling in the form of token-providers"""
import base64
import json
import logging
import os
import re
import sys
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Optional, Union

import jwt
import msal
import msal_extensions
import requests
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicNumbers
from pydantic import BaseModel, PrivateAttr, SecretStr
from requests.auth import AuthBase

from .exc import OdpAuthError
from .utils import get_version

LOG = logging.getLogger(__name__)


class TokenProvider(AuthBase, BaseModel, ABC):
    """Base class for token providers"""

    user_agent: str = "odp-sdk/" + get_version()

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

    token_uri: str = "http://localhost:8000/access_token"
    """Token endpoint."""

    def __init__(self, **data):
        super().__init__(**data)
        self.user_agent = self.user_agent + " (Workspaces)"

    def get_token(self) -> str:
        res = requests.post(self.token_uri)
        res.raise_for_status()

        return "Bearer " + res.json()["token"]


class HardcodedTokenProvider(TokenProvider):
    """Token provider for hardcoded tokens"""

    _token: str = PrivateAttr()

    def __init__(self, token: str, **data):
        super().__init__(**data)
        self._token = token
        self.user_agent = self.user_agent + " (Hardcoded)"

    def get_token(self) -> str:
        return self._token


class JwtTokenProvider(TokenProvider, ABC):
    """Token provider for JWT tokens"""

    audience: Optional[str] = None
    """IDP token audience

    Must be set if the token is to be validated
    """

    validate_token: bool = False
    """Whether to validate the token or not"""

    token_exp_lee_way: int = 300
    """Number of seconds before token expiry we should refresh the token"""

    _access_token: str = PrivateAttr(None)
    _refresh_token: Optional[str] = PrivateAttr(None)
    _claims: Dict[str, Union[int, str]] = PrivateAttr(None)
    _jwks: Dict[str, str] = PrivateAttr(None)
    _expiry: int = PrivateAttr(0)

    @abstractmethod
    def authenticate(self) -> Dict[str, str]:
        """Authenticate against the IDP and return the token

        Returns:
            The token response from the IDP

        Raises:
            OdpAuthError: If the token cannot be retrieved
        """

    @abstractmethod
    def get_jwks_uri(self) -> str:
        """Get the jwks uri for the identity provider

        Returns:
            The jwks uri
        """

    def get_token(self) -> str:
        """Returns the token to be used for authentication

        Returns:
            The token to be used for authentication

        Raises:
            OdpAuthError: If the token cannot be retrieved
        """

        if self._access_token and time.time() < self._expiry - self.token_exp_lee_way:
            access_token = self._access_token
        else:
            auth_response = self.authenticate()
            access_token = self._parse_token(auth_response)

        return "Bearer {}".format(access_token)

    def _parse_and_validate(self, access_token: str) -> str:
        token_components = [self._base64_decode(x) for x in access_token.split(".")]
        headers = json.loads(token_components[0])

        if not headers:
            raise OdpAuthError("No headers in token")

        kid = headers.pop("kid")
        jwk = self._get_jwk(kid)

        pem = (
            RSAPublicNumbers(n=self._decode_jwk_value(jwk["n"]), e=self._decode_jwk_value(jwk["e"]))
            .public_key(default_backend())
            .public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo,
            )
        )

        self._claims = jwt.decode(
            access_token,
            pem,
            verify=True,
            options=headers,
            audience=self.audience,
            algorithms=["RS256"],
        )

        self._access_token = access_token
        self._expiry = self._claims["exp"]

        return access_token

    def _parse_novalidate(self, access_token: str) -> str:
        self._claims = jwt.decode(access_token, options={"verify_signature": False})
        self._access_token = access_token
        self._expiry = self._claims["exp"]

        return access_token

    def _parse_token(self, token_response: Dict[str, str]) -> str:
        """Parse the token from the token response

        Args:
            The token response from the IDP

        Returns:
            str: The token
        """
        try:
            access_token = token_response["access_token"]
        except KeyError as e:
            raise OdpAuthError("No access token in token response") from e

        if not self.validate_token:
            return self._parse_novalidate(access_token)
        return self._parse_and_validate(access_token)

    def _get_jwk(self, kid: str) -> Dict:
        jwks = self._get_jwks()
        for jwk in jwks["keys"]:
            if jwk.get("kid") == kid:
                return jwk

        raise OdpAuthError("Invalid token: Invalid KID")

    def _get_jwks(self) -> Dict:
        if self._jwks:
            return self._jwks

        jwks_uri = self.get_jwks_uri()

        res = requests.get(jwks_uri)
        res.raise_for_status()

        self._jwks = res.json()
        return self._jwks

    @staticmethod
    def _base64_decode(data: str, altchars: bytes = b"+/") -> bytes:
        d = re.sub(rb"[^a-zA-Z0-9%s]+" % altchars, b"", data.encode("utf-8"))

        if len(d) % 4 == 1:
            d = d[:-1]
        else:
            # Add padding
            d += (-len(d) % 4) * b"="
        decoded = base64.b64decode(d, altchars=altchars)
        return decoded

    @staticmethod
    def _decode_jwk_value(val: str) -> int:
        decoded = base64.urlsafe_b64decode(val.encode("utf-8") + b"==")
        return int.from_bytes(decoded, "big")


class AzureTokenProvider(JwtTokenProvider):
    """Token provider for OAuth2 tokens"""

    client_id: SecretStr
    """IDP client ID"""

    client_secret: SecretStr
    """IDP client secret"""

    authority: str = "https://oceandataplatform.b2clogin.com/755f6e58-74f0-4a07-a599-f7479b9669ab/v2.0/"
    """IDP token Authority"""

    scope: List[str] = ["https://oceandataplatform.onmicrosoft.com/odcat/.default"]
    """IDP token scope"""

    tenant_id: str = "755f6e58-74f0-4a07-a599-f7479b9669ab"
    """Azure AD tenant id"""

    token_uri: str = "https://oceandataplatform.b2clogin.com/oceandataplatform.onmicrosoft.com/b2c_1a_signup_signin_custom/oauth2/v2.0/token"  # noqa: E501
    """Token endpoint. Will default to 'https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token'"""

    jwks_uri: str = "https://oceandataplatform.b2clogin.com/oceandataplatform.onmicrosoft.com/b2c_1a_signup_signin_custom/discovery/v2.0/keys"  # noqa: E501
    """JWKS endpoint."""

    def __init__(self, **data):
        super().__init__(**data)
        self.user_agent = self.user_agent + " (Azure)"

    def get_jwks_uri(self) -> str:
        return self.jwks_uri

    def authenticate(self) -> Dict[str, str]:
        res = requests.post(
            self.token_uri,
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id.get_secret_value(),
                "client_secret": self.client_secret.get_secret_value(),
                "audience": self.audience,
                "scope": " ".join(self.scope),
            },
        )

        res.raise_for_status()
        return res.json()


class InteractiveTokenProvider(JwtTokenProvider):
    """Token provider for interactive token input

    This token provider is intended to be used for interactive sessions, where the user is prompted for
    login credentials by a web browser. The token is then retrieved from the browser and used for authentication.
    """

    client_id: SecretStr
    """IDP client ID"""

    authority: str = (
        "https://oceandataplatform.b2clogin.com/oceandataplatform.onmicrosoft.com/b2c_1a_signup_signin_custom"
    )
    """IDP token Authority"""

    scope: List[str] = ["https://oceandataplatform.onmicrosoft.com/odcat/API_ACCESS"]
    """IDP token scope"""

    jwks_uri: str = "https://oceandataplatform.b2clogin.com/oceandataplatform.onmicrosoft.com/b2c_1a_signup_signin_custom/discovery/v2.0/keys"  # noqa: E501
    """JWKS endpoint."""

    token_persistence_file: Path = Path(".token_cache.bin")
    """File to save the token cache to"""

    token_persistence_plaintext_fallback: bool = True
    """Whether to fallback to plaintext if encryption is unavailable"""

    _app: msal.PublicClientApplication = PrivateAttr(None)

    def __init__(self, **data):
        super().__init__(**data)

        persistence = self._build_persistence(self.token_persistence_file, self.token_persistence_plaintext_fallback)
        cache = msal_extensions.PersistedTokenCache(persistence)

        self._app = msal.PublicClientApplication(
            client_id=self.client_id.get_secret_value(),
            authority=self.authority,
            token_cache=cache,
        )
        self.user_agent = self.user_agent + " (Interactive)"

    def get_jwks_uri(self) -> str:
        return self.jwks_uri

    def authenticate(self) -> Dict[str, str]:
        accounts = self._app.get_accounts()
        if accounts and len(accounts) == 1:
            account = accounts[0]

            LOG.info(
                "Found account %s. Attempting to acquire token silently",
                account["username"],
            )
            token = self._app.acquire_token_silent(scopes=self.scope, account=account)
        else:
            token = None

        if token is None:
            LOG.info("No suitable token found. Acquiring token interactively")
            res = self._app.acquire_token_interactive(
                scopes=self.scope,
            )
            if "error" in res:
                raise OdpAuthError("{}: {}".format(res["error"], res["error_description"]))

            # Token acquired successfully, rerun the method to get the token
            return self.authenticate()

        return token

    @staticmethod
    def _build_persistence(location: Path, fallback_to_plaintext: bool = False):
        if sys.platform.startswith("win"):
            return msal_extensions.FilePersistenceWithDataProtection(location)
        elif sys.platform.startswith("darwin"):
            return msal_extensions.KeychainPersistence(location, "odcat", "odcat-user")
        elif sys.platform.startswith("linux"):
            try:
                return msal_extensions.LibsecretPersistence(
                    str(location),
                    schema_name="odcat-sdk",
                    attributes={"app": "odcat", "component": "odcat-cli"},
                )
            except Exception:
                if not fallback_to_plaintext:
                    raise
                LOG.warning("Encryption unavailable. Opting in to plaintext")
        return msal_extensions.FilePersistence(str(location))


def get_default_token_provider(fallback: bool = True) -> TokenProvider:
    """Automatically select a token provider based on the environment

    Args:
        fallback: Whether to fallback to an interactive token provider if no other token provider is available

    Returns:
        The token provider

    Raises:
        OdpAuthError: If no token provider is available and fallback is False
    """
    if hardcoded_access_token := os.getenv("ODP_ACCESS_TOKEN"):
        return HardcodedTokenProvider(hardcoded_access_token)

    if os.getenv("JUPYTERHUB_API_TOKEN"):
        return OdpWorkspaceTokenProvider()

    client_id = os.getenv("ODP_CLIENT_ID", "f96fc4a5-195b-43cc-adb2-10506a82bb82")
    client_secret = os.getenv("ODP_CLIENT_SECRET")

    if client_secret:
        return AzureTokenProvider(client_id=SecretStr(client_id), client_secret=SecretStr(client_secret))

    if fallback:
        return InteractiveTokenProvider(client_id=SecretStr(client_id))

    raise OdpAuthError("Unable to find a suitable token provider")
