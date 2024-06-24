import json
from contextlib import contextmanager
from typing import Any, Dict, Iterable, Literal, Optional, Union

import requests
import validators
from pydantic import BaseModel, field_validator

from .auth import TokenProvider
from .exc import OdpForbiddenError, OdpUnauthorizedError

ParamT = Optional[Dict[str, Any]]
HeaderT = Optional[Dict[str, Any]]
ContentT = Union[bytes, str, dict, list, BaseModel, None]


class OdpHttpClient(BaseModel):
    base_url: str = "https://api.hubocean.earth"
    token_provider: TokenProvider
    custom_user_agent: Optional[str] = None

    @field_validator("base_url")
    @classmethod
    def _validate_url(cls, v: str) -> str:
        if not validators.url(v, simple_host=True):
            raise ValueError(f"Invalid base URL: {v}")

        return v.rstrip("/")

    def get(
        self,
        url: str,
        params: ParamT = None,
        headers: HeaderT = None,
        timeout: Optional[float] = None,
        stream: bool = False,
    ) -> requests.Response:
        """Make a GET request

        Args:
            url: URL to request. If it starts with a "/", it will be appended to the base URL.
            params: Query parameters
            headers: HTTP headers
            timeout: Timeout in seconds
            stream: If True, the response will be streamed.

        Returns:
            The response object
        """
        return self._request("GET", url, params=params, headers=headers, timeout=timeout, stream=stream)

    def post(
        self,
        url: str,
        params: ParamT = None,
        headers: HeaderT = None,
        timeout: Optional[float] = None,
        content: ContentT = None,
        stream: bool = False,
    ) -> requests.Response:
        """Make a POST request

        Args:
            url: URL to request. If it starts with a "/", it will be appended to the base URL.
            params: Query parameters
            headers: HTTP headers
            timeout: Timeout in seconds
            content: Request body content.
            stream: If True, the response will be streamed.

        Returns:
            The response object
        """
        return self._request(
            "POST",
            url,
            params=params,
            headers=headers,
            timeout=timeout,
            content=content,
            stream=stream,
        )

    def patch(
        self,
        url: str,
        params: ParamT = None,
        headers: HeaderT = None,
        timeout: Optional[float] = None,
        content: ContentT = None,
        stream: bool = False,
    ) -> requests.Response:
        """Make a PATCH request

        Args:
            url: URL to request. If it starts with a "/", it will be appended to the base URL.
            params: Query parameters
            headers: HTTP headers
            timeout: Timeout in seconds
            content: Request body content.
            stream: If True, the response will be streamed.

        Returns:
            The response object
        """
        return self._request(
            "PATCH",
            url,
            params=params,
            headers=headers,
            timeout=timeout,
            content=content,
            stream=stream,
        )

    def put(
        self,
        url: str,
        params: ParamT = None,
        headers: HeaderT = None,
        timeout: Optional[float] = None,
        content: ContentT = None,
        stream: bool = False,
    ) -> requests.Response:
        """Make a PUT request

        Args:
            url: URL to request. If it starts with a "/", it will be appended to the base URL.
            params: Query parameters
            headers: HTTP headers
            timeout: Timeout in seconds
            content: Request body content.
            stream: If True, the response will be streamed.

        Returns:
            The response object
        """
        return self._request(
            "PUT",
            url,
            params=params,
            headers=headers,
            timeout=timeout,
            content=content,
            stream=stream,
        )

    def delete(
        self,
        url: str,
        params: ParamT = None,
        headers: HeaderT = None,
        timeout: Optional[float] = None,
        stream: bool = False,
    ) -> requests.Response:
        """Make a DELETE request

        Args:
            url: URL to request. If it starts with a "/", it will be appended to the base URL.
            params: Query parameters
            headers: HTTP headers
            timeout: Timeout in seconds
            stream: If True, the response will be streamed.

        Returns:
            The response object
        """
        return self._request(
            "DELETE",
            url,
            params=params,
            headers=headers,
            timeout=timeout,
            stream=stream,
        )

    @contextmanager
    def _session(self) -> Iterable[requests.Session]:
        """Context manager for a requests session

        Will add authentication to the session if a token provider is set.

        Yields:
            A requests session
        """
        s = requests.Session()
        if self.token_provider:
            s.auth = self.token_provider

        yield s

    def _request(
        self,
        method: Literal["GET", "POST", "PUT", "PATCH", "DELETE"],
        url: str,
        params: ParamT = None,
        headers: HeaderT = None,
        timeout: Optional[float] = None,
        content: ContentT = None,
        stream: bool = False,
    ) -> requests.Response:
        """Make an HTTP request

        Args:
            method: HTTP method (e.g. "GET", "POST", etc.)
            url: URL to request. If it starts with a "/", it will be appended to the base URL.
            params: Query parameters
            headers: HTTP headers
            timeout: Timeout in seconds
            content: Request body content.
                If it is a dict or list, it will be serialized as JSON.
                If it is a pydantic BaseModel, it will be serialized as JSON.
            stream: If True, the response will be streamed.

        Returns:
            The response object

        Raises:
            OdpUnauthorizedError: Unauthorized request
            OdpForbiddenError: Forbidden request
        """

        if url.startswith("/"):
            base_url = self.base_url
        elif url.startswith(self.base_url):
            url = url[len(self.base_url) :]
            base_url = self.base_url
        else:
            base_url = ""

        headers = headers or {}
        if self.custom_user_agent:
            headers["User-Agent"] = self.custom_user_agent
        else:
            headers["User-Agent"] = self.token_provider.user_agent

        if isinstance(content, (dict, list)):
            body = json.dumps(content)
            headers["Content-Type"] = "application/json"
        elif isinstance(content, BaseModel):
            body = content.model_dump_json()
            headers["Content-Type"] = "application/json"
        elif isinstance(content, (bytes, str)):
            body = content
            headers.setdefault("Content-Type", "application/octet-stream")
        else:
            body = None

        request_url = f"{base_url}{url}"

        req = requests.Request(method, request_url, params=params, headers=headers, data=body)

        with self._session() as s:
            prepped = s.prepare_request(req)
            res = s.send(prepped, timeout=timeout, stream=stream)

            if res.status_code == 401:
                raise OdpUnauthorizedError("Unauthorized request")
            elif res.status_code == 403:
                raise OdpForbiddenError("Forbidden request")

            return res
