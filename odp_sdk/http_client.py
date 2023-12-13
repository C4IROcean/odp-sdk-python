import json
import re
from contextlib import contextmanager
from typing import Iterable, Literal, Optional

import requests
from pydantic import BaseModel, field_validator

from .auth import TokenProvider
from .exc import OdpForbiddenError, OdpUnauthorizedError


class OdpHttpClient(BaseModel):
    base_url: str = "https://api.hubocean.earth"
    token_provider: TokenProvider

    @field_validator("base_url")
    @classmethod
    def _validate_url(cls, v: str) -> str:
        m = re.match(
            r"https?:\/\/(www\.|localhost)?[-a-zA-Z0-9@:%._\+~#=]"
            r"{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*|:\d+)",
            v,
        )
        if not m:
            raise ValueError(f"Invalid base URL: {v}")

        return v.rstrip("/")

    def get(
        self,
        url: str,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        timeout: Optional[float] = None,
        stream: bool = False,
    ):
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
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        timeout: Optional[float] = None,
        content: Optional[bytes | str | dict | list | BaseModel] = None,
        stream: bool = False,
    ):
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
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        timeout: Optional[float] = None,
        content: Optional[bytes | str | dict | list | BaseModel] = None,
        stream: bool = False,
    ):
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
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        timeout: Optional[float] = None,
        content: Optional[bytes | str | dict | list | BaseModel] = None,
        stream: bool = False,
    ):
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
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        timeout: Optional[float] = None,
        stream: bool = False,
    ):
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
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        timeout: Optional[float] = None,
        content: Optional[bytes | str | dict | list | BaseModel] = None,
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
