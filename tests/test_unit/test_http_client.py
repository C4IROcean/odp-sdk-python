import pytest
import responses

from odp_sdk.auth import TokenProvider
from odp_sdk.http_client import OdpHttpClient


def test_request_relative(http_client):
    with responses.RequestsMock() as rsps:
        rsps.add(responses.GET, f"{http_client.base_url}/foobar", status=200)

        res = http_client.get("/foobar")
        res.raise_for_status()

        assert res.status_code == 200


def test_request_absolute(http_client):
    test_url = "http://someurl.local"

    assert test_url != http_client.base_url

    with responses.RequestsMock() as rsps:
        rsps.add(responses.GET, test_url, status=200)

        res = http_client.get(test_url)
        res.raise_for_status()

        assert res.status_code == 200


def test_request_has_auth_token(http_client):
    def _on_request(request):
        assert "Authorization" in request.headers

        auth_header = request.headers["Authorization"]
        assert auth_header is not None
        assert auth_header.startswith("Bearer ")

        return (200, {}, None)

    with responses.RequestsMock() as rsps:
        rsps.add_callback(
            responses.GET,
            f"{http_client.base_url}/foobar",
            callback=_on_request,
        )

        http_client.get("/foobar")


def test_custom_user_agent(http_client):
    custom_user_agent = "my-custom-user-agent"

    http_client.custom_user_agent = custom_user_agent

    test_url = "http://someurl.local"

    assert test_url != http_client.base_url

    with responses.RequestsMock() as rsps:
        rsps.add(responses.GET, test_url, status=200)

        res = http_client.get(test_url)
        res.raise_for_status()

        assert res.status_code == 200
        assert rsps.calls[0].request.headers["User-Agent"] == custom_user_agent


@pytest.mark.parametrize(
    "url, expected",
    [
        ("http://localhost:8888", True),
        ("localhost:8888", False),
        ("foo.bar", False),
        ("https://foo.bar.com", True),
        ("not a valid url", False),
    ],
)
def test_http_client_url(mock_token_provider: TokenProvider, url: str, expected: bool):
    try:
        http_client = OdpHttpClient(base_url=url, token_provider=mock_token_provider)
        assert http_client.base_url == url and expected
    except ValueError:
        assert not expected


def test_http_client_invalid_url(mock_token_provider: TokenProvider):
    with pytest.raises(ValueError):
        http_client = OdpHttpClient(base_url="localhost:8888", token_provider=mock_token_provider)
        assert http_client.base_url == "localhost:8888"
