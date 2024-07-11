import pytest
import responses
from odp.client.auth import TokenProvider
from odp.client.http_client import OdpHttpClient
from test_sdk.fixtures.jwt_fixtures import MOCK_TOKEN_ENDPOINT


def test_request_relative(http_client: OdpHttpClient, request_mock: responses.RequestsMock):
    request_mock.add(responses.GET, f"{http_client.base_url}/foobar", status=200)

    res = http_client.get("/foobar")
    res.raise_for_status()

    assert res.status_code == 200


def test_request_absolute(http_client: OdpHttpClient, request_mock: responses.RequestsMock):
    test_url = "http://someurl.local"

    assert test_url != http_client.base_url

    request_mock.add(responses.GET, test_url, status=200)

    res = http_client.get(test_url)
    res.raise_for_status()

    assert res.status_code == 200


def test_request_has_auth_token(http_client: OdpHttpClient, request_mock: responses.RequestsMock):
    def _on_request(request):
        assert "Authorization" in request.headers

        auth_header = request.headers["Authorization"]
        assert auth_header is not None
        assert auth_header.startswith("Bearer ")

        return (200, {}, None)

    request_mock.add_callback(
        responses.GET,
        f"{http_client.base_url}/foobar",
        callback=_on_request,
    )

    http_client.get("/foobar")


def test_request_reuse_auth_token(http_client: OdpHttpClient, request_mock: responses.RequestsMock):
    request_mock.add(responses.GET, f"{http_client.base_url}/foobar", status=200)

    res = http_client.get("/foobar")
    res.raise_for_status()

    assert res.status_code == 200
    assert request_mock.assert_call_count(MOCK_TOKEN_ENDPOINT, 1)

    res = http_client.get("/foobar")
    res.raise_for_status()

    assert res.status_code == 200
    assert request_mock.assert_call_count(MOCK_TOKEN_ENDPOINT, 1)


@pytest.mark.mock_time(use_time=123)
def test_request_renew_auth_token(http_client: OdpHttpClient, request_mock: responses.RequestsMock, mock_time):
    request_mock.add(responses.GET, f"{http_client.base_url}/foobar", status=200)

    res = http_client.get("/foobar")
    res.raise_for_status()

    assert res.status_code == 200
    assert request_mock.assert_call_count(MOCK_TOKEN_ENDPOINT, 1)

    mock_time.advance(3600)

    res = http_client.get("/foobar")
    res.raise_for_status()

    assert res.status_code == 200
    assert request_mock.assert_call_count(MOCK_TOKEN_ENDPOINT, 2)


def test_custom_user_agent(http_client: OdpHttpClient, request_mock: responses.RequestsMock):
    custom_user_agent = "my-custom-user-agent"

    http_client.custom_user_agent = custom_user_agent

    test_url = "http://someurl.local"

    assert test_url != http_client.base_url

    request_mock.add(responses.GET, test_url, status=200)

    res = http_client.get(test_url)
    res.raise_for_status()

    assert res.status_code == 200

    assert request_mock.calls[1].request.headers["User-Agent"] == custom_user_agent


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
def test_http_client_url(jwt_token_provider: TokenProvider, url: str, expected: bool):
    try:
        http_client = OdpHttpClient(base_url=url, token_provider=jwt_token_provider)
        assert http_client.base_url == url and expected
    except ValueError:
        assert not expected
