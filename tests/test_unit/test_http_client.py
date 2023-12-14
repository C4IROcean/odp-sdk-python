import responses


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
