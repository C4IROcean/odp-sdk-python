import pytest
from odp_sdk.client import ODPClient


def test_login():

    client = ODPClient()
    inspection_result = client.iam.token.inspect()

    assert inspection_result
    assert inspection_result["subject"]