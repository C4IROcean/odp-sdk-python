import re

from odp_sdk.utils import get_version


def test_get_version():
    assert re.match(r"^(\d+\.)?(\d+\.)?(\d+)$", get_version())
