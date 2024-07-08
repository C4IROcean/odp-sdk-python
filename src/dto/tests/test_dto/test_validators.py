from typing import Optional

import pytest
from odp.dto.validators import validate_resource_kind, validate_resource_version


@pytest.mark.parametrize(
    "test_value,expected",
    [
        ("hubocean.io/testGroup", True),
        ("catalog.hubocean.io/testGroup", True),
        ("function.domain.com/testGroup", True),
        ("hubocean.io/testGroup/testProject", False),
        ("foobar", False),
    ],
)
def test_validate_kind(test_value: Optional[str], expected: bool):
    if expected:
        assert test_value == validate_resource_kind(test_value)
    else:
        with pytest.raises(ValueError):
            validate_resource_kind(test_value)


@pytest.mark.parametrize(
    "test_value,expected",
    [
        ("v1alpha1", True),
        ("v1beta1", True),
        ("v2", True),
        ("v3alpha2", True),
        ("v1", True),
        ("v1alpha", False),
        ("v1beta", False),
        ("v1alpha1beta1", False),
        ("foobar", False),
        ("v100", True),
        ("v99999999", True),
        ("v1545325alpha6546464564", True),
    ],
)
def test_validate_resource_version(test_value: Optional[str], expected: bool):
    if expected:
        assert test_value == validate_resource_version(test_value)
    else:
        with pytest.raises(ValueError):
            validate_resource_version(test_value)
