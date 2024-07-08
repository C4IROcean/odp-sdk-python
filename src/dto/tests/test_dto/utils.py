from typing import Annotated

from odp.dto import ResourceRegistry, ResourceSpecABC, kind
from pydantic import Field
from pydantic.functional_validators import BeforeValidator

TESTS_RESOURCE_REGISTRY = ResourceRegistry()


def _validate_starts_with(s: str, p: str) -> str:
    if not s.startswith(p):
        raise ValueError(f"string does not start with {p}")
    return s


@kind("test.hubocean.io", "mock", "v1alpha1", TESTS_RESOURCE_REGISTRY)
class MockSpec(ResourceSpecABC):
    pass


@kind("test.hubocean.io", "simple", "v1alpha1", TESTS_RESOURCE_REGISTRY)
class SimpleSpec(ResourceSpecABC):
    some_str: str
    some_int: int = Field(..., ge=1)


class UnregisteredSpec(ResourceSpecABC):
    some_float: float
    some_validated_str: Annotated[str, BeforeValidator(lambda s: _validate_starts_with(s, "foo"))]
