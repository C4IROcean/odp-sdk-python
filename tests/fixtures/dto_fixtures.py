from uuid import uuid4

import pytest

from odp_sdk.dto import MetadataDto, ResourceDto

__all__ = [
    "raw_resource_dto",
]


@pytest.fixture()
def raw_resource_dto():
    name = "test_dataset"
    kind = "catalog.hubocean.io/dataset"
    version = "v1alpha3"
    uuid = uuid4()
    return ResourceDto(
        kind=kind,
        version=version,
        metadata=MetadataDto(name=name, uuid=uuid),
        spec=dict(
            storage_class="registry.hubocean.io/storageClass/raw",
            maintainer={"organization": "HUB Ocean"},
            documentation=["https://oceandata.earth"],
            tags=["test", "hubocean"],
        ),
    )
