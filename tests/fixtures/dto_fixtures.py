import datetime
from uuid import uuid4

import pytest

from odp_sdk.dto import MetadataDto, ResourceDto
from odp_sdk.dto.table_spec import TableSpec

__all__ = [
    "raw_resource_dto",
    "tabular_resource_dto",
    "table_spec",
    "table_stage",
]

from odp_sdk.dto.tabular_store import TableStage


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


@pytest.fixture()
def tabular_resource_dto():
    name = "test_dataset"
    kind = "catalog.hubocean.io/dataset"
    version = "v1alpha3"
    uuid = uuid4()
    return ResourceDto(
        kind=kind,
        version=version,
        metadata=MetadataDto(name=name, uuid=uuid),
        spec=dict(
            storage_class="registry.hubocean.io/storageClass/tabular",
            maintainer={"organization": "HUB Ocean"},
            documentation=["https://oceandata.earth"],
            tags=["test", "hubocean"],
        ),
    )


@pytest.fixture()
def table_spec():
    table_schema = {
        "CatalogNumber": {"type": "long"},
        "Location": {"type": "geometry"},
    }

    return TableSpec(table_schema=table_schema)


@pytest.fixture()
def table_stage():
    return TableStage(
        stage_id=uuid4(),
        status="active",
        created_time=datetime.datetime.now(),
        expiry_time=datetime.MAXYEAR
    )
