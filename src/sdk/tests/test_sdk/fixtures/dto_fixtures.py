import datetime
from uuid import uuid4

import pytest
from odp.client.dto.table_spec import TableSpec
from odp.client.dto.tabular_store import TableStage
from odp.dto import DatasetDto, DatasetSpec, Metadata

__all__ = [
    "raw_resource_dto",
    "tabular_resource_dto",
    "table_spec",
    "table_stage",
]

from odp.dto.common.contact_info import ContactInfo


@pytest.fixture()
def raw_resource_dto() -> DatasetDto:
    name = "test_dataset"
    uuid = uuid4()
    return DatasetDto(
        metadata=Metadata(name=name, uuid=uuid),
        spec=DatasetSpec(
            storage_class="registry.hubocean.io/storageClass/raw",
            maintainer=ContactInfo(
                organisation="HUB Ocean", contact="Name McNameson <name.mcnameson@emailprovider.com>"
            ),
            documentation=["https://oceandata.earth"],
            tags={"test", "hubocean"},
        ),
    )


@pytest.fixture()
def tabular_resource_dto() -> DatasetDto:
    name = "test_dataset"
    uuid = uuid4()

    return DatasetDto(
        metadata=Metadata(name=name, uuid=uuid),
        spec=DatasetSpec(
            storage_class="registry.hubocean.io/storageClass/tabular",
            maintainer=ContactInfo(
                organisation="HUB Ocean", contact="Name McNameson <name.mcnameson@emailprovider.com>"
            ),
            documentation=["https://oceandata.earth"],
            tags={"test", "hubocean"},
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
        stage_id=uuid4(), status="active", created_time=datetime.datetime.now(), expiry_time=datetime.MAXYEAR
    )
