from datetime import datetime
from uuid import uuid4

import responses

from odp_sdk.dto.resource_dto import MetadataDto, ResourceDto, ResourceStatusDto
from odp_sdk.resource_client import OdpResourceClient


def test_get_resource_by_uuid(http_client):
    resource_endpoint = "/resource"
    kind = "test.hubocean.io/tesType"
    version = "v1alpha1"
    name = "test"
    uuid = uuid4()

    client = OdpResourceClient(http_client=http_client, resource_endpoint=resource_endpoint)
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            f"{client.resource_url}/{uuid}",
            body=ResourceDto(
                kind=kind,
                version=version,
                metadata=MetadataDto(name=name, uuid=uuid),
                status=ResourceStatusDto(
                    num_updates=0,
                    created_time=datetime.fromisoformat("2021-01-01T00:00:00Z"),
                    created_by=uuid4(),
                    updated_time=datetime.fromisoformat("2021-01-01T00:00:00Z"),
                    updated_by=uuid4(),
                ),
                spec={},
            ).model_dump_json(),
            status=200,
            content_type="application/json",
        )

        manifest = client.get(uuid)

        assert manifest.kind == kind
        assert manifest.version == version
        assert manifest.metadata.name == name


def test_get_resource_by_qname(http_client):
    resource_endpoint = "/resource"
    kind = "test.hubocean.io/tesType"
    version = "v1alpha1"
    name = "test"
    uuid = uuid4()

    client = OdpResourceClient(http_client=http_client, resource_endpoint=resource_endpoint)
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            f"{client.resource_url}/{kind}/{name}",
            body=ResourceDto(
                kind=kind,
                version=version,
                metadata=MetadataDto(name=name, uuid=uuid),
                status=ResourceStatusDto(
                    num_updates=0,
                    created_time=datetime.fromisoformat("2021-01-01T00:00:00Z"),
                    created_by=uuid4(),
                    updated_time=datetime.fromisoformat("2021-01-01T00:00:00Z"),
                    updated_by=uuid4(),
                ),
                spec={},
            ).model_dump_json(),
            status=200,
            content_type="application/json",
        )

        manifest = client.get(f"{kind}/{name}")

        assert manifest.kind == kind
        assert manifest.version == version
        assert manifest.metadata.name == name
        assert manifest.metadata.uuid == uuid
        assert manifest.metadata.uuid == uuid
