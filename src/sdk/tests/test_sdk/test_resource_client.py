import json
from datetime import datetime
from uuid import UUID, uuid4

import pytest
import responses
from odp.client.resource_client import OdpResourceClient
from odp.dto import Metadata, ResourceDto, ResourceStatus


@pytest.fixture()
def resource_client(http_client) -> OdpResourceClient:
    return OdpResourceClient(http_client=http_client, resource_endpoint="/foobar")


def test_get_resource_by_uuid(
    resource_client: OdpResourceClient,
    request_mock: responses.RequestsMock,
):
    kind = "test.hubocean.io/tesType"
    version = "v1alpha1"
    name = "test"
    uuid = uuid4()

    request_mock.add(
        responses.GET,
        f"{resource_client.resource_url}/{uuid}",
        body=ResourceDto(
            kind=kind,
            version=version,
            metadata=Metadata(name=name, uuid=uuid),
            status=ResourceStatus(
                num_updates=0,
                created_time=datetime.fromisoformat("2021-01-01T00:00:00+00:00"),
                created_by=uuid4(),
                updated_time=datetime.fromisoformat("2021-01-01T00:00:00+00:00"),
                updated_by=uuid4(),
            ),
            spec={},
        ).model_dump_json(),
        status=200,
        content_type="application/json",
    )

    manifest = resource_client.get(uuid)

    assert manifest.kind == kind
    assert manifest.version == version
    assert manifest.metadata.name == name


def test_get_resource_by_qname(
    resource_client: OdpResourceClient,
    request_mock: responses.RequestsMock,
):
    kind = "test.hubocean.io/tesType"
    version = "v1alpha1"
    name = "test"
    uuid = uuid4()

    request_mock.add(
        responses.GET,
        f"{resource_client.resource_url}/{kind}/{name}",
        body=ResourceDto(
            kind=kind,
            version=version,
            metadata=Metadata(name=name, uuid=uuid),
            status=ResourceStatus(
                num_updates=0,
                created_time=datetime.fromisoformat("2021-01-01T00:00:00+00:00"),
                created_by=uuid4(),
                updated_time=datetime.fromisoformat("2021-01-01T00:00:00+00:00"),
                updated_by=uuid4(),
            ),
            spec={},
        ).model_dump_json(),
        status=200,
        content_type="application/json",
    )

    manifest = resource_client.get(f"{kind}/{name}")

    assert manifest.kind == kind
    assert manifest.version == version
    assert manifest.metadata.name == name
    assert manifest.metadata.uuid == uuid
    assert manifest.metadata.uuid == uuid


def test_create_resource(
    resource_client: OdpResourceClient,
    request_mock: responses.RequestsMock,
):
    def _on_create_request(request):
        manifest = json.loads(request.body)

        # Ensure that the status and uuid is not set. If they are set, they must have a null-value
        assert manifest.get("status", None) is None
        assert manifest["metadata"].get("uuid", None) is None

        t = datetime.now().isoformat()
        created_by = str(UUID(int=0))
        manifest["metadata"]["uuid"] = str(uuid4())
        manifest["metadata"].setdefault("owner", created_by)
        manifest["status"] = {
            "num_updates": 0,
            "created_by": created_by,
            "created_time": t,
            "updated_by": created_by,
            "updated_time": t,
        }

        return (201, {}, json.dumps(manifest))

    resource_manifest = ResourceDto(
        kind="test.hubocean.io/testType",
        version="v1alpha1",
        metadata=Metadata(name="foobar"),
        spec=dict(),
    )

    request_mock.add_callback(
        responses.POST,
        f"{resource_client.resource_url}",
        callback=_on_create_request,
        content_type="application/json",
    )

    populated_manifest = resource_client.create(resource_manifest)

    assert isinstance(populated_manifest, ResourceDto)
    assert populated_manifest.metadata.uuid is not None
    assert populated_manifest.status is not None
    assert populated_manifest.status.num_updates == 0
    assert populated_manifest.kind == resource_manifest.kind
    assert populated_manifest.metadata.name == resource_manifest.metadata.name
