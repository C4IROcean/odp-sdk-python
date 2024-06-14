import pytest
import responses
from pandas import DataFrame

from odp_sdk.exc import OdpResourceExistsError, OdpResourceNotFoundError
from odp_sdk.tabular_storage_client import OdpTabularStorageClient


@pytest.fixture()
def tabular_storage_client(http_client) -> OdpTabularStorageClient:
    return OdpTabularStorageClient(http_client=http_client, tabular_storage_endpoint="/data")


def test_create_schema_success(tabular_storage_client, tabular_resource_dto, table_spec):
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            tabular_storage_client.tabular_endpoint(tabular_resource_dto, "schema"),
            body=table_spec.model_dump_json(),
            status=200,
            content_type="application/json",
        )

        result = tabular_storage_client.create_schema(tabular_resource_dto, table_spec)

        assert result.table_schema == table_spec.table_schema
        assert result.partitioning == table_spec.partitioning


def test_create_schema_fail_409(tabular_storage_client, tabular_resource_dto, table_spec):
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            tabular_storage_client.tabular_endpoint(tabular_resource_dto, "schema"),
            status=409,
        )

        with pytest.raises(OdpResourceExistsError):
            tabular_storage_client.create_schema(tabular_resource_dto, table_spec)


def test_get_schema_success(tabular_storage_client, tabular_resource_dto, table_spec):
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            tabular_storage_client.tabular_endpoint(tabular_resource_dto, "schema"),
            body=table_spec.model_dump_json(),
            status=200,
            content_type="application/json",
        )

        result = tabular_storage_client.get_schema(tabular_resource_dto)

        assert result.table_schema == table_spec.table_schema
        assert result.partitioning == table_spec.partitioning


def test_get_schema_fail_404(tabular_storage_client, tabular_resource_dto):
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            tabular_storage_client.tabular_endpoint(tabular_resource_dto, "schema"),
            status=404,
        )

        with pytest.raises(OdpResourceNotFoundError):
            tabular_storage_client.get_schema(tabular_resource_dto)


def test_delete_schema_success(tabular_storage_client, tabular_resource_dto):
    url = tabular_storage_client.tabular_endpoint(tabular_resource_dto, "schema") + "?delete_data=False"

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.DELETE,
            url,
            status=200,
        )

        tabular_storage_client.delete_schema(tabular_resource_dto)

        assert rsps.assert_call_count(url, 1)


def test_delete_schema_fail_404(tabular_storage_client, tabular_resource_dto):
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.DELETE,
            tabular_storage_client.tabular_endpoint(tabular_resource_dto, "schema"),
            status=404,
        )

        with pytest.raises(OdpResourceNotFoundError):
            tabular_storage_client.delete_schema(tabular_resource_dto)


def test_create_stage_success(tabular_storage_client, tabular_resource_dto, table_stage):
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            tabular_storage_client.tabular_endpoint(tabular_resource_dto, "stage"),
            body=table_stage.model_dump_json(),
            status=200,
            content_type="application/json",
        )

        result = tabular_storage_client.create_stage_request(tabular_resource_dto)

        assert result.stage_id == table_stage.stage_id


def test_create_stage_fail_409(tabular_storage_client, tabular_resource_dto):
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            tabular_storage_client.tabular_endpoint(tabular_resource_dto, "stage"),
            status=409,
        )

        with pytest.raises(OdpResourceExistsError):
            tabular_storage_client.create_stage_request(tabular_resource_dto)


def test_commit_stage_success(tabular_storage_client, tabular_resource_dto, table_stage):
    url = tabular_storage_client.tabular_endpoint(tabular_resource_dto, "stage")

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            url,
            status=200,
        )
        tabular_storage_client.commit_stage_request(tabular_resource_dto, table_stage)

        assert rsps.assert_call_count(url, 1)


def test_get_stage_success(tabular_storage_client, tabular_resource_dto, table_stage):
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            tabular_storage_client.tabular_endpoint(tabular_resource_dto, "stage", str(table_stage.stage_id)),
            body=table_stage.model_dump_json(),
            status=200,
            content_type="application/json",
        )
        rsps.add(
            responses.GET,
            tabular_storage_client.tabular_endpoint(tabular_resource_dto, "stage", str(table_stage.stage_id)),
            body=table_stage.model_dump_json(),
            status=200,
            content_type="application/json",
        )

        result_uuid = tabular_storage_client.get_stage_request(tabular_resource_dto, table_stage.stage_id)
        result_table_stage = tabular_storage_client.get_stage_request(tabular_resource_dto, table_stage)

        assert result_uuid.stage_id == table_stage.stage_id
        assert result_table_stage.stage_id == table_stage.stage_id


def test_get_stage_fail_400(tabular_storage_client, tabular_resource_dto, table_stage):
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            tabular_storage_client.tabular_endpoint(tabular_resource_dto, "stage", str(table_stage.stage_id)),
            status=400,
        )

        with pytest.raises(OdpResourceNotFoundError):
            tabular_storage_client.get_stage_request(tabular_resource_dto, table_stage.stage_id)


def test_list_stage_success(tabular_storage_client, tabular_resource_dto, table_stage):
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            tabular_storage_client.tabular_endpoint(tabular_resource_dto, "stage"),
            body=f"[{table_stage.model_dump_json()}, {table_stage.model_dump_json()}]",
            status=200,
            content_type="application/json",
        )

        result = tabular_storage_client.list_stage_request(tabular_resource_dto)

        assert len(result) == 2


def test_list_stage_fail_400(tabular_storage_client, tabular_resource_dto):
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            tabular_storage_client.tabular_endpoint(tabular_resource_dto, "stage"),
            status=400,
        )

        with pytest.raises(OdpResourceNotFoundError):
            tabular_storage_client.list_stage_request(tabular_resource_dto)


def test_delete_stage_success(tabular_storage_client, tabular_resource_dto, table_stage):
    url = (
        tabular_storage_client.tabular_endpoint(tabular_resource_dto, "stage", str(table_stage.stage_id))
        + "?force_delete=False"
    )

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.DELETE,
            url,
            status=200,
        )

        tabular_storage_client.delete_stage_request(tabular_resource_dto, table_stage)

        assert rsps.assert_call_count(url, 1)


def test_delete_stage_fail_400(tabular_storage_client, tabular_resource_dto, table_stage):
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.DELETE,
            tabular_storage_client.tabular_endpoint(tabular_resource_dto, "stage", str(table_stage.stage_id)),
            status=400,
        )

        with pytest.raises(OdpResourceNotFoundError):
            tabular_storage_client.delete_stage_request(tabular_resource_dto, table_stage)


def test_select_as_stream_success(tabular_storage_client, tabular_resource_dto):
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            tabular_storage_client.tabular_endpoint(tabular_resource_dto, "list"),
            body='{"test_key1": "test_value"}\n{"test_key2": "test_value2"}',
            status=200,
            content_type="application/x-ndjson",
        )

        response = tabular_storage_client.select_as_stream(tabular_resource_dto, filter_query=None)
        response_as_list = list(response)

        assert len(response_as_list) == 2
        assert response_as_list[0]["test_key1"] == "test_value"
        assert response_as_list[1]["test_key2"] == "test_value2"


def test_select_as_list_success(tabular_storage_client, tabular_resource_dto):
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            tabular_storage_client.tabular_endpoint(tabular_resource_dto, "list"),
            body='{"test_key1": "test_value"}\n{"test_key2": "test_value2"}\n{"@@end": true}',
            status=200,
            content_type="application/x-ndjson",
        )

        response = tabular_storage_client.select_as_list(tabular_resource_dto, filter_query=None)

        assert len(response) == 2
        assert response[0]["test_key1"] == "test_value"
        assert response[1]["test_key2"] == "test_value2"


def test_select_as_list_wkt_success(tabular_storage_client, tabular_resource_dto):
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            tabular_storage_client.tabular_endpoint(tabular_resource_dto, "list"),
            body='{"test_key1": "POINT(0 0)"}\n{"test_key2": "POINT(0 1)"}\n{"@@end": true}',
            status=200,
            content_type="application/x-ndjson",
        )

        response = tabular_storage_client.select_as_list(tabular_resource_dto, filter_query=None)

        assert len(response) == 2
        assert response[0]["test_key1"] == {"coordinates": [0.0, 0.0], "type": "Point"}
        assert response[1]["test_key2"] == {"coordinates": [0.0, 1.0], "type": "Point"}


def test_select_as_list_wkb_success(tabular_storage_client, tabular_resource_dto):
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            tabular_storage_client.tabular_endpoint(tabular_resource_dto, "list"),
            body='{"test_key1": "010100000000000000000000000000000000000000"}\n'
            '{"test_key2": "01010000000000000000000000000000000000f03f"}\n{"@@end": true}',
            status=200,
            content_type="application/x-ndjson",
        )

        response = tabular_storage_client.select_as_list(tabular_resource_dto, filter_query=None)

        assert len(response) == 2
        assert response[0]["test_key1"] == {"coordinates": [0.0, 0.0], "type": "Point"}
        assert response[1]["test_key2"] == {"coordinates": [0.0, 1.0], "type": "Point"}


def test_select_as_stream_fail_404(tabular_storage_client, tabular_resource_dto):
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            tabular_storage_client.tabular_endpoint(tabular_resource_dto, "list"),
            status=404,
        )

        with pytest.raises(OdpResourceNotFoundError):
            response = tabular_storage_client.select_as_stream(tabular_resource_dto, filter_query=None)
            list(response)


def test_select_as_list_fail_404(tabular_storage_client, tabular_resource_dto):
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            tabular_storage_client.tabular_endpoint(tabular_resource_dto, "list"),
            status=404,
        )

        with pytest.raises(OdpResourceNotFoundError):
            tabular_storage_client.select_as_list(tabular_resource_dto, filter_query=None)


def test_select_as_dataframe(tabular_storage_client, tabular_resource_dto):
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            tabular_storage_client.tabular_endpoint(tabular_resource_dto, "list"),
            body='{"test_key1": "test_value"}\n{"test_key2": "test_value2"}\n{"@@end": true}',
            status=200,
            content_type="application/x-ndjson",
        )

        response = tabular_storage_client.select_as_dataframe(tabular_resource_dto, filter_query=None)

        assert len(response) == 2
        assert response["test_key1"][0] == "test_value"
        assert response["test_key2"][1] == "test_value2"


def test_write_small_success(tabular_storage_client, tabular_resource_dto):
    url = tabular_storage_client.tabular_endpoint(tabular_resource_dto)

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            url,
            status=200,
        )

        data = [{"test_key1": "test_value"}, {"test_key2": "test_value2"}]

        tabular_storage_client.write(tabular_resource_dto, data, table_stage=None)

        assert rsps.assert_call_count(url, 1)


def test_write_fail_404(tabular_storage_client, tabular_resource_dto):
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            tabular_storage_client.tabular_endpoint(tabular_resource_dto),
            status=404,
        )

        data = [{"test_key1": "test_value"}, {"test_key2": "test_value2"}]

        with pytest.raises(OdpResourceNotFoundError):
            tabular_storage_client.write(tabular_resource_dto, data, table_stage=None)


def test_write_as_dataframe(tabular_storage_client, tabular_resource_dto):
    url = tabular_storage_client.tabular_endpoint(tabular_resource_dto)

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            url,
            status=200,
        )

        data = DataFrame([{"test_key1": "test_value"}, {"test_key2": "test_value2"}])

        tabular_storage_client.write_dataframe(tabular_resource_dto, data, table_stage=None)

        assert rsps.assert_call_count(url, 1)


def test_delete_success(tabular_storage_client, tabular_resource_dto):
    url = tabular_storage_client.tabular_endpoint(tabular_resource_dto, "delete")

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            url,
            status=200,
        )

        tabular_storage_client.delete(tabular_resource_dto, filter_query=None)

        assert rsps.assert_call_count(url, 1)


def test_delete_fail_400(tabular_storage_client, tabular_resource_dto):
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            tabular_storage_client.tabular_endpoint(tabular_resource_dto, "delete"),
            status=404,
        )

        with pytest.raises(OdpResourceNotFoundError):
            tabular_storage_client.delete(tabular_resource_dto, filter_query=None)


def test_update_success(tabular_storage_client, tabular_resource_dto):
    url = tabular_storage_client.tabular_endpoint(tabular_resource_dto)

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.PATCH,
            url,
            status=200,
        )

        data = [{"test_key1": "test_value"}, {"test_key2": "test_value2"}]

        tabular_storage_client.update(tabular_resource_dto, filter_query=None, data=data)

        assert rsps.assert_call_count(url, 1)


def test_update_fail_404(tabular_storage_client, tabular_resource_dto):
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.PATCH,
            tabular_storage_client.tabular_endpoint(tabular_resource_dto),
            status=404,
        )

        data = [{"test_key1": "test_value"}, {"test_key2": "test_value2"}]

        with pytest.raises(OdpResourceNotFoundError):
            tabular_storage_client.update(tabular_resource_dto, filter_query=None, data=data)


def test_update_dataframe_success(tabular_storage_client, tabular_resource_dto):
    url = tabular_storage_client.tabular_endpoint(tabular_resource_dto)

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.PATCH,
            url,
            status=200,
        )

        data = DataFrame([{"test_key1": "test_value"}, {"test_key2": "test_value2"}])

        tabular_storage_client.update_dataframe(tabular_resource_dto, filter_query=None, data=data)

        assert rsps.assert_call_count(url, 1)
