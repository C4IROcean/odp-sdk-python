import random
import string
from typing import Tuple
from uuid import UUID

from odp.dto import DatasetDto, DatasetSpec
from odp_sdk.client import OdpClient
from odp_sdk.dto.table_spec import TableSpec
from odp_sdk.exc import OdpResourceNotFoundError


def test_tabular_client(odp_client_test_uuid: Tuple[OdpClient, UUID]):
    my_dataset = DatasetDto(
        **{
            "kind": "catalog.hubocean.io/dataset",
            "version": "v1alpha3",
            "metadata": {
                "name": "".join(random.choices(string.ascii_lowercase + string.digits, k=20)),
                "labels": {"test_uuid": odp_client_test_uuid[1]},
            },
            "spec": {
                "storage_controller": "registry.hubocean.io/storageController/storage-tabular",
                "storage_class": "registry.hubocean.io/storageClass/tabular",
                "maintainer": {"contact": "Just Me <raw_client_example@hubocean.earth>"},
            },
        }
    )

    my_dataset = odp_client_test_uuid[0].catalog.create(my_dataset)
    assert isinstance(my_dataset.spec, DatasetSpec)

    table_schema = {"Data": {"type": "string"}}
    my_table_spec = TableSpec(table_schema=table_schema)

    mt_table_spec = odp_client_test_uuid[0].tabular.create_schema(resource_dto=my_dataset, table_spec=my_table_spec)
    assert isinstance(mt_table_spec, TableSpec)

    test_data = [{"Data": "Test"}, {"Data": "Test1"}]
    odp_client_test_uuid[0].tabular.write(resource_dto=my_dataset, data=test_data)

    our_data = odp_client_test_uuid[0].tabular.select_as_list(my_dataset)
    assert len(our_data) == 2

    our_data = list(odp_client_test_uuid[0].tabular.select_as_stream(my_dataset))
    assert len(our_data) == 2

    update_filters = {"#EQUALS": ["$Data", "Test"]}
    new_data = [{"Data": "Test Updated"}]
    odp_client_test_uuid[0].tabular.update(
        resource_dto=my_dataset,
        data=new_data,
        filter_query=update_filters,
    )

    result = odp_client_test_uuid[0].tabular.select_as_list(my_dataset)
    assert len(result) == 2

    delete_filters = {"#EQUALS": ["$Data", "Test1"]}
    odp_client_test_uuid[0].tabular.delete(resource_dto=my_dataset, filter_query=delete_filters)
    result = odp_client_test_uuid[0].tabular.select_as_list(my_dataset)
    assert len(result) == 1

    odp_client_test_uuid[0].tabular.delete_schema(my_dataset)

    try:
        odp_client_test_uuid[0].tabular.get_schema(my_dataset)
    except OdpResourceNotFoundError as e:
        print("Schema not found error since it is deleted:")
        print(e)

    odp_client_test_uuid[0].catalog.delete(my_dataset)
