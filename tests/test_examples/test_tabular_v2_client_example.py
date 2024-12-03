import random
import string
from typing import Tuple
from uuid import UUID

import pyarrow as pa
from odp.client import OdpClient
from odp.client.exc import OdpResourceNotFoundError
from odp.client.tabular_v2.util import exp
from odp.dto import DatasetDto, DatasetSpec


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

    table = odp_client_test_uuid[0].table_v2(my_dataset)

    table_schema = pa.schema({"Data": pa.string()})
    table.create(table_schema)

    assert table.schema() is not None

    test_data = [{"Data": "Test"}, {"Data": "Test1"}]
    with table as tx:
        tx.insert(test_data)

    our_data = list(table.select().rows())
    assert len(our_data) == 2

    our_data = list(table.select().batches())
    assert len(our_data) == 1
    assert our_data[0].num_rows == 2

    update_filters = exp.parse("Data == 'Test'")
    new_data = [{"Data": "Test Updated"}]
    with table as tx:
        tx.delete(update_filters)
        tx.insert(new_data)

    result = list(table.select().rows())
    assert new_data[0] in result
    assert len(result) == 2

    delete_filters = exp.parse("Data == 'Test1'")
    with table as tx:
        tx.delete(delete_filters)
    result = list(table.select().rows())
    assert len(result) == 1

    table.drop()

    try:
        table.select()
    except OdpResourceNotFoundError as e:
        print("Schema not found error since it is deleted")
        print(e)

    odp_client_test_uuid[0].catalog.delete(my_dataset)
