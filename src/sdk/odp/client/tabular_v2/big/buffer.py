import uuid
from threading import Lock
from typing import Optional

import pandas as pd
import pyarrow as pa
from odp.client.tabular_v2.big import BigCol
from odp.client.tabular_v2.big.big import MAX_BIGFILE_SIZE, SMALL_MAX, STR_LIMIT, STR_MIN


def convert_schema_inward(schema: pa.Schema) -> pa.Schema:
    """add .ref fields for columns marked with big, helper only used by create()"""
    return Buffer(None).with_outer_schema(schema).inner_schema


class Buffer:
    def __init__(self, parent: Optional[BigCol]):
        self.data = []
        self.size = 0
        self.next_id = uuid.uuid4().hex
        self.lock = Lock()
        self.parent = parent
        self.big_fields = []
        self.small_fields = []
        self.inner_schema = None

    def with_inner_schema(self, inner_schema: pa.Schema):
        self.inner_schema = inner_schema
        for name in inner_schema.names:
            if name.endswith(".ref"):
                continue
            f = inner_schema.field(name)
            if f.type != pa.string() and f.type != pa.binary():
                continue
            meta = f.metadata
            if meta and b"big" in meta:
                self.big_fields.append(name)
            else:
                self.small_fields.append(name)
        return self

    def with_outer_schema(self, outer_schema: pa.Schema) -> "Buffer":
        fields = []
        for name in outer_schema.names:
            field: pa.Field = outer_schema.field(name)
            fields.append(field)
            if field.type != pa.string() and field.type != pa.binary():
                continue
            meta = field.metadata
            if meta and b"big" in meta:
                fields.append(pa.field(name + ".ref", pa.string()))
                self.big_fields.append(name)
            else:
                self.small_fields.append(name)
        self.inner_schema = pa.schema(fields)
        return self

    def encode(self, batch: pa.RecordBatch):
        # TODO: avoid pandas?
        df: pd.DataFrame = batch.to_pandas()
        out = df.apply(self.append, axis=1)
        return pa.RecordBatch.from_pandas(out, schema=self.inner_schema)

    def append(self, row):
        for name in self.small_fields:
            data = row[name]
            if data is None:
                continue
            if len(data) > SMALL_MAX:
                raise ValueError(f"field {name} is too long: “{data}”")

        for name in self.big_fields:
            row[name + ".ref"] = None
            data = row[name]
            if data is None:
                continue
            if isinstance(data, str):
                data = data.encode("utf-8")  # convert to bytes
            size = len(data)  # size in bytes
            if size < STR_LIMIT:
                continue
            with self.lock:
                ref = f"{self.next_id}:{self.size}:{size}"  # noqa # ref to the current position
                self.data.append(data)  # append the new data to the buffer
                self.size += size  # update the size of the buffer
                if self.size > MAX_BIGFILE_SIZE:  # too much data? flush
                    self.parent.upload(self.next_id, self.data)
                    self.next_id = uuid.uuid4().hex
            row[name + ".ref"] = ref
            row[name] = row[name][0:STR_MIN]
        return row

    def flush(self):
        with self.lock:
            if self.size > 0:
                self.parent.upload(self.next_id, self.data)
                self.data = []
                self.size = 0
                self.next_id = uuid.uuid4().hex
