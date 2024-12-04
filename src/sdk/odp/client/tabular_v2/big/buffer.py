import uuid
from threading import Lock
from typing import Union

import pandas as pd
import pyarrow as pa
from odp.client.tabular_v2.big.big import MAX_BIGFILE_SIZE, STR_LIMIT, STR_MIN, BigCol, _fields_from_schema


class Buffer:
    def __init__(self, parent: BigCol, schema: pa.Schema):
        self.data = []
        self.size = 0
        self.next_id = uuid.uuid4().hex
        self.lock = Lock()
        self.parent = parent
        self.fields = _fields_from_schema(schema)  # NOTE(oha) we can precompute this because ingestion is schema-bound

    def encode(self, batch: pa.RecordBatch):
        # TODO: avoid pandas?
        df: pd.DataFrame = batch.to_pandas()
        df[self.fields] = df[self.fields].map(self.append)
        return pa.RecordBatch.from_pandas(df, schema=batch.schema)

    def append(self, data: Union[str, bytes, None]) -> Union[str, bytes, None]:
        if data is None:
            return None
        if len(data) < STR_LIMIT:
            if isinstance(data, str):
                return data + "~"
            else:
                return data + b"~"

        prefix = data[0:STR_MIN]  # prefix need to original type to avoid partial utf-8 characters
        if isinstance(data, str):
            data = data.encode("utf-8")  # we need to count bytes, not characters
        size = len(data)

        with self.lock:
            ref = f"~{self.next_id}:{self.size}:{size}"  # noqa # ref to the current position
            self.data.append(data)  # append the new data to the buffer
            self.size += size  # update the size of the buffer
            if self.size > MAX_BIGFILE_SIZE:  # too much data? flush
                self.parent.upload(self.next_id, self.data)
                self.next_id = uuid.uuid4().hex

        # return the reference with the right type
        if isinstance(prefix, str):
            return prefix + ref
        else:
            return prefix + ref.encode("utf-8")

    def flush(self):
        with self.lock:
            if self.size > 0:
                self.parent.upload(self.next_id, self.data)
                self.data = []
                self.size = 0
                self.next_id = uuid.uuid4().hex
