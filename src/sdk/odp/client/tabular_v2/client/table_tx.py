import io
import logging
from typing import Dict, Iterator, List, Union

import pyarrow as pa
from odp.client.tabular_v2 import big
from odp.client.tabular_v2.bsquare import bsquare
from odp.client.tabular_v2.client.table_cursor import ScannerCursorException
from odp.client.tabular_v2.client.tablehandler import TableHandler
from odp.client.tabular_v2.util import exp
from odp.client.tabular_v2.util.reader import Iter2Reader


class Transaction:
    def __init__(self, table: TableHandler, tx_id: str):
        if not tx_id:
            raise ValueError("tx_id must not be empty")
        self._table = table
        self._id = tx_id
        self._buf: list[pa.RecordBatch] = []
        self._buf_rows = 0
        self._big_buf: big.Buffer = big.Buffer(table._bigcol, table._inner_schema)
        self._old_rid = None

    def select(self, query: Union[exp.Op, str, None] = None) -> Iterator[dict]:
        for row in self._table.select(query).rows():
            yield row

    def replace(self, query: Union[exp.Op, str, None] = None) -> Iterator[dict]:
        """perform a two-step replace:
        rows that don't match the query are kept.
        rows that match are removed and sent to the caller.
        the caller might insert them again or do something else.
        """
        if query is None:
            raise ValueError("For your own safety, please provide a query like 1==1")
        assert self._buf_rows == 0  # FIXME: handle buffered data in replace/select
        if isinstance(query, str):
            query = exp.parse(query)
        inner_query = bsquare.convert_query(self._table._outer_schema, query)
        inner_query = big.inner_exp(self._table._inner_schema, inner_query)
        inner_query = str(inner_query.pyarrow())

        def scanner(cursor: str) -> Iterator[pa.RecordBatch]:
            res = self._table._client._request(
                path="/api/table/v2/replace",
                params={
                    "table_id": self._table._id,
                    "tx_id": self._id,
                },
                data={
                    "query": inner_query,
                    "cursor": cursor,
                },
            )
            r = Iter2Reader(res.iter())
            r = pa.ipc.RecordBatchStreamReader(r)
            for bm in r.iter_batches_with_custom_metadata():
                if bm.custom_metadata:
                    meta = bm.custom_metadata
                    if b"cursor" in meta:
                        raise ScannerCursorException(meta[b"cursor"].decode())
                    if b"error" in meta:
                        raise ValueError("remote: " + meta[b"error"].decode())
                if bm.batch:
                    yield bm.batch

        from odp.client.tabular_v2.client import Cursor

        for b in Cursor(scanner=scanner).batches():
            b = self._table._bigcol.decode(b)  # TODO(oha): use buffer for partial big files not uploaded
            b = bsquare.decode(b)
            tab = pa.Table.from_batches([b], schema=self._table._outer_schema)
            for b2 in tab.filter(~query.pyarrow()).to_batches():
                if b2.num_rows > 0:
                    self.insert(b2)

            for b2 in tab.filter(query.pyarrow()).to_batches():
                for row in b2.to_pylist():
                    yield row

    def delete(self, query: Union[exp.Op, str, None] = None) -> int:
        ct = 0
        for _ in self.replace(query):
            ct += 1
        return ct

    def flush(self):
        logging.info("flushing to stage %s", self._id)
        if len(self._buf) == 0:
            return
        buf = io.BytesIO()
        w = pa.ipc.RecordBatchStreamWriter(buf, self._table._inner_schema)
        for b in self._buf:
            b = bsquare.encode(b)
            b = self._big_buf.encode(b)
            w.write_batch(b)
        w.close()
        self._table._client._request(
            path="/api/table/v2/insert",
            params={
                "table_id": self._table._id,
                "tx_id": self._id,
            },
            data=buf.getvalue(),
        ).json()
        self._buf = []
        self._buf_rows = 0

    def insert(self, data: Union[Dict, List[Dict], pa.RecordBatch]):
        """queue data to be inserted on flush()"""
        if isinstance(data, dict):
            data = [data]
        if isinstance(data, list):
            data = pa.RecordBatch.from_pylist(data, schema=self._table._outer_schema)

        if isinstance(data, pa.RecordBatch):
            # TODO bigcol + bsquare
            self._buf.append(data)
            self._buf_rows += data.num_rows
        else:
            raise ValueError(f"unexpected type {type(data)}")

        if self._buf_rows > 5_000:
            self.flush()
