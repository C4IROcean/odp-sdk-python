import io
import logging
from typing import TYPE_CHECKING, Iterator, List, Optional, Union

import pyarrow as pa
from odp.client.tabular_v2.big import big
from odp.client.tabular_v2.big.remote import RemoteBigCol
from odp.client.tabular_v2.bsquare import bsquare

# from odp.ktable.client import Cursor
from odp.client.tabular_v2.client import Client
from odp.client.tabular_v2.client.table_cursor import CursorException
from odp.client.tabular_v2.util import exp

if TYPE_CHECKING:
    from odp.client.tabular_v2.client import Cursor


class TableHandler:
    def __init__(self, client: Client, table_id: str):
        self._id = table_id
        self._client = client
        self._inner_schema: Optional[pa.Schema] = None
        self._outer_schema: Optional[pa.Schema] = None
        self._tx = None
        try:
            self._fetch_schema()
        except FileNotFoundError:
            pass  # ktable does not exist yet

        self._bigcol = RemoteBigCol(
            self._bigcol_upload,
            self._bigcol_download,
            "/tmp/cache_big/",  # FIXME
        )

    def _fetch_schema(self):
        if self._inner_schema:
            return
        empty = list(self._select(inner_query=exp.parse('"fetch" == "schema"')))
        assert len(empty) == 1
        assert empty[0].num_rows == 0
        self._inner_schema = empty[0].schema
        self._outer_schema = bsquare.convert_schema_outward(self._inner_schema)

    def drop(self):
        try:
            res = self._client._request(
                path="/api/table/v2/drop",
                params={
                    "table_id": self._id,
                },
            ).json()
            logging.info("dropped %s: %s", self._id, res)
            return res
        except FileNotFoundError:
            logging.info("table %s does not exist", self._id)

    def create(self, schema: pa.Schema):
        self._outer_schema = schema
        self._inner_schema = bsquare.convert_schema_inward(schema)
        buf = io.BytesIO()
        w = pa.ipc.RecordBatchStreamWriter(buf, self._inner_schema)
        w.write_batch(pa.RecordBatch.from_pylist([], schema=self._inner_schema))
        w.close()

        self._client._request(
            path="/api/table/v2/create",
            params={
                "table_id": self._id,
            },
            data=buf.getvalue(),
        ).json()

    def _bigcol_upload(self, bid: str, data: bytes):
        self._client._request(
            path="/api/table/v2/big_upload",
            params={"table_id": self._id, "big_id": bid},
            data=data,
        )

    def _bigcol_download(self, big_id: str) -> bytes:
        return self._client._request(
            path="/api/table/v2/big_download",
            params={"table_id": self._id, "big_id": big_id},
        ).all()

    def select(
        self,
        query: Union[exp.Op, str, None] = None,
        cols: Optional[List[str]] = None,
    ) -> "Cursor":
        from odp.client.tabular_v2.client import Cursor

        if isinstance(query, str):
            query = exp.parse(query)
        if query:
            if cols:
                logging.info("cols: %s", cols)
                inner_cols = set(cols)  # add filtering columns
                for op in query.all():
                    if isinstance(op, exp.Field):
                        inner_cols.add(str(op))
                inner_cols = list(inner_cols)
                logging.info("inner_cols: %s", inner_cols)
            else:
                inner_cols = None

            logging.info("outer query: %s", query)
            inner_query = bsquare.convert_query(self._outer_schema, query)
            logging.info("bsquare query: %s", inner_query)
            inner_query = big.inner_exp(self._inner_schema, inner_query)
            logging.info("bigcol query: %s", inner_query)
            e = query.pyarrow()
        else:
            inner_cols = cols
            inner_query = None
            e = None

        def scanner(scanner_cursor: str) -> Iterator[pa.RecordBatch]:
            logging.info("selecting with cursor %s", scanner_cursor)
            for b in self._select("", inner_query, inner_cols, scanner_cursor):
                logging.info("got %d rows, decoding...", b.num_rows)

                # repackage into small batches because bigcol can use lots of memory
                tab = pa.Table.from_batches([b], schema=b.schema)
                del b
                for b in tab.to_batches(max_chunksize=2_000):
                    b = self._bigcol.decode(b)  # convert to panda first, then do the magic
                    b = bsquare.decode(b)
                    tab = pa.Table.from_batches([b], schema=b.schema)
                    if e is not None:  # drop false positives
                        before = tab.num_rows
                        logging.debug("filtering before %d rows...", tab.num_rows)
                        tab = tab.filter(e)
                        logging.info("filtered %d rows to %d", before, tab.num_rows)
                    if cols:
                        tab = tab.select(cols)  # drop the cols used for filtering
                    for x in tab.to_batches():
                        yield x

        return Cursor(scanner=scanner)

    def __enter__(self):
        from odp.client.tabular_v2.client import Transaction

        if self._inner_schema is None:
            raise FileNotFoundError("table does not exist, create() it first")

        if self._tx:
            raise ValueError("already in a transaction")

        res = self._client._request(
            path="/api/table/v2/begin",
            params={
                "table_id": self._id,
            },
        ).json()

        self._tx = Transaction(self, res["tx_id"])
        return self._tx

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            logging.warning("aborting transaction %s", self._tx._id)
            try:
                self._client._request(
                    path="/api/table/v2/rollback",
                    params={
                        "table_id": self._id,
                        "tx_id": self._tx._id,
                    },
                )
            except Exception as e:
                logging.error("ignored: rollback failed: %s", e)
            return False
        self._tx.flush()
        self._tx._big_buf.flush()  # flush the bigcol buffer
        self._client._request(
            path="/api/table/v2/commit",
            params={
                "table_id": self._id,
                "tx_id": self._tx._id,
            },
        )
        self._tx = None

    def schema(self) -> pa.Schema:
        return self._outer_schema

    @property
    def name(self) -> str:
        return self._id

    # used as a filter in Cursor, encode in tx
    def _decode(self, b: pa.RecordBatch) -> pa.RecordBatch:
        b = self._bigcol.decode(b)  # convert to panda first, then do the magic
        b = bsquare.decode(b)
        return b

    def _select(
        self,
        tid: str = "",
        inner_query: Optional[exp.Op] = None,
        cols: Optional[List[str]] = None,
        cursor: str = "",
    ) -> Iterator[pa.RecordBatch]:
        """
        internal: select data with optional transaction id, only expand bigcol refs.
        further filtering is done by the caller
        """
        res = self._client._request(
            path="/api/table/v2/select",
            params={
                "table_id": self._id,
                "tx_id": tid,
            },
            data={
                "query": str(inner_query) if inner_query else None,
                "cols": cols,
                "cursor": cursor,
            },
        )
        r = pa.ipc.RecordBatchStreamReader(res.reader())
        for bm in r.iter_batches_with_custom_metadata():
            if bm.custom_metadata:
                if b"error" in bm.custom_metadata:
                    raise Exception("server error: %s" % bm.custom_metadata[b"error"].decode())
                elif b"cursor" in bm.custom_metadata:
                    raise CursorException(bm.custom_metadata[b"cursor"].decode())
                else:
                    logging.warning("ignoring custom metadata: %s", bm.custom_metadata)

            yield bm.batch

    def _insert_batch(
        self,
        data: pa.RecordBatch,
        tx: str = "",
    ):
        """
        internal: insert data with optional transaction id
        """
        assert self._inner_schema
        buf = io.BytesIO()
        w = pa.ipc.RecordBatchStreamWriter(buf, self._inner_schema)
        w.write_batch(data)
        w.close()

        self._client._request(
            path="/api/table/v2/insert",
            params={
                "table_id": self._id,
                "tx_id": tx,
            },
            data=buf.getvalue(),
        ).json()
