import logging
from typing import Optional

import pyarrow as pa
import shapely
from odp.client.tabular_v2.bsquare.query import _QueryContext
from odp.client.tabular_v2.util.exp import Op


def convert_schema_inward(outer_schema: pa.Schema) -> pa.Schema:
    out = []
    for name in outer_schema.names:
        f = outer_schema.field(name)
        if f.metadata and b"isGeometry" in f.metadata:
            meta = f.metadata
            if b"index" in meta:
                new_meta = meta.copy()
                del new_meta[b"index"]
                f = f.with_metadata(new_meta)
            out.append(f)
            out.append(pa.field(name + ".x", pa.float64(), True, metadata=meta))
            out.append(pa.field(name + ".y", pa.float64(), True, metadata=meta))
            out.append(pa.field(name + ".q", pa.float64(), True, metadata=meta))
        else:
            out.append(f)
    return pa.schema(out)


# convert the inner_schema to outer_schema
def convert_schema_outward(inner_schema: pa.Schema) -> pa.Schema:
    geo_indexes = set()

    def is_subfield(schema: pa.Schema, f: pa.Field) -> bool:
        if "." not in f.name:
            return False
        left, right = f.name.rsplit(".", 1)
        if left not in schema.names:
            return False
        if schema.field(left).metadata and b"isGeometry" not in schema.field(left).metadata:
            return False
        if f.metadata and b"index" in f.metadata:
            geo_indexes.add(left)
        return True

    # create a new schema with only the fields that are not subfields
    fields = []
    for names in inner_schema.names:
        f = inner_schema.field(names)
        if not is_subfield(inner_schema, f):
            fields.append(f)

    # add back the "index" to the main field (which was removed when creating the subfields)
    for i, f in enumerate(fields):
        if f.name in geo_indexes:
            meta = f.metadata
            meta[b"index"] = b"1"
            fields[i] = f.with_metadata(meta)
    return pa.schema(fields)


# convert outer query to inner query using bsquare in .x, .y and .q
def convert_query(outer_schema: pa.Schema, outer_query: Optional[Op]) -> Optional[Op]:
    if outer_query is None:
        return None

    geo_fields = []
    for f in outer_schema:
        if f.metadata and b"isGeometry" in f.metadata:
            geo_fields.append(f.name)

    return _QueryContext(geo_fields).convert(outer_query)


def decode(b: pa.RecordBatch) -> pa.RecordBatch:
    outer_schema = convert_schema_outward(b.schema)
    if b.num_rows == 0:
        return pa.RecordBatch.from_pylist([], schema=outer_schema)
    list = pa.Table.from_batches([b], schema=b.schema).select(outer_schema.names).to_batches()
    if len(list) != 1:
        raise ValueError("expected exactly one batch")
    return list[0]


def encode(b: pa.RecordBatch) -> pa.RecordBatch:
    logging.info("bsquare encoding %d rows", b.num_rows)
    inner_schema = convert_schema_inward(b.schema)
    geo_names = []
    for name in b.schema.names:
        f = b.schema.field(name)
        if f.metadata and b"isGeometry" in f.metadata:
            geo_names.append(name)

    # we encode rows by rows to made it simple to create multiple columns
    def _encode(row):
        for name in geo_names:
            if name in row and row[name] is not None:
                val = row[name]
                if isinstance(val, str):
                    val = shapely.from_wkt(val)
                elif isinstance(val, bytes):
                    val = shapely.from_wkb(val)
                else:
                    raise ValueError(f"Unsupported type: {type(val)}")
                min_x, min_y, max_x, max_y = val.bounds
                row[name + ".x"] = (min_x + max_x) / 2
                row[name + ".y"] = (min_y + max_y) / 2
                row[name + ".q"] = max(max_x - min_x, max_y - min_y) / 2
            else:
                row[name + ".x"] = None
                row[name + ".y"] = None
                row[name + ".q"] = None
        return row

    d = b.to_pandas()
    for geo_name in geo_names:
        d[geo_name + ".x"] = None
        d[geo_name + ".y"] = None
        d[geo_name + ".q"] = None
    d = d.apply(func=_encode, axis=1)
    return pa.RecordBatch.from_pandas(d, schema=inner_schema)


class BSquare:
    geometry_fields = ["{col_name}.x", "{col_name}.y", "{col_name}.q"]  # add complexity and confuse the user

    def __init__(self, inner_schema: Optional[pa.Schema] = None):
        assert not "good"
        self._inner_schema = inner_schema
        self._geo_fields = []  # FIXME do this earlier, then cash on it
