import json
import logging
from abc import abstractmethod
from typing import Iterable, Optional

import pyarrow as pa
from odp.client.tabular_v2.util.exp import BinOp, Field, Op, Parens, Scalar, UnaryOp

SMALL_MAX = 256
STR_LIMIT = 128  # when to start using a reference
STR_MIN = 12  # what to keep as prefix in the reference
MAX_BIGFILE_SIZE = 64 * 1024 * 1024  # max size of a big file


def convert_schema_outward(schema: pa.Schema) -> pa.Schema:
    """drops the .ref fields"""
    out = []
    for name in schema.names:
        field: pa.Field = schema.field(name)
        if name.endswith(".ref") and name[:-4] in schema.names and field.type == pa.string():
            continue  # skip
        out.append(field)
    return pa.schema(out)


class BigCol:
    def __init__(self):
        pass

    @abstractmethod
    def fetch(self, md5: str) -> bytes:
        """fetch data, called often, should cache"""
        raise NotImplementedError()

    @abstractmethod
    def upload(self, md5: str, data: Iterable[bytes]):
        """upload data"""
        raise NotImplementedError()

    def decode(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        cache = {}  # FIXME: can this use too much memory?
        outer_schema = convert_schema_outward(batch.schema)

        refs = []
        for name in outer_schema.names:
            if name.endswith(".ref"):
                refs.append(name)

        if not refs:
            return batch.select(outer_schema.names)

        def decode_by_row(row):
            for name in refs:
                ref = row[name]
                if not ref:
                    continue

                target = name[:-4]
                big_id, start, size = ref.split(":")
                start = int(start)
                size = int(size)
                if big_id in cache:
                    data = cache[big_id]
                else:
                    data = self.fetch(big_id)
                    cache[big_id] = data
                if isinstance(row[name], str):  # the field must contain the prefix, from which we infer the type
                    row[target] = data[start : start + size].decode("utf-8")
                else:
                    row[target] = data[start : start + size]
            return row

        df = batch.to_pandas()
        df = df.apply(decode_by_row, axis=1)
        return pa.RecordBatch.from_pandas(df, schema=outer_schema)


def inner_exp(schema: pa.Schema, op: Optional[Op]) -> Optional[Op]:
    if op is None:
        return None

    fields = []
    for name in schema.names:
        field: pa.Field = schema.field(name)
        if field.type != pa.string() and field.type != pa.binary():
            continue
        if field.metadata and b"big" in field.metadata:
            fields.append(name)

    # TODO don't use the visitor, instead parse manually and use negation context
    def visitor(neg: bool, op: Op) -> Op:
        if isinstance(op, Field):
            return op
        if isinstance(op, Scalar):
            return op
        if isinstance(op, Parens):
            op.exp = visitor(neg, op.exp)
            return op
        if isinstance(op, UnaryOp):
            if op.prefix in ["~", "not", "!", "invert"]:
                return UnaryOp(prefix=op.prefix, exp=visitor(~neg, op.exp), suffix=op.suffix)
            return op
        if isinstance(op, BinOp):
            op = BinOp(left=visitor(neg, op.left), op=op.op, right=visitor(neg, op.right))
            if isinstance(op.left, Field):
                if str(op.left) in fields:
                    return _inner_exp_binop(neg, op.left, op.op, op.right)
                return op
            elif isinstance(op.right, Field):
                try:
                    op = op.flip()
                except NotImplementedError:
                    logging.warning("can't flip big-col expression: %s", op)
                    return Scalar(src="True", type="bool")
                return visitor(neg, op)
            else:
                return op
        raise ValueError(f"can't convert big-col expression: {type(op)}")

    op = visitor(False, op)
    logging.info("big: inner_exp: %s", repr(op))
    return op


def _inner_exp_binop_str(neg: bool, field: Field, op: str, right: str) -> Op:
    if len(right) > STR_MIN:
        a = right[:STR_MIN]
        b = right[: STR_MIN - 1] + chr(ord(right[STR_MIN - 1]) + 1)
        logging.info("big: str: %s .. %s", json.dumps(a), json.dumps(b))

        if op == "==":
            if neg:
                return Scalar.from_py(False)
            return BinOp(
                left=BinOp(
                    left=Scalar.from_py(a),
                    op="<",
                    right=field,
                ),
                op="and",
                right=BinOp(
                    left=field,
                    op="<",
                    right=Scalar.from_py(b),
                ),
            )
        elif op == "!=":
            if neg:
                return Scalar.from_py(False)
            else:
                return Scalar.from_py(True)
        elif op == ">" or op == ">=":
            return BinOp(
                left=field,
                op=op,
                right=Scalar.from_py(a),
            )
        elif op == "<" or op == "<=":
            return BinOp(
                left=field,
                op=op,
                right=Scalar.from_py(b),
            )
    else:
        return BinOp(
            left=field,
            op=op,
            right=Scalar.from_py(right),
        )
    logging.error("can't convert big-col expression: %s %s %s", field, op, right)
    raise ValueError("can't convert big-col expression")


def _inner_exp_binop(neg: bool, left: Field, op: str, right: Op) -> Op:
    if isinstance(right, Scalar):
        v = right.to_py()
        if isinstance(v, str):
            return _inner_exp_binop_str(neg, left, op, v)
        else:
            raise ValueError("can't convert big-col expression for scalar %s", right)
    raise ValueError("can't convert big-col expression: %s %s %s", left, op, right)
