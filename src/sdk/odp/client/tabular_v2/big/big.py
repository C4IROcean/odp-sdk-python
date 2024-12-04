import json
import logging
from abc import abstractmethod
from typing import Iterable, Optional

import pyarrow as pa
from odp.client.tabular_v2.util.exp import BinOp, Field, Op, Parens, Scalar, UnaryOp

STR_LIMIT = 128  # when to start using a reference
STR_MIN = 12  # what to keep as prefix in the reference
MAX_BIGFILE_SIZE = 64 * 1024 * 1024  # max size of a big file


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
        cache = {}

        def decode(x):
            if x is None:
                return None
            if isinstance(x, str):
                prefix, ref = x.rsplit("~", 1)
            else:
                prefix, ref = x.rsplit(b"~", 1)
                ref = ref.decode("utf-8")
            if ref == "":
                return prefix
            big_id, start, size = ref.split(":")
            start = int(start)
            size = int(size)
            if big_id in cache:
                data = cache[big_id]
            else:
                data = self.fetch(big_id)
                cache[big_id] = data
            if isinstance(x, str):
                return data[start : start + size].decode("utf-8")
            else:
                return data[start : start + size]

        fields = _fields_from_schema(batch.schema)
        df = batch.to_pandas()
        df[fields] = df[fields].map(decode)
        return pa.RecordBatch.from_pandas(df, schema=batch.schema)


def _fields_from_schema(schema: pa.Schema) -> list[str]:
    fields = []
    for name in schema.names:
        field: pa.Field = schema.field(name)
        if field.type == pa.string():
            fields.append(name)
        if field.type == pa.binary():
            fields.append(name)
    return fields


def inner_exp(schema: pa.Schema, op: Optional[Op]) -> Optional[Op]:
    if op is None:
        return None

    fields = _fields_from_schema(schema)

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
            right=Scalar.from_py(right + "~"),
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
