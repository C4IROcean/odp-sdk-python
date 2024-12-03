import copy
import logging

import shapely
from odp.client.tabular_v2.util import exp


class _QueryContext:
    def __init__(self, geo_fields: list[str]):
        self.geo_fields = geo_fields
        self.negate = False

    def neg(self) -> "_QueryContext":
        c = copy.copy(self)
        c.negate = ~c.negate
        return c

    def is_geo_field(self, op: exp.Op | None) -> bool:
        if isinstance(op, exp.Field):
            return op.name in self.geo_fields
        return False

    def convert(self, op: exp.Op | None) -> exp.Op | None:
        if op is None:
            return None
        if isinstance(op, exp.Parens):
            inner = self.convert(op.exp)
            if isinstance(inner, exp.Parens):
                return inner
            return exp.Parens(inner)
        if isinstance(op, exp.BinOp):
            if self.is_geo_field(op.left) or self.is_geo_field(op.right):
                if op.op in ["intersects", "contains", "within", "=="]:
                    return self._convert_intersect(op)
            elif op.op in ["intersects", "contains", "within"]:
                raise ValueError(f"can't do '{op.op}' on non-geo fields")
            left = self.convert(op.left)
            right = self.convert(op.right)
            return exp.BinOp(left=left, op=op.op, right=right)
        if isinstance(op, exp.Field):
            return op
        if isinstance(op, exp.Scalar):
            return op
        if isinstance(op, exp.UnaryOp):
            cur = self
            if op.prefix == "~":
                cur = self.neg()
            return exp.UnaryOp(prefix=op.prefix, exp=cur.convert(op.exp), suffix=op.suffix)
        if isinstance(op, exp.Func):
            cur = self
            if op.name == "invert":
                cur = self.neg()
            args = [cur.convert(a) for a in op.args]
            return exp.Func(name=op.name, args=args)
        raise ValueError(f"can't convert {op}: {type(op)}")

    def _convert_intersect(self, op: exp.BinOp) -> exp.Op:
        if isinstance(op.left, exp.Field):
            if isinstance(op.right, exp.Scalar):
                geo = shapely.from_wkt(op.right.to_py())
                return self._intersect_field(op.left, geo)
            # if isinstance(op.right, exp.Field):
            # return exp.Scalar.from_py(~self.negate)

        if isinstance(op.right, exp.Field):
            if isinstance(op.left, exp.Scalar):
                geo = shapely.from_wkt(op.left.to_py())
                return self._intersect_field(op.right, geo)

        raise ValueError(f"unsupported: {type(op.left)} {op.op} {type(op.right)}")

    def _intersect_field(self, field: exp.Field, geo: shapely.Geometry) -> exp.Op:
        logging.info("intersecting field '%s' with '%s'", field, geo)
        fx = exp.Field(name=field.name + ".x")
        fy = exp.Field(name=field.name + ".y")
        fq = exp.Field(name=field.name + ".q")
        x0, y0, x1, y1 = shapely.bounds(geo).tolist()
        if self.negate:
            xop = exp.BinOp(
                exp.Parens(exp.BinOp(exp.BinOp(fx, "-", fq), ">=", exp.Scalar.from_py(x0))),
                "and",
                exp.Parens(exp.BinOp(exp.BinOp(fx, "+", fq), "<=", exp.Scalar.from_py(x1))),
            )
            yop = exp.BinOp(
                exp.Parens(exp.BinOp(exp.BinOp(fy, "-", fq), ">=", exp.Scalar.from_py(y0))),
                "and",
                exp.Parens(exp.BinOp(exp.BinOp(fy, "+", fq), "<=", exp.Scalar.from_py(y1))),
            )
        else:
            xop = exp.BinOp(
                exp.Parens(exp.BinOp(exp.BinOp(fx, "+", fq), ">=", exp.Scalar.from_py(x0))),
                "and",
                exp.Parens(exp.BinOp(exp.BinOp(fx, "-", fq), "<=", exp.Scalar.from_py(x1))),
            )
            yop = exp.BinOp(
                exp.Parens(exp.BinOp(exp.BinOp(fy, "+", fq), ">=", exp.Scalar.from_py(y0))),
                "and",
                exp.Parens(exp.BinOp(exp.BinOp(fy, "-", fq), "<=", exp.Scalar.from_py(y1))),
            )
        return exp.Parens(exp.BinOp(xop, "and", yop))


def test_query():
    op = exp.parse("color == 'red' and not (area intersect 'POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))')")
    logging.info("'%s'...", op)
    c = _QueryContext(["area"])
    op2 = c.convert(op)
    logging.info("'%s'...", op2)
    assert "color == 'red'" in str(op2)
    assert "area.x - area.q >= 0" in str(op2)  # inverted sign

    # check that raises exception if intersect with no geo field
    try:
        op = exp.parse("other_field intersect 'POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))'")
        c = _QueryContext([])
        c.convert(op)
    except ValueError as e:
        assert "intersect" in str(e)
    else:
        assert False
