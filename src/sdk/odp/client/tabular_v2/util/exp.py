import ast
import json
from typing import Any, Callable, Dict, Iterator, List, Optional

import pyarrow.compute as pc
from lark import Lark, Transformer, UnexpectedInput
from pydantic import BaseModel, PrivateAttr
from shapely.geometry.geo import shape


class Op(BaseModel):
    def __repr__(self):
        return f"〔{str(self)}〕"

    def all(self) -> Iterator["Op"]:
        """returns all the nodes in the tree"""
        yield self
        for attr in self.__dict__:
            value = getattr(self, attr)
            if isinstance(value, Op):
                yield from value.all()
            if isinstance(value, list):
                for v in value:
                    if isinstance(v, Op):
                        yield from v.all()

    def walk(self, fn: Callable[["Op"], "Op"]) -> "Op":
        """walks the tree and applies the given function to each node"""
        out = fn(self) or self

        for attr in out.__dict__:
            value = getattr(out, attr)
            if isinstance(value, Op):
                value = value.walk(fn)
                setattr(out, attr, value)
            if isinstance(value, list):
                value = [v.walk(fn) for v in value if isinstance(v, Op)]
                setattr(out, attr, value)

        return out

    def pyarrow(self) -> pc.Expression:
        raise NotImplementedError("pyarrow not implemented for %s" % self.__class__.__name__)


class Field(Op):
    _invocant: Optional[Op] = PrivateAttr(None)
    name: str

    def with_invocant(self, invocant: Op) -> "Field":
        self._invocant = invocant
        return self

    def __str__(self):
        if self._invocant:
            return f"{self._invocant}.{self.name}"
        else:
            return self.name

    def __repr__(self):
        if self._invocant:
            return f"〔{self._invocant.__repr__()} . {self.name}〕"
        else:
            return f"field({self.name})"

    def pyarrow(self) -> pc.Expression:
        if self._invocant is None:
            return pc.field(self.name)
        name = []
        cur = self
        while cur:
            if isinstance(cur, Field):
                name.append(cur.name)
                cur = cur._invocant
            else:
                raise ValueError(f"pyarrow unsupported {self}")
        return pc.field(".".join(reversed(name)))


class Func(Field):
    args: list[Op]

    def __str__(self):
        if self._invocant is None:
            return f"{self.name}({', '.join(map(str, self.args))})"
        return f"{super().__str__()}({', '.join(map(str, self.args))})"

    def __repr__(self):
        if self._invocant is None:
            return f"〔{self.name}({', '.join(map(repr, self.args))})〕"
        return f"〔{super().__repr__()}({', '.join(map(repr, self.args))})〕"

    def pyarrow(self) -> pc.Expression:
        args = [a.pyarrow() for a in self.args]
        if self._invocant is None:
            return pc.field("")._call(self.name, args)
        raise ValueError(f"pyarrow unsupported {self}")


_op_map = {
    "AND": "and",
    "&": "and",
    "OR": "or",
    "|": "or",
    "=": "==",
}

_op_rev = {
    "==": "==",
    "!=": "!=",
    "<": ">=",
    "<=": ">",
    ">": "<=",
    ">=": "<",
}


class BinOp(Op):
    left: Op
    op: str
    right: Op

    def __init__(self, left: Op, op: str, right: Op):
        super().__init__(
            left=left,
            op=_op_map.get(op, op),
            right=right,
        )

    def flip(self) -> "BinOp":
        if self.op in _op_rev:
            return BinOp(self.right, _op_rev[self.op], self.left)
        raise NotImplementedError(f"Can't flip {self.op}")

    def __str__(self):
        return f"{self.left} {self.op} {self.right}"

    def __repr__(self):
        return f"〔{self.left.__repr__()} {self.op} {self.right.__repr__()}〕"

    def pyarrow(self) -> pc.Expression:
        left = self.left.pyarrow()
        right = self.right.pyarrow()
        if self.op == "<":
            return left < right
        if self.op == "<=":
            return left <= right
        if self.op == ">":
            return left > right
        if self.op == ">=":
            return left >= right
        if self.op == "==":
            return left == right
        if self.op == "!=":
            return left != right
        if self.op == "+":
            return left + right
        if self.op == "-":
            return left - right
        if self.op == "*":
            return left * right
        if self.op == "/":
            return left / right
        if self.op == "and":
            return pc.and_kleene(left, right)
        if self.op == "or":
            return pc.or_kleene(left, right)
        raise NotImplementedError(f"pyarrow not implemented for {self.op}")


class UnaryOp(Op):
    prefix: str
    exp: Op
    suffix: str

    def __str__(self):
        return f"{self.prefix}{self.exp}{self.suffix}"

    def pyarrow(self):
        return ~(self.exp.pyarrow())

    @staticmethod
    def negate(op: Op) -> "UnaryOp":
        return UnaryOp(prefix="~", exp=op, suffix="")


class Scalar(Op):
    src: str  # what was in the source code, e.g. '"123"' or 'True'
    type: str

    def __str__(self):
        return self.src

    @classmethod
    def from_py(cls, val):
        if val is None:
            return cls(src="null", type="null")
        if isinstance(val, bool):
            if val:
                return cls(src="true", type="bool")
            else:
                return cls(src="false", type="bool")
        if isinstance(val, float | int):
            return cls(src=str(val), type="SIGNED_NUMBER")
        if isinstance(val, str):
            return cls(src=json.dumps(val), type="ESCAPED_STRING")
        raise NotImplementedError(f"Scalar.from_py not implemented for {type(val)}")

    def to_py(self):
        if self.type in ["SIGNED_NUMBER"]:
            return float(self.src)
        if self.type in ["ESCAPED_STRING"]:
            return json.loads(self.src)
        if self.type == "bool":
            if self.src.lower() == "true":
                return True
            if self.src.lower() == "false":
                return False
            raise ValueError("unexpected bool value: %s" % self.src)
        if self.type == "null":
            return None
        try:
            return ast.literal_eval(self.src)  # raises ValueError
        except Exception:
            pass
        raise ValueError(f"unexpected scalar value: {self.src}")

    def pyarrow(self) -> pc.Expression:
        if self.type in ["SIGNED_NUMBER"]:
            return pc.scalar(float(self.src))  # NOTE(oha) is this ok?
        if self.type in ["ESCAPED_STRING"]:
            return pc.scalar(json.loads(self.src))
        if self.type == "bool":
            if self.src.lower() == "true":
                return pc.scalar(True)
            if self.src.lower() == "false":
                return pc.scalar(False)
            raise ValueError("unexpected bool value: %s" % self.src)
        if self.type == "null":
            return pc.scalar(None)
        try:
            return pc.scalar(ast.literal_eval(self.src))  # raises ValueError
        except ValueError:
            raise ValueError(f"unexpected scalar value: {self.src}")


class Parens(Op):
    exp: Op

    def __init__(self, exp: Op):
        super().__init__(exp=exp)

    def __str__(self):
        return f"({self.exp})"

    def pyarrow(self) -> pc.Expression:
        return self.exp.pyarrow()


class _T(Transformer):
    def exp(self, tok):
        return tok[0]

    def func(self, tok):
        name = tok[0].value
        args = tok[1:]
        if args == [None]:  # no args
            args = []
        if name == "add_checked" or name == "add":
            assert len(args) == 2
            return Parens(BinOp(left=args[0], op="+", right=args[1]))
        if name == "subtract_checked":
            assert len(args) == 2
            return Parens(BinOp(left=args[0], op="-", right=args[1]))
        return Func(name=name, args=args or [])

    def scalar(self, tok):
        return Scalar(src=tok[0].value, type=tok[0].type)

    def field(self, tok):
        name = tok[0].value
        if name in ["True", "False", "true", "false"]:
            return Scalar(src=name, type="bool")
        if name in ["NULL", "null", "None"]:
            return Scalar(src="null", type="null")
        return Field(name=name)

    def member(self, tok):
        member: Field = tok[1]
        member = member.with_invocant(tok[0])
        return member

    def parens(self, tok):
        return Parens(exp=tok[0])

    def mul(self, tok):
        return BinOp(left=tok[0], op=tok[1], right=tok[2])

    def add(self, tok):
        return BinOp(left=tok[0], op=tok[1], right=tok[2])

    def cmp(self, tok):
        return BinOp(left=tok[0], op=tok[1], right=tok[2])

    def cmp2(self, tok):
        return BinOp(left=tok[0], op=tok[1], right=tok[2])

    def bool(self, tok):
        return BinOp(left=tok[0], op=tok[1], right=tok[2])

    def notop(self, tok: Any):
        return UnaryOp(prefix="~", exp=tok[0], suffix="")

    def is_null(self, tok):
        return Func(name="is_null", args=[tok[0]])

    def is_not_null(self, tok):
        return UnaryOp(prefix="~", exp=self.is_null(tok), suffix="")


_parser = Lark(
    """

?exp: bool

parens: "(" bool ")"

?bool: (bool /(and|or|AND|OR)/)? cmp

?cmp: (cmp /(==|!=)/)? cmp2
    | is_null
    | is_not_null

is_null: add "is" "null"
       | add "is" "None"
is_not_null: add "is" "not" "null"
           | add "is" "not" "None"

?cmp2: (cmp2 /(<=|<|>=|>)/)? add

?add: (add /(\\+|-)/)? mul

?mul: (mul /(\\*|\\/|%)/)? term

?pow: term ("**" pow)?

notop: "~" term
     | "not" term

?term: notop
     | func
     | scalar
     | member
     | field
     | parens

scalar: SIGNED_NUMBER
      | /(True|False|None|null|true|false)/
      | ESCAPED_STRING
      | /'[^']*'/

member: (parens | member | field | func) "." (func | field)

field: NAME

func: NAME "(" [exp ("," exp)*] ")"

NAME: /[a-zA-Z_][a-zA-Z0-9_]*/

%import common.ESCAPED_STRING
%import common.SIGNED_NUMBER
%import common.WS

%ignore WS
""",
    start="exp",
    transformer=_T(),
    parser="lalr",
    debug=True,
)


class ExpressionError(Exception):
    def __init__(self, msg: str, at: str):
        super().__init__(msg)
        self.at = at


def parse(s: str) -> Op:
    """parses the given string into an Op tree"""
    try:
        return _parser.parse(s)  # noqa
    except UnexpectedInput as e:
        at = e.get_context(s)
        raise ExpressionError("Can't parse:\n" + at + "\n" + str(e), at) from e


def parse_oqs_dict(oqs: Dict[str, List[Any]] | str) -> Op:
    """parses the given JSON string into an Op tree"""
    # assert len(oqs) == 1
    if not oqs:
        return Scalar.from_py(None)
    if isinstance(oqs, dict):
        operation, operands = next(iter(oqs.items()))
        operation = operation.lower()
        if not operation.startswith("#"):
            return Scalar.from_py(shape(oqs).wkt)
        if operation == "#constant":
            return Scalar.from_py(operands[0])
        if operation == "#ref":
            return Field(name=operands[0])
        if operation == "#list":
            return [parse_oqs_dict(v) for v in operands]
        if operation == "#equals":
            return BinOp(left=parse_oqs_dict(operands[0]), op="==", right=parse_oqs_dict(operands[1]))
        if operation == "#not_equals":
            return BinOp(left=parse_oqs_dict(operands[0]), op="!=", right=parse_oqs_dict(operands[1]))
        if operation == "#greater_than":
            return BinOp(left=parse_oqs_dict(operands[0]), op=">", right=parse_oqs_dict(operands[1]))
        if operation == "#greater_than_or_equals":
            return BinOp(left=parse_oqs_dict(operands[0]), op=">=", right=parse_oqs_dict(operands[1]))
        if operation == "#less_than":
            return BinOp(left=parse_oqs_dict(operands[0]), op="<", right=parse_oqs_dict(operands[1]))
        if operation == "#less_than_or_equals":
            return BinOp(left=parse_oqs_dict(operands[0]), op="<=", right=parse_oqs_dict(operands[1]))
        if operation == "#and":
            op_list = [parse_oqs_dict(v) for v in operands]
            return list_to_tree_expression(op_list, "and")
        if operation == "#or":
            op_list = [parse_oqs_dict(v) for v in operands]
            return list_to_tree_expression(op_list, "or")
        if operation == "#xor":
            raise ValueError("xor not supported in tabular")
        if operation == "#within":
            right_op = parse_oqs_dict(operands[1])
            if isinstance(right_op, list):
                op_list = [BinOp(left=parse_oqs_dict(operands[0]), op="==", right=operand) for operand in right_op]
                return list_to_tree_expression(op_list, "or")
            return BinOp(left=parse_oqs_dict(operands[0]), op="==", right=right_op)
        if operation == "#negate":
            return UnaryOp.negate(parse_oqs_dict(operands[0]))
        if operation == "#non_null":
            return BinOp(left=parse_oqs_dict(operands[0]), op="!=", right=Scalar.from_py(None))
        if operation == "#null":
            return BinOp(left=parse_oqs_dict(operands[0]), op="==", right=Scalar.from_py(None))
        if operation == "#true":
            return Scalar.from_py(True)
        if operation == "#false":
            return Scalar.from_py(False)
        if operation == "#sum":
            return Func(name="sum", args=[parse_oqs_dict(operands[0])])
        if operation == "#difference":
            return Func(name="difference", args=[parse_oqs_dict(operands[0])])
        if operation == "#product":
            return Func(name="product", args=[parse_oqs_dict(operands[0])])
        if operation == "#quotient" or operation == "#floor_quotient":
            return BinOp(left=parse_oqs_dict(operands[0]), op="/", right=parse_oqs_dict(operands[1]))
        if operation == "#modulo":
            return BinOp(left=parse_oqs_dict(operands[0]), op="%", right=parse_oqs_dict(operands[1]))
        if operation == "#exponentiation":
            return BinOp(left=parse_oqs_dict(operands[0]), op="**", right=parse_oqs_dict(operands[1]))
        if operation == "#st_within":
            return Func(name="ktable_st_within", args=[parse_oqs_dict(operands[0]), parse_oqs_dict(operands[1])])
        if operation == "#st_equals":
            return Func(name="ktable_st_equals", args=[parse_oqs_dict(operands[0]), parse_oqs_dict(operands[1])])
        if operation == "#st_intersects":
            return Func(name="ktable_st_intersects", args=[parse_oqs_dict(operands[0]), parse_oqs_dict(operands[1])])
        if operation == "#st_contains":
            return Func(name="ktable_st_contains", args=[parse_oqs_dict(operands[0]), parse_oqs_dict(operands[1])])
        # TODO: Decide how to handle times, timestamps and dates
        raise ValueError(f"unexpected key {operation}")

    if isinstance(oqs, str):
        if oqs.startswith("$"):
            return Field(name=oqs.removeprefix("$"))

    try:
        return Scalar.from_py(oqs)
    except NotImplementedError:
        raise ValueError(f"unexpected value {oqs} with type {type(oqs)}")


def list_to_tree_expression(list_expressions: List[Op], operation: str) -> Op:
    if len(list_expressions) == 1:
        return list_expressions[0]
    else:
        temp = []
        for i in range(0, len(list_expressions), 2):
            if i + 1 == len(list_expressions):
                temp.append(list_expressions[i])
            else:
                temp.append(BinOp(left=list_expressions[i], op=operation, right=list_expressions[i + 1]))
        return list_to_tree_expression(temp, operation)
