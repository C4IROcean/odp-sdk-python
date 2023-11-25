from typing import IO, Any, Callable, Dict, List, Optional, Protocol, Type, Union

JsonType = Union[None, int, str, bool, List["JsonType"], Dict[str, "JsonType"]]


class JsonParser(Protocol):
    """JSON serialization/deserialization interface"""

    @staticmethod
    def load(
        fp,
        decoded_type: Type,
        cast_decimal: bool = True,
        cls: Optional[Type] = None,
        parse_float: Optional[Callable[[str], float]] = None,
        parse_int: Optional[Callable[[str], int]] = None,
        parse_constant: Optional[Callable[[str], JsonType]] = None,
        **kwargs
    ) -> JsonType:
        ...

    @staticmethod
    def loads(
        s: str,
        cast_decimal: bool = True,
        cls: Optional[Type] = None,
        parse_float: Optional[Callable[[str], float]] = None,
        parse_int: Optional[Callable[[str], int]] = None,
        parse_constant: Optional[Callable[[str], JsonType]] = None,
        **kwargs
    ) -> JsonType:
        ...

    @staticmethod
    def dump(
        obj: Any,
        fp: IO,
        skipkeys: bool = False,
        ensure_ascii: bool = True,
        check_circular: bool = True,
        allow_nan: bool = True,
        cls: Optional[Type] = None,
        indent=None,
        separators=None,
        default=None,
        sort_keys=False,
        **kwargs
    ):
        ...

    @staticmethod
    def dumps(
        obj: Any,
        skipkeys: bool = False,
        ensure_ascii: bool = True,
        check_circular: bool = True,
        allow_nan: bool = True,
        cls: Optional[Type] = None,
        indent: Optional[int] = None,
        separators: Optional[str] = None,
        default: Optional[Callable[[str], str]] = None,
        sort_keys: bool = False,
        **kwargs
    ) -> str:
        ...
