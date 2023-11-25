import json
from collections import deque
from io import BytesIO, StringIO
from typing import IO, Deque, Iterable, Optional, cast
from warnings import warn

from .json import JsonParser, JsonType


class NdJsonParser:
    def __init__(
        self,
        s: Optional[str | bytes] = None,
        fp: Optional[IO] = None,
        json_parser: JsonParser = json,
    ):
        self.json_parser = json_parser
        self.line = ""
        self.delimiter_stack: Deque[str] = deque()

        if s and fp:
            raise ValueError("Either 's' or 'fp' must be set, but now both")
        elif not s and not fp:
            raise ValueError("Either 's' or 'fp' must be set")

        self.fb = fp if fp else (StringIO(s) if isinstance(s, str) else BytesIO(s))

    def consume_line(self) -> JsonType:
        if self.delimiter_stack:
            warn("Attempting to parse NDJSON line while the delimiter stack was non-empty")

        obj = self.json_parser.loads(self.line)
        self.line = ""
        self.delimiter_stack.clear()

        return obj

    def __iter__(self) -> Iterable[JsonType]:
        return cast(Iterable[JsonType], self)

    def __next__(self) -> JsonType:
        while True:
            try:
                s = next(self.fb)
            except StopIteration:
                if self.line:
                    return self.consume_line()
                raise

            for c in s:
                if isinstance(c, int):
                    c = chr(c)
                last_delimiter = self.delimiter_stack[-1] if self.delimiter_stack else None

                in_quote = last_delimiter in {"'", '"', "\\"}

                if c == "\n" and not in_quote:
                    return self.consume_line()

                self.line += c
                if in_quote:
                    if last_delimiter == "\\":
                        self.delimiter_stack.pop()
                    elif c == "\\":
                        self.delimiter_stack.append(c)
                    elif c == last_delimiter:
                        self.delimiter_stack.pop()

                    continue

                is_quote = c in {"'", '"'}
                if is_quote:
                    self.delimiter_stack.append(c)
                    continue

                is_opening_bracket = c in {"{", "["}

                if is_opening_bracket:
                    self.delimiter_stack.append(c)
                    continue

                in_bracket = last_delimiter in {"{", "["}
                is_closing_bracket = c in {"}", "]"}

                if is_closing_bracket:
                    if not in_bracket:
                        raise ValueError(f"Got unexpected delimiter: {c}")

                    if last_delimiter == "{" and c == "}":
                        self.delimiter_stack.pop()
                    elif last_delimiter == "[" and c == "]":
                        self.delimiter_stack.pop()
                    else:
                        raise ValueError(f"Got unexpected delimiter: {c}")
