import json
from collections import deque
from io import BytesIO, StringIO
from typing import IO, Deque, Iterable, Optional, cast
from warnings import warn

from .json import JsonParser, JsonType

DEFAULT_JSON_PARSER = cast(JsonParser, json)


class NdJsonParser:
    """Newline delimited JSON parser

    Parses NDJSON from a file-like object or a string.
    """

    def __init__(
        self,
        s: Optional[str | bytes] = None,
        fp: Optional[IO] = None,
        json_parser: JsonParser = DEFAULT_JSON_PARSER,
    ):
        """Initialize the parser

        Args:
            s: String to parse, either this or 'fp' must be set
            fp: File-like object to parse, either this or 's' must be set
            json_parser: JSON parser to use, defaults to the standard `json` module
        """
        self.json_parser = json_parser
        self.line = ""
        self.delimiter_stack: Deque[str] = deque()

        if s and fp:
            raise ValueError("Either 's' or 'fp' must be set, but now both")
        elif not s and not fp:
            raise ValueError("Either 's' or 'fp' must be set")

        self.fb = fp if fp else (StringIO(s) if isinstance(s, str) else BytesIO(s))

    def _consume_line(self) -> JsonType:
        """Consume a line from the file-like object

        Returns:
            Parsed JSON object
        """
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
                    return self._consume_line()
                raise

            for c in s:
                if isinstance(c, int):
                    c = chr(c)
                last_delimiter = self.delimiter_stack[-1] if self.delimiter_stack else None

                in_quote = last_delimiter in {"'", '"', "\\"}

                if c == "\n" and not in_quote:
                    return self._consume_line()

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


def load(fp: IO, json_parser: JsonParser = DEFAULT_JSON_PARSER) -> Iterable[JsonType]:
    """Load NDJSON from a file-like object

    Args:
        fp: File-like object
        json_parser: JSON parser to use

    Returns:
        Iterable of parsed JSON objects
    """
    return iter(NdJsonParser(fp=fp, json_parser=json_parser))


def loads(s: str | bytes, json_parser: JsonParser = DEFAULT_JSON_PARSER) -> Iterable[JsonType]:
    """Load NDJSON from a string

    Args:
        s: String
        json_parser: JSON parser to use

    Returns:
        Iterable of parsed JSON objects
    """
    return iter(NdJsonParser(s=s, json_parser=json_parser))
