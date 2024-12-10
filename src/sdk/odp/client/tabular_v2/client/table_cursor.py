from typing import Callable, Iterator

import pyarrow as pa


class CursorException(Exception):
    """Raised when the client is required to connect again with the given cursor to fetch more data"""

    def __init__(self, cursor: str):
        self.cursor = cursor


class Cursor:
    def __init__(
        self,
        scanner: Callable[[str], Iterator[pa.RecordBatch]],
    ):
        self.scanner = scanner

    def batches(self) -> Iterator[pa.RecordBatch]:
        cursor = ""
        while True:
            try:
                for b in self.scanner(cursor):
                    yield b
            except CursorException as e:
                cursor = e.cursor
                continue  # FIXME: Should not be raised?
            break

    def rows(self) -> Iterator[dict]:
        for b in self.batches():
            for row in b.to_pylist():
                yield row

    def pages(self, size: int = 0) -> Iterator[list[dict]]:
        if size < 1:  # page based on what we get
            for b in self.batches():
                yield b.to_pydict()
            return

        # page based on page_size
        buf: list[dict] = []
        for b in self.batches():
            buf.extend(b.to_pydict())
            while len(buf) >= size:
                yield buf[:size]
                buf = buf[size:]
        if len(buf) > 0:
            yield buf
