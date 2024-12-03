import logging
from typing import Iterator


class Reader:
    def read(self, size: int = -1) -> bytes:
        raise NotImplementedError()


class Writer:
    def write(self, data: bytes):
        raise NotImplementedError()

    def close(self):
        pass


class Iter2Reader(Reader):
    """
    convert a byte iterator to a file-like object
    reads will attempt to read the next bytes from the iterator when needed

    FIXME: seems broken when used with real cases, avoid using
    """

    def __init__(self, i: Iterator[bytes]):
        self.iter = i
        self.closed = False
        self.buf = b""

    def preload(self):
        if not self.iter:
            return self
        try:
            self.buf += next(self.iter)
        except StopIteration:
            self.iter = None
        return self

    def read_some(self) -> bytes:
        if not self.buf:
            self.preload()
        out = self.buf
        self.buf = b""
        logging.debug("read %d", len(out))
        return out

    def read(self, size: int = -1) -> bytes:
        logging.debug("reading...")
        if size < 0:
            return self.read_some()
        while len(self.buf) < size:
            if not self.iter:
                break
            self.preload()
        ret = self.buf[:size]
        self.buf = self.buf[len(ret) :]
        logging.debug("read %d", len(ret))
        return ret
