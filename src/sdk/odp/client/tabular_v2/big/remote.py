import gzip
import io
import logging
from typing import Callable, Iterable

from odp.client.tabular_v2.big.big import BigCol
from odp.client.tabular_v2.util.cache import Cache


class RemoteBigCol(BigCol):
    def __init__(
        self,
        uploader: Callable[[str, bytes], None],
        downloader: Callable[[str], bytes],
        root_cache: str,
    ):
        super().__init__()
        self.cache = Cache(root_cache)
        self.uploader = uploader
        self.downloader = downloader
        # TODO: make sure to not fill up the disk?

    def fetch(self, bid: str) -> bytes:
        with self.cache.key("big." + bid) as e:
            if not e.exists():
                logging.info("fetching %s", bid)
                comp = self.downloader(bid)
                e.set(comp)
            else:
                logging.info("cache hit %s", bid)
                comp = e.get()
            # if exists, use the cached version
            return gzip.decompress(comp)

    def upload(self, bid: str, data: Iterable[bytes]):
        with self.cache.key("big." + bid) as e:
            buf = io.BytesIO()
            with gzip.GzipFile(fileobj=buf, mode="wb") as f:
                for d in data:
                    f.write(d)
            comp = buf.getvalue()
            self.uploader(bid, comp)
            e.set(comp)
