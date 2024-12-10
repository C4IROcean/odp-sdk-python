import logging
import os
from typing import Iterable

from odp.client.tabular_v2.big.big import BigCol


class LocalBigCol(BigCol):
    def __init__(self, root: str):
        super().__init__()
        self.root = root
        os.makedirs(self.root, exist_ok=True)

    def fetch(self, big_id: str) -> bytes:
        logging.info("downloading %s", big_id)
        with open(f"{self.root}/{big_id}.big", "rb") as f:
            return f.read()

    def upload(self, big_id: str, data: Iterable[bytes]):
        logging.info("uploading %s", big_id)
        with open(f"{self.root}/{big_id}.big", "wb") as f:
            for d in data:
                f.write(d)
