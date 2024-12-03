import base64
import logging
import os
import threading
from time import time

from odp.client.tabular_v2.util.util import size2human


class Cache:
    class Entry:
        def __init__(self, key: str, cache: "Cache"):
            self.key = key
            self.lock = threading.Lock()
            self.cache = cache
            self.filename = base64.b64encode(key.encode()).decode()
            self.size = 0

        def set(self, value: bytes) -> bool:
            if len(value) > self.cache.max_entry_size:
                return False

            self.cache._make_space(len(value))
            self.cache.tot_bytes -= self.size  # if replacing, this will be non-zero
            self.size = len(value)
            self.cache.tot_bytes += self.size

            with open(self.cache.root_folder + "/" + self.filename, "wb") as f:
                f.write(value)

        def exists(self) -> bool:
            filename = self.cache.root_folder + "/" + self.filename
            return os.path.exists(filename)

        def age(self) -> float:
            return time() - os.path.getctime(self.cache.root_folder + "/" + self.filename)

        def get(self, max_age: float | None = None) -> bytes | None:
            try:
                if max_age is not None:
                    if self.age() > max_age:  # expired
                        logging.info("expired %s (age: %.f > %.f)", self.key, self.age(), max_age)
                        # TODO remove?
                        return None
                with open(self.cache.root_folder + "/" + self.filename, "rb") as f:
                    return f.read()
            except FileNotFoundError:
                return None

        def touch(self):
            file_path = self.cache.root_folder + "/" + self.filename
            if not os.path.exists(file_path):
                return
            os.utime(file_path)

        def unlink(self):
            try:
                os.unlink(self.cache.root_folder + "/" + self.filename)
            except FileNotFoundError:
                logging.info(
                    "removing but already gone: %s (%s)", self.key, self.cache.root_folder + "/" + self.filename
                )

        def __enter__(self):
            self.lock.acquire()
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            self.lock.release()

    def __init__(self, folder: str, max_entries=100, max_bytes=64 * 1024 * 1024):
        self.lock = threading.Lock()
        self.cache = []
        self.root_folder = folder
        self.max_entries = max_entries
        self.max_bytes = max_bytes
        self.max_entry_size = max_bytes // 16
        self.tot_bytes = 0

        os.makedirs(self.root_folder, exist_ok=True)
        # list files by mtime
        files = sorted(os.listdir(self.root_folder), key=lambda f: os.path.getmtime(self.root_folder + "/" + f))
        for f in files:
            key = base64.b64decode(f.encode()).decode()
            e = Cache.Entry(key, self)
            size = os.path.getsize(self.root_folder + "/" + f)
            self.tot_bytes += size
            assert f == e.filename
            logging.info("recovered %s file %s", size2human(size), key)
            self.cache.append(e)

        self._make_space(0)
        logging.info("recovered %d files for a total of %s", len(self.cache), size2human(self.tot_bytes))

    def _make_space(self, space_needed):
        with self.lock:
            while self.tot_bytes + space_needed > self.max_bytes:
                # FIXME: Needs to be properly handled
                if len(self.cache) == 0:
                    self.tot_bytes = 0
                    return
                e = self.cache.pop(0)
                try:
                    size = os.path.getsize(self.root_folder + "/" + e.filename)
                    self.tot_bytes -= size
                    e.unlink()
                    logging.info("evicted %s file %s", size2human(size), e.key)
                except FileNotFoundError:
                    logging.info("evicted but already gone: %s", e.key)

    def remove(self, key):
        with self.lock:
            for e in self.cache:
                if e.key == key:
                    self.cache.remove(e)
                    e.unlink()
                    self.tot_bytes -= e.size

    def key(self, key):
        with self.lock:
            for e in self.cache:
                if e.key == key:
                    self.cache.remove(e)
                    self.cache.append(e)  # move to end
                    return e
            if len(self.cache) >= self.max_entries:
                e = self.cache.pop(0)
                self.tot_bytes -= e.size
                e.unlink()
            e = Cache.Entry(key, self)
            self.cache.append(e)
            return e
