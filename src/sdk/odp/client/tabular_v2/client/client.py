import io
import logging
from typing import TYPE_CHECKING, Iterator

import requests
from odp.client.tabular_v2.util.reader import Iter2Reader

if TYPE_CHECKING:
    from odp.client.tabular_v2.client import TableHandler


class Client:
    def __init__(self, base_url: str):
        self._base_url = base_url

    class Response:
        # Abstraction for response object, shared between http client and test client
        def __init__(self, res: requests.Response | Iterator[bytes] | dict | bytes):
            if isinstance(res, requests.Response):
                if res.status_code == 204:
                    raise FileNotFoundError(res.text)
                res.raise_for_status()
            logging.info("response: %s", res)
            self.res = res

        def reader(self):
            if isinstance(self.res, bytes):
                return io.BytesIO(self.res)
            if isinstance(self.res, Iterator):
                return Iter2Reader(self.res)
            return self.res.raw

        def iter(self) -> Iterator[bytes]:
            if isinstance(self.res, bytes):
                return iter([self.res])
            if isinstance(self.res, Iterator):
                return self.res
            return self.res.iter_content()

        def all(self) -> bytes:
            if isinstance(self.res, bytes):
                return self.res
            if isinstance(self.res, Iterator):
                return b"".join(self.res)
            return self.res.content

        def json(self) -> dict:
            if isinstance(self.res, dict):
                return self.res
            return self.res.json()

    def _request(
        self,
        path: str,
        data: dict | bytes | None = None,
        params: dict | None = None,
        headers: dict | None = None,
    ) -> Response:
        logging.info("ktable: REQ %s %s (%d bytes)", path, params, len(data) if data else 0)
        if isinstance(data, dict):
            res = requests.post(self._base_url + path, headers=headers, params=params, json=data, stream=True)
        elif isinstance(data, bytes):
            res = requests.post(self._base_url + path, headers=headers, params=params, data=data, stream=True)
        elif isinstance(data, Iterator):
            res = requests.post(self._base_url + path, headers=headers, params=params, data=data, stream=True)
        elif data is None:
            res = requests.post(self._base_url + path, headers=headers, params=params, stream=True)
        else:
            raise ValueError(f"unexpected type {type(data)}")
        logging.info("response: %s", res.status_code)
        return self.Response(res)

    # @lru_cache(maxsize=10)
    def table(self, table_id: str) -> "TableHandler":
        from odp.client.tabular_v2.client.tablehandler import TableHandler

        return TableHandler(self, table_id)
