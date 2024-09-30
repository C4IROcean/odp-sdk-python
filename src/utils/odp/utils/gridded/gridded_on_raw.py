from pathlib import Path
from typing import Iterable, Iterator, List, Tuple, Union
from uuid import UUID

from odp.client import OdpClient
from odp.client.dto.file_dto import FileMetadataDto
from odp.client.exc import OdpFileAlreadyExistsError, OdpFileNotFoundError
from odp.dto import ResourceDto
from zarr._storage.store import Store, StoreV3
from zarr._storage.v3 import RmdirV3


class OdpStoreV3(RmdirV3, StoreV3, Store):
    """Zarr storage implementation for storing gridded data on ODP RAW using Zarr

    Examples:
        Read a gridded dataset:

        ```python
        import xarray as xr
        from odp.client import OdpClient

        client = OdpClient()
        dataset = ... # get or create dataset

        store = OdpStoreV3(client, dataset)

        # Load dataset from ODP RAW into an Xarray dataset
        ds = xr.open_zarr(store)

        # Do XArray stuff
        ...
        ```

        Write a gridded dataset:

        ```python
        import xarray as xr
        from odp.client import OdpClient

        client = OdpClient()
        dataset = ... # get or create dataset

        store = OdpStoreV3(client, dataset)

        ds = ... # Create or load an XArray dataset

        # Write dataset to ODP RAW
        ds.to_zarr(store)
        ```
    """

    FINFO_ZARR_KEY = "io.hubocean.raw.gridded/key"

    def __init__(self, client: OdpClient, dataset: Union[UUID, str, ResourceDto]):
        self._client = client

        if isinstance(dataset, ResourceDto):
            self._dataset = dataset
            self._dataset_ref = dataset.metadata.uuid or f"{dataset.kind}/{dataset.metadata.name}"
        else:
            self._dataset_ref = dataset

    @property
    def dataset(self) -> ResourceDto:
        if self._dataset is None or self._dataset.metadata.uuid is None:
            self._dataset = self._client.catalog.get(self._dataset_ref)
        return self._dataset

    def _dir_fname_tuple(self, key: str) -> Tuple[str, str, str]:
        """Simple function to split a path into directory name, file name and file suffix"""
        pth = Path(key)
        return str(pth.parent), pth.name, pth.suffix

    def _key_from_finfo(self, finfo: FileMetadataDto) -> str:
        """Convert from `FileMetadataDto`-object to zarr key using `io.hubocean.raw.gridded/key`-key from
        `finfo.metadata`.

        Args:
            finfo: FileMetadataDto

        Returns:
            Zarr key from `finfo`

        Raises:
            KeyError: Zarr key does not exists
        """
        try:
            if not finfo.metadata:
                raise ValueError(f"Missing zarr key from metadata: {self.FINFO_ZARR_KEY}")
            return finfo.metadata[self.FINFO_ZARR_KEY]
        except (ValueError, KeyError) as e:
            raise KeyError("Unable to get zarr key from file metadata") from e

    def _key_obj(self, key) -> FileMetadataDto:
        """Convert from a zarr key to `FileMetadataDto`

        Converts an incoming key by splitting key into its directory and filename components and returning
        `FileMetadataDto`. The original key is also added to the `metadata`-member of the returned object.

        Args:
            key: Key to be converted

        Returns:
            `FileMetadataDto` based on `key`
        """
        dirname, fname, suffix = self._dir_fname_tuple(key)
        if not suffix:
            fname = f"{fname}.bin"

        return FileMetadataDto(name=f"{dirname}/{fname}", metadata={self.FINFO_ZARR_KEY: key})

    def __getitem__(self, key: str) -> bytes:
        """Get a value associated with a given key

        Returns:
            Value associated with `key` if it assists

        Raises:
            KeyError: If key does not exist
        """
        try:
            return self._client.raw.download_file(self.dataset, self._key_obj(key))
        except OdpFileNotFoundError as e:
            raise KeyError(f"Key not found: '{key}'") from e

    def __setitem__(self, key: str, value: bytes):
        """Set a key-value pair

        Args:
            key: Key to be set
            value: Value associated with `key`
        """
        self._validate_key(key)
        key_obj = self._key_obj(key)

        try:
            self._client.raw.create_file(self.dataset, key_obj, value)
            return
        except OdpFileAlreadyExistsError:
            pass

        # File already exists - simply overwrite it
        self._client.raw.upload_file(self.dataset, key_obj, value, overwrite=True)

    def __delitem__(self, key: str):
        """Delete a key-value pair by key

        Args:
            key: Key to be deleted

        Raises:
            KeyError: Key does not exist
        """
        try:
            self._client.raw.delete_file(self.dataset, self._key_obj(key))
        except OdpFileNotFoundError as e:
            raise KeyError(f"Key not found: '{key}'") from e

    def keys(self) -> Iterable[str]:
        """Iterate keys in store

        Yields:
            Keys in store
        """
        for finfo in self._client.raw.list(self.dataset):
            try:
                yield self._key_from_finfo(finfo)
            except KeyError:
                pass

    def list(self) -> List[str]:
        """List keys in store

        Returns:
            All keys in store as a list
        """
        return list(self.keys())

    def __iter__(self) -> Iterator[str]:
        """Iterate keys in store

        Yields:
            Keys in store
        """
        yield from self.keys()

    def __len__(self) -> int:
        """Get number of keys in store

        Returns:
            Number of keys in store
        """
        return len(self.list())

    def clear(self):
        """Delete all keys in store"""
        for finfo in self._client.raw.list(self.dataset):
            self._client.raw.delete_file(self.dataset, finfo)
