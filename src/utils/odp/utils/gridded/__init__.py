import warnings

try:
    import os

    os.environ["ZARR_V3_EXPERIMENTAL_API"] = "1"
    warnings.filterwarnings("ignore", message=r".*The experimental Zarr V3.*")

    from zarr._storage.store import assert_zarr_v3_api_available

    assert_zarr_v3_api_available()

    from .gridded_on_raw import OdpStoreV3  # noqa: F401, F841

    # Delete package reference to prevent them from being imported from here
    del assert_zarr_v3_api_available
    del os

except ImportError:
    warnings.warn("Zarr not installed. Gridded support not enabled")
except NotImplementedError as e:
    raise ImportError(
        "Zarr V3 is unavailable. "
        "Make sure to import this module before importing the 'zarr'-package "
        "or ensure that the env-variable 'ZARR_V3_EXPERIMENTAL_API'='1' is set."
    ) from e

# Delete package reference to prevent them from being imported from here
del warnings
