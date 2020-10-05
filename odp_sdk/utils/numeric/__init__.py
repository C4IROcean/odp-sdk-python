
try:
    import metpy
    import scipy

    del metpy
    del scipy

except ImportError as e:
    raise ImportError("Missing dependencies for extended functionalities."
                      "Please install package using the [all]-option.\n"
                      "Example: pip install odp_sdk[all]\n"
                      ">>>> " + str(e))

from .casts import (
    interpolate_casts_to_z,
    interpolate_profile,
    interpolate_to_grid
)
