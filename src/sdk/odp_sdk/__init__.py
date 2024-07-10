import warnings

from odp.sdk import *  # noqa: F401, F403

warnings.warn("odp_sdk is deprecated, please import odp.client instead", DeprecationWarning)

del warnings
