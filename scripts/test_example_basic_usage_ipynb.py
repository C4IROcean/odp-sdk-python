import papermill as pm
from os import getenv


pm.execute_notebook(
    "Examples/Basic Usage.ipynb",
    "bin/Basic Usage.ipynb",
    parameters={
        "ODP_API_KEY": getenv("ODP_API_KEY")
    }
)

