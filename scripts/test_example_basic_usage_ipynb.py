import papermill as pm
from os import getenv, path
from tempfile import TemporaryDirectory


with TemporaryDirectory() as td:
    pm.execute_notebook(
        "Examples/Basic Usage.ipynb",
        path.join(td, "Basic Usage.ipynb"),
        parameters={
            "ODP_API_KEY": getenv("ODP_API_KEY")
        }
    )

