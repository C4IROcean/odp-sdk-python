import papermill as pm
from os import getenv, path
from tempfile import TemporaryDirectory


with TemporaryDirectory() as td:
    pm.execute_notebook(
        "Examples/Example Notebook Basic Usage.ipynb",
        path.join(td, "Basic Usage.ipynb"),
        parameters={
            "ODP_API_KEY": getenv("ODP_API_KEY")
        }
    )

