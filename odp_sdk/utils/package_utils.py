import os.path
from pathlib import Path

import tomli


def _get_config_path_string() -> str:
    root_dir = Path(__file__).parent.parent.parent
    return os.path.join(root_dir, "pyproject.toml")


def _get_project_meta():
    with open(_get_config_path_string(), mode="rb") as pyproject:
        return tomli.load(pyproject)["tool"]["poetry"]


def get_version():
    pkg_meta = _get_project_meta()
    return str(pkg_meta["version"])
