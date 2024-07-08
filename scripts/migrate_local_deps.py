from pathlib import Path
from typing import Annotated, Dict, List, Optional, Union

import tomli
import tomli_w
import typer

app = typer.Typer()


def _update_local_version(
    dep: Dict[str, Union[str, Dict[str, str]]], version_overrides: Dict[str, str], default_version_tag: str
) -> Dict[str, Union[str, Dict[str, str]]]:
    ret = {}

    for dep_name, dep_info in dep.items():
        if isinstance(dep_info, str):
            ret[dep_name] = dep_info
            continue

        dep_info = dep_info.copy()
        ret[dep_name] = dep_info

        pth = dep_info.get("path")
        if pth is None:
            continue

        dep_info.pop("path")
        dep_info.pop("develop", None)

        dep_info["version"] = version_overrides.get(pth, default_version_tag)

    return ret


@app.command()
def migrate(
    default_version_tag: Annotated[str, typer.Argument(help="Default version tag to use for local dependencies")],
    src_file: Annotated[Path, typer.Argument(help="Path to the file to migrate")],
    dest_file: Annotated[
        Optional[Path], typer.Argument(help="Path to the destination file, defaults to the source file if not set")
    ] = None,  # noqa: E501
    dry_run: Annotated[bool, typer.Option(help="Run in dry-run mode")] = False,
    overwrite: Annotated[bool, typer.Option(help="Overwrite the destination file if it exists")] = False,
    version_tag: Annotated[List[str], typer.Option(help="Version tags to use for local dependencies")] = [],
):
    try:
        version_overrides = {k: v for k, v in (x.split("=") for x in version_tag)}
    except ValueError as e:
        typer.echo(f"Invalid version tag: {e}")
        raise typer.Exit(code=1)

    print(f"Version overrides: {version_overrides}")

    if not src_file.suffix and src_file.is_dir():
        typer.echo("Directory detected, looking for pyproject.toml")
        src_file /= "pyproject.toml"
    elif src_file.suffix != ".toml":
        typer.echo("Only TOML files are supported: {}".format(src_file))
        raise typer.Exit(code=1)

    if not dest_file and not dry_run:
        if not overwrite:
            typer.echo(
                "Destination file not set. Using source file as destination but overwrite-flag is not set. Please set the destination file or use the --overwrite flag."  # noqa: E501
            )
            typer.Exit(code=1)

        typer.echo("Destination file not set. Using source file as destination.")
        dest_file = src_file

    if dest_file and not dest_file.suffix and dest_file.is_dir():
        dest_file /= "pyproject.toml"
    elif dest_file and dest_file.suffix != ".toml":
        typer.echo("Only TOML files are supported")
        raise typer.Exit(code=1)

    if not src_file.exists():
        typer.echo(f"Source file {src_file} does not exist")
        raise typer.Exit(code=1)

    if dest_file and dest_file.exists() and not overwrite:
        typer.echo(f"Destination file {dest_file} exists and overwrite flag is not set")
        raise typer.Exit(code=1)

    with src_file.open("rb") as f:
        data = tomli.load(f)

        try:
            poetry_base = data["tool"]["poetry"]
        except KeyError:
            typer.echo("No poetry section found in the source file")
            raise typer.Exit(code=1)

        for key in ["dependencies", "dev-dependencies", "optional-dependencies"]:
            if key in poetry_base:
                poetry_base[key] = _update_local_version(poetry_base[key], version_overrides, default_version_tag)

        for group in poetry_base.get("group", []):
            try:
                poetry_base[group]["dependencies"] = _update_local_version(
                    poetry_base[group]["dependencies"], version_overrides, default_version_tag
                )
            except KeyError:
                pass

    if dry_run:
        typer.echo("Dry-run mode, not writing to file")
        typer.echo(tomli_w.dumps(data))
        raise typer.Exit(code=0)

    with dest_file.open("wb+") as f:
        tomli_w.dump(data, f)


if __name__ == "__main__":
    app()
