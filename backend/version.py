from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version as package_version
from pathlib import Path
import tomllib


def _repo_pyproject_path() -> Path:
    return Path(__file__).resolve().parents[1] / "pyproject.toml"


def _version_from_pyproject(path: Path) -> str:
    with path.open("rb") as fh:
        data = tomllib.load(fh)
    return str(data["project"]["version"])


def get_forge_version() -> str:
    pyproject_path = _repo_pyproject_path()
    if pyproject_path.exists():
        return _version_from_pyproject(pyproject_path)

    try:
        return package_version("forge")
    except PackageNotFoundError:
        return "0.0.0"
