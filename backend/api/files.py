"""Lightweight file/directory browser for the Forge UI.

Exposes a single endpoint that lists entries in a directory so the
frontend can offer a file-picker experience for I/O block filepath params.
"""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel

from backend.api.deps import get_services
from backend.services import AppServices

router = APIRouter(prefix="/files", tags=["files"])


class FileEntry(BaseModel):
    name: str
    path: str  # absolute path
    is_dir: bool


class BrowseResponse(BaseModel):
    current: str  # absolute path of the listed directory
    parent: str | None  # absolute path of the parent (None for root)
    entries: list[FileEntry]


def _normalize_browse_target(path: Path, fallback: Path) -> Path:
    target = path
    if not target.exists():
        target = target.parent if target.parent.exists() else fallback
    if target.is_file():
        target = target.parent
    return target


@router.get("/browse", response_model=BrowseResponse)
def browse_directory(
    path: str = Query(
        default="",
        description="Directory to list. Defaults to DEFAULT_FILE_PATH or the user home.",
    ),
    show_hidden: bool = Query(default=False, description="Include hidden files/dirs"),
    services: AppServices = Depends(get_services),
) -> BrowseResponse:
    """List files and directories at *path* or the configured default browse location."""

    configured_default = str(services.settings.default_file_path or "").strip()
    fallback = (
        Path(configured_default).expanduser().resolve()
        if configured_default
        else Path.home()
    )
    fallback = _normalize_browse_target(fallback, Path.home())

    requested = Path(path).expanduser().resolve() if path else fallback
    target = _normalize_browse_target(requested, fallback)

    entries: list[FileEntry] = []
    try:
        for entry in sorted(target.iterdir(), key=lambda p: (not p.is_dir(), p.name.lower())):
            if not show_hidden and entry.name.startswith("."):
                continue
            try:
                entries.append(
                    FileEntry(
                        name=entry.name,
                        path=str(entry),
                        is_dir=entry.is_dir(),
                    )
                )
            except PermissionError:
                continue
    except PermissionError:
        pass

    parent = str(target.parent) if target.parent != target else None

    return BrowseResponse(
        current=str(target),
        parent=parent,
        entries=entries,
    )
