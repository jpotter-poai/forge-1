"""REST endpoints for custom block plugin management."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, UploadFile
from fastapi.responses import Response

from backend.api.deps import get_services
from backend.custom_blocks import get_template
from backend.services import AppServices

router = APIRouter(prefix="/custom-blocks", tags=["custom-blocks"])


@router.get("")
def list_custom_blocks(services: AppServices = Depends(get_services)) -> list[dict]:
    """List all installed custom block files."""
    return services.custom_block_manager.list_blocks()


@router.post("/install")
async def install_custom_block(
    file: UploadFile,
    services: AppServices = Depends(get_services),
) -> dict:
    """
    Upload a .py file, install any declared pip requirements, copy to the
    custom blocks directory, and hot-reload the block registry.
    """
    filename = file.filename or "custom_block.py"
    content = await file.read()

    result = services.custom_block_manager.install(filename, content)

    if result.success:
        # Hot-reload registry so the new block appears immediately
        services.registry.reload_custom_blocks()

    return {
        "success": result.success,
        "block_name": result.block_name,
        "filename": result.filename,
        "installed_packages": result.installed_packages,
        "skipped_packages": result.skipped_packages,
        "errors": result.errors,
        "message": result.message,
    }


@router.delete("/{filename}")
def delete_custom_block(
    filename: str,
    services: AppServices = Depends(get_services),
) -> dict:
    """Remove a custom block file and reload the registry."""
    deleted = services.custom_block_manager.delete(filename)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Custom block not found: {filename}")
    services.registry.reload_custom_blocks()
    return {"deleted": filename}


@router.get("/template")
def download_template(name: str = "My Custom Block") -> Response:
    """Download a filled-in block template .py file."""
    source = get_template(block_name=name)
    safe_stem = "".join(c if c.isalnum() or c in "-_" else "_" for c in name)
    filename = f"{safe_stem}.py"
    return Response(
        content=source.encode("utf-8"),
        media_type="text/x-python",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
        },
    )


@router.get("/{filename}/export")
def export_custom_block(
    filename: str,
    services: AppServices = Depends(get_services),
) -> Response:
    """Download the source of an installed custom block as a .py file."""
    try:
        content = services.custom_block_manager.export(filename)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"Custom block not found: {filename}")
    return Response(
        content=content,
        media_type="text/x-python",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
        },
    )
