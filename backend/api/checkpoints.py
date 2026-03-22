from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse
import pandas as pd

from backend.api.deps import get_services
from backend.schemas import CheckpointPreviewModel
from backend.services import AppServices

router = APIRouter(prefix="/checkpoints", tags=["checkpoints"])


def _normalize_index_labels(index: pd.Index, existing_columns: list[str]) -> list[str]:
    if isinstance(index, pd.MultiIndex):
        raw = [
            str(name) if name is not None and str(name).strip() else f"index_{i}"
            for i, name in enumerate(index.names)
        ]
    else:
        raw_name = index.name
        raw = [str(raw_name) if raw_name is not None and str(raw_name).strip() else "index"]

    taken = {str(col) for col in existing_columns}
    labels: list[str] = []
    for i, base in enumerate(raw):
        candidate = base
        if candidate in taken or candidate in labels:
            suffix = 1
            while f"{base}_{suffix}" in taken or f"{base}_{suffix}" in labels:
                suffix += 1
            candidate = f"{base}_{suffix}"
        labels.append(candidate)
        taken.add(candidate)
    return labels


@router.get("/{checkpoint_id}/preview", response_model=CheckpointPreviewModel)
def get_checkpoint_preview(
    checkpoint_id: str,
    limit: int = Query(default=50, ge=1, le=500),
    output_handle: str = Query(default="output_0"),
    services: AppServices = Depends(get_services),
) -> CheckpointPreviewModel:
    try:
        data = services.checkpoint_store.load_output(checkpoint_id, output_handle)
    except FileNotFoundError as exc:
        raise HTTPException(
            status_code=404,
            detail=f"Output handle '{output_handle}' not found for checkpoint: {checkpoint_id}",
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=404, detail=f"Checkpoint not found: {checkpoint_id}"
        ) from exc

    preview = data.head(limit)
    index_labels = _normalize_index_labels(preview.index, list(preview.columns))
    preview_with_index = preview.reset_index()
    renamed = list(preview_with_index.columns)
    for idx, label in enumerate(index_labels):
        renamed[idx] = label
    preview_with_index.columns = renamed

    return CheckpointPreviewModel(
        checkpoint_id=checkpoint_id,
        rows=preview_with_index.to_dict(orient="records"),  # pyright: ignore[reportArgumentType]
        columns=list(preview_with_index.columns),
        dtypes={
            column: str(dtype) for column, dtype in preview_with_index.dtypes.items()
        },  # pyright: ignore[reportArgumentType]
        total_rows=int(data.shape[0]),
    )


@router.get("/{checkpoint_id}/provenance")
def get_checkpoint_provenance(
    checkpoint_id: str,
    services: AppServices = Depends(get_services),
) -> dict:
    provenance_path = (
        Path(services.settings.checkpoint_dir) / checkpoint_id / "provenance.json"
    )
    if not provenance_path.exists():
        raise HTTPException(
            status_code=404, detail=f"Provenance not found: {checkpoint_id}"
        )
    import json

    return json.loads(provenance_path.read_text(encoding="utf-8"))


@router.get("/{checkpoint_id}/images/{filename}")
def get_checkpoint_image(
    checkpoint_id: str,
    filename: str,
    services: AppServices = Depends(get_services),
) -> FileResponse:
    safe_name = Path(filename).name
    image_path = (
        Path(services.settings.checkpoint_dir) / checkpoint_id / "images" / safe_name
    )
    if not image_path.exists():
        raise HTTPException(status_code=404, detail=f"Image not found: {safe_name}")
    return FileResponse(path=image_path)
