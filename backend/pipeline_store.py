from __future__ import annotations

import json
from pathlib import Path
import re
from typing import Any

from backend.schemas import normalize_pipeline_payload


def _slugify(name: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_-]+", "_", name.strip().lower())
    cleaned = cleaned.strip("_")
    return cleaned or "pipeline"


class PipelineStore:
    def __init__(self, root_dir: str | Path) -> None:
        self.root_dir = Path(root_dir)
        self.root_dir.mkdir(parents=True, exist_ok=True)

    def _path(self, pipeline_id: str) -> Path:
        return self.root_dir / f"{pipeline_id}.json"

    def list(self) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for path in sorted(self.root_dir.glob("*.json")):
            payload = json.loads(path.read_text(encoding="utf-8"))
            items.append(
                {
                    "id": path.stem,
                    "name": payload.get("name", path.stem),
                    "path": str(path),
                    "updated_at": path.stat().st_mtime,
                }
            )
        return items

    def read(self, pipeline_id: str) -> dict[str, Any]:
        path = self._path(pipeline_id)
        if not path.exists():
            raise FileNotFoundError(f"Pipeline not found: {pipeline_id}")
        raw_payload = json.loads(path.read_text(encoding="utf-8"))
        payload = normalize_pipeline_payload(raw_payload)
        if payload != raw_payload:
            path.write_text(
                json.dumps(payload, indent=2, sort_keys=False),
                encoding="utf-8",
            )
        return payload

    def create(self, payload: dict[str, Any], pipeline_id: str | None = None) -> tuple[str, dict[str, Any]]:
        payload = normalize_pipeline_payload(payload)
        candidate = pipeline_id or _slugify(payload.get("name", "pipeline"))
        path = self._path(candidate)
        if path.exists():
            suffix = 1
            while self._path(f"{candidate}_{suffix}").exists():
                suffix += 1
            candidate = f"{candidate}_{suffix}"
            path = self._path(candidate)

        path.write_text(json.dumps(payload, indent=2, sort_keys=False), encoding="utf-8")
        return candidate, payload

    def update(self, pipeline_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        path = self._path(pipeline_id)
        if not path.exists():
            raise FileNotFoundError(f"Pipeline not found: {pipeline_id}")
        payload = normalize_pipeline_payload(payload)
        path.write_text(json.dumps(payload, indent=2, sort_keys=False), encoding="utf-8")
        return payload

    def delete(self, pipeline_id: str) -> None:
        path = self._path(pipeline_id)
        if not path.exists():
            raise FileNotFoundError(f"Pipeline not found: {pipeline_id}")
        path.unlink()
