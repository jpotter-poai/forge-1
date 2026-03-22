from __future__ import annotations

from copy import deepcopy
from typing import Any

from backend.block import BaseBlock
from backend.registry import BlockSpec


def list_block_presets(
    spec: BlockSpec, block_cls: type[BaseBlock] | None = None
) -> list[dict[str, Any]]:
    presets = [
        {
            "id": "default",
            "label": "Default",
            "description": "The block's default parameter payload.",
            "params": deepcopy(spec.params),
        }
    ]
    raw_presets = getattr(block_cls, "presets", []) if block_cls is not None else []
    if not isinstance(raw_presets, (list, tuple)):
        return presets
    for preset in raw_presets:
        if not isinstance(preset, dict):
            continue
        preset_id = str(preset.get("id") or "").strip()
        label = str(preset.get("label") or "").strip()
        params = preset.get("params")
        if not preset_id or not label or not isinstance(params, dict):
            continue
        presets.append(
            {
                "id": preset_id,
                "label": label,
                "description": str(preset.get("description") or "").strip(),
                "params": deepcopy(params),
            }
        )
    return presets
