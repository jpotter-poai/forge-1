from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "__dict__"):
        return value.__dict__
    return str(value)


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        default=_json_default,
    )


def sha256_text(text: str) -> str:
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


def sha256_bytes(payload: bytes) -> str:
    digest = hashlib.sha256(payload).hexdigest()
    return f"sha256:{digest}"


def compute_initial_data_signature(filepath: str | Path) -> str:
    path = Path(filepath)
    return sha256_bytes(path.read_bytes())


def combine_parent_history_hashes(parent_hashes: list[str]) -> str:
    if not parent_hashes:
        raise ValueError("Expected at least one parent history hash.")
    if len(parent_hashes) == 1:
        return parent_hashes[0]
    return canonical_json(parent_hashes)


def compute_history_hash(
    parent_history_hash: str,
    block_name: str,
    block_version: str,
    params: dict[str, Any] | None,
) -> str:
    payload = (
        parent_history_hash + block_name + block_version + canonical_json(params or {})
    )
    return sha256_text(payload)


def utc_now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


@dataclass(slots=True)
class Provenance:
    checkpoint_id: str
    block_name: str
    block_version: str
    params: dict[str, Any] = field(default_factory=dict)
    parent_checkpoint_ids: list[str] = field(default_factory=list)
    initial_data_signature: str | None = None
    history_hash: str = ""
    timestamp: str = field(default_factory=utc_now_iso)
    execution_time_seconds: float = 0.0
    output_shape: list[int] = field(default_factory=list)
    images: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)  # type: ignore

    def to_json(self) -> str:
        return canonical_json(self.to_dict())

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "Provenance":
        return cls(**payload)
