from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from backend.custom_blocks import get_template
from backend.main import create_app
from backend.settings import Settings


def _build_client(tmp_path: Path, custom_blocks_dir: Path) -> TestClient:
    settings = Settings(
        checkpoint_dir=str(tmp_path / "checkpoints"),
        pipeline_dir=str(tmp_path / "pipelines"),
        blocks_dir="blocks",
        custom_blocks_dir=str(custom_blocks_dir),
        cors_origins=["http://localhost:5173"],
    )
    app = create_app(settings)
    return TestClient(app)


def test_custom_blocks_endpoint_exposes_plugin_metadata_and_blocks(tmp_path: Path) -> None:
    custom_blocks_dir = tmp_path / "custom_blocks"
    custom_blocks_dir.mkdir()

    (custom_blocks_dir / "special_plugin.py").write_text(
        """
REQUIREMENTS = ["scipy>=1.12"]
PLUGIN_TITLE = "Special Workbench"
PLUGIN_DESCRIPTION = "Reference scoring and key-building helpers."

from backend.block import BaseBlock, BlockOutput
import pandas as pd


class ReferenceNoveltyScore(BaseBlock):
    name = "ReferenceNoveltyScore"
    version = "1.0.0"
    category = "Special"
    n_inputs = 0

    def execute(self, data, params=None):
        return BlockOutput(data=pd.DataFrame([{"score": 1.0}]))


class BuildCompositeKey(BaseBlock):
    name = "BuildCompositeKey"
    version = "1.0.0"
    category = "Special"
    n_inputs = 0

    def execute(self, data, params=None):
        return BlockOutput(data=pd.DataFrame([{"key": "abc"}]))
""".strip(),
        encoding="utf-8",
    )

    (custom_blocks_dir / "fallback_plugin.py").write_text(
        """
from backend.block import BaseBlock, BlockOutput
import pandas as pd


class FallbackBlock(BaseBlock):
    name = "FallbackBlock"
    version = "1.0.0"
    category = "Custom"
    n_inputs = 0

    def execute(self, data, params=None):
        return BlockOutput(data=pd.DataFrame([{"value": 1}]))
""".strip(),
        encoding="utf-8",
    )

    with _build_client(tmp_path, custom_blocks_dir) as client:
        response = client.get("/api/custom-blocks")

    assert response.status_code == 200
    payload = response.json()
    by_filename = {entry["filename"]: entry for entry in payload}

    assert set(by_filename) == {"special_plugin.py", "fallback_plugin.py"}

    special = by_filename["special_plugin.py"]
    assert special["title"] == "Special Workbench"
    assert special["description"] == "Reference scoring and key-building helpers."
    assert special["requirements"] == ["scipy>=1.12"]
    assert [block["name"] for block in special["blocks"]] == [
        "BuildCompositeKey",
        "ReferenceNoveltyScore",
    ]

    fallback = by_filename["fallback_plugin.py"]
    assert fallback["title"] == "Fallback Plugin"
    assert fallback["description"] == (
        "Custom block plugin installed from fallback_plugin.py."
    )
    assert fallback["requirements"] == []
    assert [block["name"] for block in fallback["blocks"]] == ["FallbackBlock"]


def test_custom_block_template_includes_plugin_metadata_fields() -> None:
    template = get_template("My Plugin")

    assert 'PLUGIN_TITLE = "My Plugin"' in template
    assert (
        'PLUGIN_DESCRIPTION = "Describe what this plugin file provides."'
        in template
    )


def test_custom_block_install_uses_plugin_title_in_response(tmp_path: Path) -> None:
    custom_blocks_dir = tmp_path / "custom_blocks"
    custom_blocks_dir.mkdir()

    plugin_source = """
PLUGIN_TITLE = "Predictive Oncology Custom Toolkit"
PLUGIN_DESCRIPTION = "Includes proprietary blocks for POAI."

from backend.block import BaseBlock, BlockOutput
import pandas as pd


class ReferenceNoveltyScore(BaseBlock):
    name = "ReferenceNoveltyScore"
    version = "1.0.0"
    category = "Special"
    n_inputs = 0

    def execute(self, data, params=None):
        return BlockOutput(data=pd.DataFrame([{"score": 1.0}]))
""".strip()

    with _build_client(tmp_path, custom_blocks_dir) as client:
        response = client.post(
            "/api/custom-blocks/install",
            files={"file": ("poai.py", plugin_source, "text/x-python")},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["success"] is True
    assert payload["block_name"] == "Predictive Oncology Custom Toolkit"
    assert payload["message"] == "Installed 'Predictive Oncology Custom Toolkit'."
