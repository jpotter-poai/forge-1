from __future__ import annotations

import io
import json
from pathlib import Path
import subprocess
import sys
import zipfile

import pandas as pd
from fastapi.testclient import TestClient

from backend.main import create_app
from backend.settings import Settings


def _write_csv(path: Path, rows: list[dict]) -> None:
    pd.DataFrame(rows).to_csv(path, index=False)


def _build_client(tmp_path: Path) -> TestClient:
    settings = Settings(
        checkpoint_dir=str(tmp_path / "checkpoints"),
        pipeline_dir=str(tmp_path / "pipelines"),
        blocks_dir="blocks",
        cors_origins=["http://localhost:5173"],
    )
    app = create_app(settings)
    return TestClient(app)


def test_blocks_endpoint_lists_registered_blocks(tmp_path: Path) -> None:
    with _build_client(tmp_path) as client:
        resp = client.get("/api/blocks")
        assert resp.status_code == 200
        blocks = resp.json()
        keys = {item["key"] for item in blocks}
        assert "LoadCSV" in keys
        assert "MedianCenterRows" in keys

        by_key = {item["key"]: item for item in blocks}
        load_csv_schema = {
            field["key"]: field for field in by_key["LoadCSV"]["param_schema"]
        }
        export_csv_schema = {
            field["key"]: field for field in by_key["ExportCSV"]["param_schema"]
        }
        scatter_schema = {
            field["key"]: field
            for field in by_key["MatrixScatterPlot"]["param_schema"]
        }

        assert load_csv_schema["filepath"]["browse_mode"] == "open_file"
        assert load_csv_schema["sep"]["browse_mode"] is None
        assert export_csv_schema["filepath"]["browse_mode"] == "save_file"
        assert scatter_schema["export_dir"]["browse_mode"] == "directory"


def test_file_browser_uses_default_file_path_when_no_path_is_provided(tmp_path: Path) -> None:
    browse_root = tmp_path / "browse_root"
    browse_root.mkdir()
    seeded_file = browse_root / "seed.csv"
    seeded_file.write_text("id,value\n1,10\n", encoding="utf-8")

    settings = Settings(
        checkpoint_dir=str(tmp_path / "checkpoints"),
        pipeline_dir=str(tmp_path / "pipelines"),
        blocks_dir="blocks",
        default_file_path=str(seeded_file),
        cors_origins=["http://localhost:5173"],
    )

    with TestClient(create_app(settings)) as client:
        resp = client.get("/api/files/browse")

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["current"] == str(browse_root.resolve())
    assert any(entry["name"] == "seed.csv" for entry in payload["entries"])


def test_pipeline_crud_execute_staleness_and_preview(tmp_path: Path) -> None:
    data_path = tmp_path / "input.csv"
    _write_csv(
        data_path,
        [
            {"id": 1, "value": 10},
            {"id": 2, "value": 20},
            {"id": 3, "value": 30},
        ],
    )

    pipeline_payload = {
        "name": "API Pipeline",
        "nodes": [
            {"id": "load", "block": "LoadCSV", "params": {"filepath": str(data_path)}},
            {
                "id": "filter",
                "block": "FilterRows",
                "params": {"column": "value", "operator": "gt", "value": 15},
            },
            {"id": "center", "block": "MedianCenterRows", "params": {}},
        ],
        "edges": [
            {"source": "load", "target": "filter"},
            {"source": "filter", "target": "center"},
        ],
    }

    with _build_client(tmp_path) as client:
        create_resp = client.post("/api/pipelines", json=pipeline_payload)
        assert create_resp.status_code == 201
        created = create_resp.json()
        pipeline_id = created["id"]

        list_resp = client.get("/api/pipelines")
        assert list_resp.status_code == 200
        assert any(item["id"] == pipeline_id for item in list_resp.json())

        stale_before = client.get(f"/api/pipelines/{pipeline_id}/staleness")
        assert stale_before.status_code == 200
        assert stale_before.json()["stale"]["load"] is True

        execute_resp = client.post(f"/api/pipelines/{pipeline_id}/execute")
        assert execute_resp.status_code == 200
        execute_payload = execute_resp.json()
        assert execute_payload["executed_nodes"] == ["load", "filter", "center"]

        stale_after = client.get(f"/api/pipelines/{pipeline_id}/staleness")
        assert stale_after.status_code == 200
        assert stale_after.json()["stale"]["load"] is False
        assert stale_after.json()["stale"]["filter"] is False
        assert stale_after.json()["stale"]["center"] is False

        center_checkpoint_id = execute_payload["node_results"]["center"]["checkpoint_id"]
        preview_resp = client.get(f"/api/checkpoints/{center_checkpoint_id}/preview")
        assert preview_resp.status_code == 200
        preview_payload = preview_resp.json()
        assert preview_payload["checkpoint_id"] == center_checkpoint_id
        assert preview_payload["total_rows"] == 2

        updated = dict(pipeline_payload)
        updated["nodes"] = [dict(node) for node in pipeline_payload["nodes"]]
        updated["nodes"][1] = dict(updated["nodes"][1])
        updated["nodes"][1]["params"] = dict(updated["nodes"][1]["params"])
        updated["nodes"][1]["params"]["value"] = 25
        update_resp = client.put(f"/api/pipelines/{pipeline_id}", json=updated)
        assert update_resp.status_code == 200

        stale_changed = client.get(f"/api/pipelines/{pipeline_id}/staleness")
        assert stale_changed.status_code == 200
        stale_payload = stale_changed.json()["stale"]
        assert stale_payload["load"] is False
        assert stale_payload["filter"] is True
        assert stale_payload["center"] is True

        delete_resp = client.delete(f"/api/pipelines/{pipeline_id}")
        assert delete_resp.status_code == 204
        missing_resp = client.get(f"/api/pipelines/{pipeline_id}")
        assert missing_resp.status_code == 404


def test_execute_websocket_streams_node_status_and_result(tmp_path: Path) -> None:
    data_path = tmp_path / "input.csv"
    _write_csv(
        data_path,
        [
            {"id": 1, "value": 10},
            {"id": 2, "value": 20},
        ],
    )

    pipeline_payload = {
        "name": "WS Pipeline",
        "nodes": [
            {"id": "load", "block": "LoadCSV", "params": {"filepath": str(data_path)}},
            {"id": "center", "block": "MedianCenterRows", "params": {}},
        ],
        "edges": [{"source": "load", "target": "center"}],
    }

    with _build_client(tmp_path) as client:
        create_resp = client.post("/api/pipelines", json=pipeline_payload)
        assert create_resp.status_code == 201
        pipeline_id = create_resp.json()["id"]

        messages: list[dict] = []
        with client.websocket_connect(f"/api/ws/execute/{pipeline_id}") as ws:
            for _ in range(30):
                message = ws.receive_json()
                messages.append(message)
                if message.get("type") == "run_status" and message.get("status") == "complete":
                    break

        assert any(msg.get("type") == "node_status" and msg.get("node_id") == "load" for msg in messages)
        assert any(msg.get("type") == "node_status" and msg.get("node_id") == "center" for msg in messages)
        assert any(msg.get("type") == "run_result" for msg in messages)


def test_cancel_endpoint_returns_not_running_when_no_active_execution(tmp_path: Path) -> None:
    with _build_client(tmp_path) as client:
        resp = client.post("/api/pipelines/missing/cancel")
        assert resp.status_code == 200
        payload = resp.json()
        assert payload["pipeline_id"] == "missing"
        assert payload["status"] == "not_running"


def test_checkpoint_preview_includes_index_values(tmp_path: Path) -> None:
    data_path = tmp_path / "pivot_input.csv"
    _write_csv(
        data_path,
        [
            {"record_id": "A1", "col_key": "Group1__Step1", "value": 0.1},
            {"record_id": "A1", "col_key": "Group2__Step1", "value": 0.2},
            {"record_id": "A2", "col_key": "Group1__Step1", "value": 0.3},
            {"record_id": "A2", "col_key": "Group2__Step1", "value": 0.4},
        ],
    )

    pipeline_payload = {
        "name": "Pivot Preview Index",
        "nodes": [
            {"id": "load", "block": "LoadCSV", "params": {"filepath": str(data_path)}},
            {
                "id": "pivot",
                "block": "PivotTable",
                "params": {
                    "index": "record_id",
                    "columns": "col_key",
                    "values": "value",
                    "aggfunc": "mean",
                },
            },
        ],
        "edges": [{"source": "load", "target": "pivot"}],
    }

    with _build_client(tmp_path) as client:
        create_resp = client.post("/api/pipelines", json=pipeline_payload)
        assert create_resp.status_code == 201
        pipeline_id = create_resp.json()["id"]

        execute_resp = client.post(f"/api/pipelines/{pipeline_id}/execute")
        assert execute_resp.status_code == 200
        checkpoint_id = execute_resp.json()["node_results"]["pivot"]["checkpoint_id"]

        preview_resp = client.get(f"/api/checkpoints/{checkpoint_id}/preview")
        assert preview_resp.status_code == 200
        preview = preview_resp.json()

        assert "record_id" in preview["columns"]
        record_ids = [row["record_id"] for row in preview["rows"]]
        assert record_ids == ["A1", "A2"]


def test_export_pipeline_python_bundle_contains_runtime_inputs_and_rewritten_paths(
    tmp_path: Path,
) -> None:
    data_path = tmp_path / "export_input.csv"
    _write_csv(
        data_path,
        [
            {"id": 1, "value": 10},
            {"id": 2, "value": 20},
        ],
    )

    pipeline_payload = {
        "name": "Export API Pipeline",
        "nodes": [
            {
                "id": "load",
                "block": "LoadCSV",
                "params": {"filepath": str(data_path)},
                "group_ids": ["comment_group"],
            },
            {
                "id": "center",
                "block": "MedianCenterRows",
                "params": {},
                "group_ids": ["comment_group"],
            },
            {
                "id": "export",
                "block": "ExportCSV",
                "params": {
                    "filepath": str(tmp_path / "original_outputs" / "result.csv"),
                    "index": False,
                },
            },
        ],
        "edges": [
            {"source": "load", "target": "center"},
            {"source": "center", "target": "export"},
        ],
        "comments": [
            {
                "id": "comment_1",
                "title": "Load and center",
                "description": "This section prepares the matrix.",
                "position": {"x": 0, "y": 0},
                "width": 320,
                "height": 180,
                "group_id": "comment_group",
            }
        ],
        "groups": [
            {
                "id": "comment_group",
                "name": "Load and center",
                "description": "This section prepares the matrix.",
                "comment_id": "comment_1",
            }
        ],
    }

    with _build_client(tmp_path) as client:
        create_resp = client.post("/api/pipelines", json=pipeline_payload)
        assert create_resp.status_code == 201
        pipeline_id = create_resp.json()["id"]

        export_resp = client.get(f"/api/pipelines/{pipeline_id}/export", params={"format": "python"})
        assert export_resp.status_code == 200
        assert export_resp.headers["content-type"] == "application/zip"

    with zipfile.ZipFile(io.BytesIO(export_resp.content)) as archive:
        names = set(archive.namelist())
        root = "export_api_pipeline_python"

        assert f"{root}/export_api_pipeline.py" in names
        assert f"{root}/export_api_pipeline.pipeline.json" in names
        assert f"{root}/Forge/export_runtime.py" in names
        assert f"{root}/backend/engine/runner.py" in names
        assert f"{root}/blocks/io.py" in names
        assert f"{root}/inputs/load/export_input.csv" in names

        script_text = archive.read(f"{root}/export_api_pipeline.py").decode("utf-8")
        assert "# Load and center" in script_text
        assert script_text.index("# Load and center") < script_text.index("# load: Load CSV")
        assert 'str(ROOT_DIR / "inputs" / "load" / "export_input.csv")' in script_text

        exported_pipeline = json.loads(
            archive.read(f"{root}/export_api_pipeline.pipeline.json").decode("utf-8")
        )
        load_node = next(node for node in exported_pipeline["nodes"] if node["id"] == "load")
        export_node = next(node for node in exported_pipeline["nodes"] if node["id"] == "export")
        assert load_node["params"]["filepath"] == "inputs/load/export_input.csv"
        assert export_node["params"]["filepath"] == "outputs/exports/export/result.csv"


def test_exported_python_bundle_runs_end_to_end(tmp_path: Path) -> None:
    data_path = tmp_path / "run_export_input.csv"
    _write_csv(
        data_path,
        [
            {"id": 1, "value": 10},
            {"id": 2, "value": 30},
            {"id": 3, "value": 50},
        ],
    )

    pipeline_payload = {
        "name": "Runnable Export",
        "nodes": [
            {"id": "load", "block": "LoadCSV", "params": {"filepath": str(data_path)}},
            {
                "id": "filter",
                "block": "FilterRows",
                "params": {"column": "value", "operator": "gt", "value": 20},
            },
            {
                "id": "export",
                "block": "ExportCSV",
                "params": {
                    "filepath": str(tmp_path / "ignored" / "filtered.csv"),
                    "index": False,
                },
            },
        ],
        "edges": [
            {"source": "load", "target": "filter"},
            {"source": "filter", "target": "export"},
        ],
    }

    with _build_client(tmp_path) as client:
        create_resp = client.post("/api/pipelines", json=pipeline_payload)
        assert create_resp.status_code == 201
        pipeline_id = create_resp.json()["id"]

        export_resp = client.get(f"/api/pipelines/{pipeline_id}/export", params={"format": "python"})
        assert export_resp.status_code == 200

    extract_root = tmp_path / "bundle"
    with zipfile.ZipFile(io.BytesIO(export_resp.content)) as archive:
        archive.extractall(extract_root)

    bundle_dir = extract_root / "runnable_export_python"
    script_path = bundle_dir / "runnable_export.py"
    run = subprocess.run(
        [sys.executable, script_path.name],
        cwd=bundle_dir,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )

    assert run.returncode == 0, run.stderr
    assert (bundle_dir / "outputs" / "run_summary.json").exists()
    exported_csv = bundle_dir / "outputs" / "exports" / "export" / "filtered.csv"
    assert exported_csv.exists()

    exported_rows = pd.read_csv(exported_csv).to_dict(orient="records")
    assert exported_rows == [
        {"id": 2, "value": 30},
        {"id": 3, "value": 50},
    ]


def test_export_pipeline_notebook_bundle_contains_notebook_cells(tmp_path: Path) -> None:
    data_path = tmp_path / "notebook_input.csv"
    _write_csv(
        data_path,
        [
            {"id": 1, "value": 1},
            {"id": 2, "value": 2},
        ],
    )

    pipeline_payload = {
        "name": "Notebook Export",
        "nodes": [
            {
                "id": "load",
                "block": "LoadCSV",
                "params": {"filepath": str(data_path)},
                "group_ids": ["comment_group"],
            },
            {
                "id": "center",
                "block": "MedianCenterRows",
                "params": {},
                "group_ids": ["comment_group"],
            },
        ],
        "edges": [{"source": "load", "target": "center"}],
        "comments": [
            {
                "id": "comment_1",
                "title": "Notebook comment",
                "description": "Appears before the first grouped block.",
                "position": {"x": 0, "y": 0},
                "width": 300,
                "height": 140,
                "group_id": "comment_group",
            }
        ],
        "groups": [
            {
                "id": "comment_group",
                "name": "Notebook comment",
                "description": "Appears before the first grouped block.",
                "comment_id": "comment_1",
            }
        ],
    }

    with _build_client(tmp_path) as client:
        create_resp = client.post("/api/pipelines", json=pipeline_payload)
        assert create_resp.status_code == 201
        pipeline_id = create_resp.json()["id"]

        export_resp = client.get(f"/api/pipelines/{pipeline_id}/export", params={"format": "notebook"})
        assert export_resp.status_code == 200

    with zipfile.ZipFile(io.BytesIO(export_resp.content)) as archive:
        notebook = json.loads(
            archive.read("notebook_export_notebook/notebook_export.ipynb").decode("utf-8")
        )

    assert notebook["nbformat"] == 4
    assert notebook["cells"][0]["cell_type"] == "markdown"
    pipeline_cells = [
        "".join(cell.get("source", []))
        for cell in notebook["cells"]
        if cell["cell_type"] == "code"
    ]
    first_block_cell = next(cell for cell in pipeline_cells if "# load: Load CSV" in cell)
    assert first_block_cell.index("# Notebook comment") < first_block_cell.index("# load: Load CSV")


def test_export_pipeline_skips_incomplete_nodes_like_native_runner(tmp_path: Path) -> None:
    data_path = tmp_path / "skip_input.csv"
    _write_csv(
        data_path,
        [
            {"id": 1, "value": 1},
            {"id": 2, "value": 2},
        ],
    )

    pipeline_payload = {
        "name": "Skip Incomplete Export",
        "nodes": [
            {"id": "load", "block": "LoadCSV", "params": {"filepath": str(data_path)}},
            {
                "id": "export_unwired",
                "block": "ExportCSV",
                "params": {
                    "filepath": str(tmp_path / "ignored" / "unwired.csv"),
                    "index": False,
                },
            },
        ],
        "edges": [],
    }

    with _build_client(tmp_path) as client:
        create_resp = client.post("/api/pipelines", json=pipeline_payload)
        assert create_resp.status_code == 201
        pipeline_id = create_resp.json()["id"]

        export_resp = client.get(f"/api/pipelines/{pipeline_id}/export", params={"format": "python"})
        assert export_resp.status_code == 200

    with zipfile.ZipFile(io.BytesIO(export_resp.content)) as archive:
        script_text = archive.read(
            "skip_incomplete_export_python/skip_incomplete_export.py"
        ).decode("utf-8")

    assert "# Skipped export_unwired: Export CSV" in script_text
    assert "expects 1 input(s) and only 0 edge(s) are connected" in script_text
    assert 'node_id="export_unwired"' not in script_text
