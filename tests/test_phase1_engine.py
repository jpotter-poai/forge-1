from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from backend.block import BaseBlock, BlockOutput
from backend.engine.checkpoint_store import CheckpointStore
from backend.engine.provenance import compute_history_hash
from backend.engine.runner import PipelineRunner
from backend.registry import BlockRegistry


def _build_runner(tmp_path: Path) -> PipelineRunner:
    registry = BlockRegistry(blocks_dir="blocks", package_name="blocks")
    registry.discover(force_reload=True)
    store = CheckpointStore(tmp_path / "checkpoints")
    return PipelineRunner(registry=registry, checkpoint_store=store)


def _write_csv(path: Path, rows: list[dict]) -> None:
    frame = pd.DataFrame(rows)
    frame.to_csv(path, index=False)


class SplitDualOutputBlock(BaseBlock):
    name = "Split Dual Output"
    version = "1.0.0"
    category = "Test"
    output_labels = ["Base", "Scaled"]

    def execute(self, data: pd.DataFrame, params: object | None = None) -> BlockOutput:
        base = data[["value"]].copy()
        scaled = base.copy()
        scaled["value"] = scaled["value"] * 10
        return BlockOutput(
            data=base,
            outputs={"output_0": base, "output_1": scaled},
        )


class ConsumeValueBlock(BaseBlock):
    name = "Consume Value"
    version = "1.0.0"
    category = "Test"

    def execute(self, data: pd.DataFrame, params: object | None = None) -> BlockOutput:
        value = float(data["value"].iloc[0])
        return BlockOutput(data=pd.DataFrame([{"value": value}]))


def _register_test_blocks(runner: PipelineRunner) -> None:
    runner.registry._blocks["SplitDualOutputBlock"] = SplitDualOutputBlock
    runner.registry._blocks["ConsumeValueBlock"] = ConsumeValueBlock


def test_registry_resolves_legacy_display_name_aliases(tmp_path: Path) -> None:
    runner = _build_runner(tmp_path)
    assert runner.registry.get("Nuisance ALS Residual Matrix").__name__ == "NuisanceALS"
    assert runner.registry.get("NuisanceALSConsensus").__name__ == "NuisanceALS"
    assert runner.registry.get("NuisanceALSResidualMatrix").__name__ == "NuisanceALS"


def test_provenance_hash_is_deterministic() -> None:
    params_a = {"b": 2, "a": 1}
    params_b = {"a": 1, "b": 2}
    hash_a = compute_history_hash("sha256:parent", "TestBlock", "1.0.0", params_a)
    hash_b = compute_history_hash("sha256:parent", "TestBlock", "1.0.0", params_b)

    assert hash_a == hash_b
    assert hash_a != compute_history_hash(
        "sha256:parent",
        "TestBlock",
        "1.0.0",
        {"a": 1, "b": 3},
    )


def test_param_change_invalidates_target_and_descendants(tmp_path: Path) -> None:
    csv_path = tmp_path / "input.csv"
    _write_csv(
        csv_path,
        [
            {"id": 1, "value": 10},
            {"id": 2, "value": 20},
            {"id": 3, "value": 30},
        ],
    )

    pipeline = {
        "name": "param-change",
        "nodes": [
            {"id": "load", "block": "LoadCSV", "params": {"filepath": str(csv_path)}},
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

    runner = _build_runner(tmp_path)
    first = runner.run_pipeline(pipeline)
    assert first.executed_nodes == ["load", "filter", "center"]

    updated = json.loads(json.dumps(pipeline))
    updated["nodes"][1]["params"]["value"] = 25
    second = runner.run_pipeline(updated)

    assert second.node_results["load"].status == "reused"
    assert second.node_results["filter"].status == "executed"
    assert second.node_results["center"].status == "executed"


def test_block_version_change_invalidates_dependents(tmp_path: Path, monkeypatch) -> None:
    csv_path = tmp_path / "input.csv"
    _write_csv(
        csv_path,
        [
            {"id": 1, "value": 10},
            {"id": 2, "value": 20},
            {"id": 3, "value": 30},
        ],
    )

    pipeline = {
        "name": "version-change",
        "nodes": [
            {"id": "load", "block": "LoadCSV", "params": {"filepath": str(csv_path)}},
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

    runner = _build_runner(tmp_path)
    runner.run_pipeline(pipeline)

    filter_cls = runner.registry.get("FilterRows")
    original_version = filter_cls.version
    monkeypatch.setattr(filter_cls, "version", "2.0.0-test")
    second = runner.run_pipeline(pipeline)

    assert second.node_results["load"].status == "reused"
    assert second.node_results["filter"].status == "executed"
    assert second.node_results["center"].status == "executed"
    assert original_version != filter_cls.version


def test_checkpoint_reuse_skips_execution(tmp_path: Path) -> None:
    csv_path = tmp_path / "input.csv"
    _write_csv(
        csv_path,
        [
            {"id": 1, "value": 10},
            {"id": 2, "value": 20},
        ],
    )

    pipeline = {
        "name": "reuse",
        "nodes": [
            {"id": "load", "block": "LoadCSV", "params": {"filepath": str(csv_path)}},
            {"id": "center", "block": "MedianCenterRows", "params": {}},
        ],
        "edges": [{"source": "load", "target": "center"}],
    }

    runner = _build_runner(tmp_path)
    first = runner.run_pipeline(pipeline)
    second = runner.run_pipeline(pipeline)

    assert first.executed_nodes == ["load", "center"]
    assert second.executed_nodes == []
    assert second.reused_nodes == ["load", "center"]


def test_checkpoint_reuse_preserves_dataframe_index(tmp_path: Path) -> None:
    csv_path = tmp_path / "input.csv"
    _write_csv(
        csv_path,
        [
            {"id": 1, "value": 10},
            {"id": 2, "value": 20},
            {"id": 3, "value": 30},
        ],
    )

    pipeline = {
        "name": "reuse-index",
        "nodes": [
            {"id": "load", "block": "LoadCSV", "params": {"filepath": str(csv_path)}},
            {
                "id": "filter",
                "block": "FilterRows",
                "params": {"column": "value", "operator": "gt", "value": 15},
            },
        ],
        "edges": [{"source": "load", "target": "filter"}],
    }

    runner = _build_runner(tmp_path)
    first = runner.run_pipeline(pipeline)
    first_data = runner.checkpoint_store.load_data(first.node_results["filter"].checkpoint_id)

    # FilterRows should preserve original row labels [1, 2] from CSV row order.
    assert first_data.index.tolist() == [1, 2]

    second = runner.run_pipeline(pipeline)
    second_data = runner.checkpoint_store.load_data(second.node_results["filter"].checkpoint_id)
    assert second.node_results["filter"].status == "reused"
    assert second_data.index.tolist() == [1, 2]


def test_export_csv_always_executes_even_when_checkpoint_exists(tmp_path: Path) -> None:
    csv_path = tmp_path / "input.csv"
    export_path = tmp_path / "exports" / "output.csv"
    _write_csv(
        csv_path,
        [
            {"id": 1, "value": 10},
            {"id": 2, "value": 20},
        ],
    )

    pipeline = {
        "name": "always-execute-export",
        "nodes": [
            {"id": "load", "block": "LoadCSV", "params": {"filepath": str(csv_path)}},
            {
                "id": "export",
                "block": "ExportCSV",
                "params": {"filepath": str(export_path), "index": False},
            },
        ],
        "edges": [{"source": "load", "target": "export"}],
    }

    runner = _build_runner(tmp_path)
    first = runner.run_pipeline(pipeline)
    assert first.executed_nodes == ["load", "export"]
    assert export_path.exists()

    export_path.unlink()
    assert not export_path.exists()

    second = runner.run_pipeline(pipeline)
    assert second.node_results["load"].status == "reused"
    assert second.node_results["export"].status == "executed"
    assert second.executed_nodes == ["export"]
    assert export_path.exists()

    exported = pd.read_csv(export_path)
    assert exported.to_dict(orient="records") == [
        {"id": 1, "value": 10},
        {"id": 2, "value": 20},
    ]


def test_export_enabled_visualization_executes_even_when_checkpoint_exists(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "input.csv"
    export_dir = tmp_path / "exports"
    export_path = export_dir / "Histogram.png"
    _write_csv(
        csv_path,
        [
            {"id": 1, "value": 10},
            {"id": 2, "value": 20},
            {"id": 3, "value": 30},
        ],
    )

    pipeline = {
        "name": "always-execute-visualization-export",
        "nodes": [
            {"id": "load", "block": "LoadCSV", "params": {"filepath": str(csv_path)}},
            {
                "id": "hist",
                "block": "MatrixHistogram",
                "params": {
                    "column_name": "value",
                    "plot_title": "Histogram",
                    "export_enabled": True,
                    "export_dir": str(export_dir),
                },
            },
        ],
        "edges": [{"source": "load", "target": "hist"}],
    }

    runner = _build_runner(tmp_path)
    first = runner.run_pipeline(pipeline)
    assert first.executed_nodes == ["load", "hist"]
    assert export_path.exists()

    export_path.unlink()
    assert not export_path.exists()

    second = runner.run_pipeline(pipeline)
    assert second.node_results["load"].status == "reused"
    assert second.node_results["hist"].status == "executed"
    assert second.executed_nodes == ["hist"]
    assert export_path.exists()


def test_multi_input_edge_ordering_controls_positional_inputs(tmp_path: Path) -> None:
    left_csv = tmp_path / "left.csv"
    right_csv = tmp_path / "right.csv"
    _write_csv(left_csv, [{"source": "left", "value": 1}])
    _write_csv(right_csv, [{"source": "right", "value": 2}])

    base_nodes = [
        {"id": "left", "block": "LoadCSV", "params": {"filepath": str(left_csv)}},
        {"id": "right", "block": "LoadCSV", "params": {"filepath": str(right_csv)}},
        {"id": "append", "block": "AppendDatasets", "params": {"ignore_index": True}},
    ]

    pipeline_a = {
        "name": "append-a",
        "nodes": base_nodes,
        "edges": [
            {"source": "left", "target": "append"},
            {"source": "right", "target": "append"},
        ],
    }
    pipeline_b = {
        "name": "append-b",
        "nodes": base_nodes,
        "edges": [
            {"source": "right", "target": "append"},
            {"source": "left", "target": "append"},
        ],
    }

    runner = _build_runner(tmp_path)
    first = runner.run_pipeline(pipeline_a)
    first_data = runner.checkpoint_store.load_data(first.node_results["append"].checkpoint_id)
    assert first_data["source"].tolist() == ["left", "right"]

    second = runner.run_pipeline(pipeline_b)
    second_data = runner.checkpoint_store.load_data(second.node_results["append"].checkpoint_id)
    assert second.node_results["append"].status == "executed"
    assert second_data["source"].tolist() == ["right", "left"]


def test_mixed_handle_and_unset_inputs_map_to_remaining_slots(tmp_path: Path) -> None:
    data_csv = tmp_path / "data.csv"
    lookup_csv = tmp_path / "lookup.csv"
    _write_csv(
        data_csv,
        [
            {"item_id": 1001, "value": 1.1, "extra": "a"},
            {"item_id": 1002, "value": 2.2, "extra": "b"},
            {"item_id": 1003, "value": 3.3, "extra": "c"},
        ],
    )
    _write_csv(
        lookup_csv,
        [
            {"item_id": 1002, "coverage": 0.3},
            {"item_id": 1003, "coverage": 0.4},
        ],
    )

    pipeline = {
        "name": "lookup-mixed-handles",
        "nodes": [
            {"id": "data", "block": "LoadCSV", "params": {"filepath": str(data_csv)}},
            {"id": "lookup", "block": "LoadCSV", "params": {"filepath": str(lookup_csv)}},
            {
                "id": "filter",
                "block": "FilterByLookupValues",
                "params": {"data_key": "item_id", "lookup_key": "item_id"},
            },
        ],
        "edges": [
            # Legacy/older payloads can have targetHandle unset for one input.
            {"source": "data", "target": "filter", "targetHandle": None},
            {"source": "lookup", "target": "filter", "targetHandle": "input_1"},
        ],
    }

    runner = _build_runner(tmp_path)
    out = runner.run_pipeline(pipeline)
    filtered = runner.checkpoint_store.load_data(out.node_results["filter"].checkpoint_id)

    # Data input must remain positional input 0 even when its targetHandle is missing.
    assert list(filtered.columns) == ["item_id", "value", "extra"]
    assert filtered["item_id"].tolist() == [1002, 1003]


def test_non_default_output_handle_routes_correct_dataframe_and_reuses_checkpoint(
    tmp_path: Path, monkeypatch
) -> None:
    csv_path = tmp_path / "input.csv"
    _write_csv(csv_path, [{"value": 2.0}])

    pipeline = {
        "name": "multi-output-reuse",
        "nodes": [
            {"id": "load", "block": "LoadCSV", "params": {"filepath": str(csv_path)}},
            {"id": "split", "block": "SplitDualOutputBlock", "params": {}},
            {"id": "consume", "block": "ConsumeValueBlock", "params": {}},
        ],
        "edges": [
            {"source": "load", "target": "split"},
            {"source": "split", "target": "consume", "sourceHandle": "output_1"},
        ],
    }

    runner = _build_runner(tmp_path)
    _register_test_blocks(runner)

    first = runner.run_pipeline(pipeline)
    first_data = runner.checkpoint_store.load_data(
        first.node_results["consume"].checkpoint_id
    )
    assert first_data["value"].tolist() == [20.0]

    monkeypatch.setattr(ConsumeValueBlock, "version", "2.0.0-test")
    second = runner.run_pipeline(pipeline)
    second_data = runner.checkpoint_store.load_data(
        second.node_results["consume"].checkpoint_id
    )
    assert second.node_results["split"].status == "reused"
    assert second.node_results["consume"].status == "executed"
    assert second_data["value"].tolist() == [20.0]


def test_source_handle_change_invalidates_descendant_hash(tmp_path: Path) -> None:
    csv_path = tmp_path / "input.csv"
    _write_csv(csv_path, [{"value": 2.0}])

    base_nodes = [
        {"id": "load", "block": "LoadCSV", "params": {"filepath": str(csv_path)}},
        {"id": "split", "block": "SplitDualOutputBlock", "params": {}},
        {"id": "consume", "block": "ConsumeValueBlock", "params": {}},
    ]
    pipeline_a = {
        "name": "multi-output-hash-a",
        "nodes": base_nodes,
        "edges": [
            {"source": "load", "target": "split"},
            {"source": "split", "target": "consume", "source_output": 0},
        ],
    }
    pipeline_b = {
        "name": "multi-output-hash-b",
        "nodes": base_nodes,
        "edges": [
            {"source": "load", "target": "split"},
            {"source": "split", "target": "consume", "source_output": 1},
        ],
    }

    runner = _build_runner(tmp_path)
    _register_test_blocks(runner)

    first = runner.run_pipeline(pipeline_a)
    first_data = runner.checkpoint_store.load_data(
        first.node_results["consume"].checkpoint_id
    )
    assert first_data["value"].tolist() == [2.0]

    second = runner.run_pipeline(pipeline_b)
    second_data = runner.checkpoint_store.load_data(
        second.node_results["consume"].checkpoint_id
    )
    assert second.node_results["split"].status == "reused"
    assert second.node_results["consume"].status == "executed"
    assert second_data["value"].tolist() == [20.0]
