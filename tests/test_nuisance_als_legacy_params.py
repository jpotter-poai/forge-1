from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pandas as pd

from backend.engine.checkpoint_store import CheckpointStore
from backend.engine.runner import PipelineRunner
from backend.registry import BlockRegistry
from blocks.factorization import NuisanceALS


def _build_runner(tmp_path: Path) -> PipelineRunner:
    registry = BlockRegistry(blocks_dir="blocks", package_name="blocks")
    registry.discover(force_reload=True)
    store = CheckpointStore(tmp_path / "checkpoints")
    return PipelineRunner(registry=registry, checkpoint_store=store)


def _write_csv(path: Path, frame: pd.DataFrame) -> None:
    frame.to_csv(path, index=False)


def _scalar_df(value: object) -> pd.DataFrame:
    return pd.DataFrame({"value": [value]})


def test_nuisance_als_legacy_params_are_ignored_and_inputs_drive_fit() -> None:
    matrix = pd.DataFrame(
        [[1.0, 2.0, 3.0], [2.5, 1.5, 0.5]],
        index=["A1", "A2"],
        columns=["T1", "T2", "T3"],
    )
    weights = pd.DataFrame(
        1.0,
        index=matrix.index,
        columns=matrix.columns,
    )

    params = NuisanceALS.Params.model_validate(
        {
            "n_iters": 2,
            "seeds": "0,1",
            "bias_weight_mode": "binary",
            "row_bias_column": "row_bias",
            "output_prefix": "program_",
            "use_best_from_sweep": False,
            "k": 25,
            "lambda_value": 99,
        }
    )

    out = NuisanceALS().execute([matrix, weights, _scalar_df(2), _scalar_df(1.0)], params)

    assert out.metadata["k"] == 2
    assert out.metadata["lambda"] == 1.0
    assert "row_bias" in out.outputs["output_0"].columns
    assert out.outputs["output_1"].shape == (3, 2)


def test_runner_history_hash_ignores_legacy_nuisance_als_params(tmp_path: Path) -> None:
    csv_path = tmp_path / "matrix.csv"
    _write_csv(
        csv_path,
        pd.DataFrame(
            {
                "T1": [1.0, 2.0],
                "T2": [3.0, 4.0],
                "T3": [5.0, 6.0],
            }
        ),
    )

    pipeline = {
        "name": "nuisance-als-legacy-params",
        "nodes": [
            {"id": "matrix", "block": "LoadCSV", "params": {"filepath": str(csv_path)}},
            {"id": "weights", "block": "LoadCSV", "params": {"filepath": str(csv_path)}},
            {"id": "k", "block": "Constant", "params": {"value": "2", "value_type": "int"}},
            {"id": "lambda", "block": "Constant", "params": {"value": "1.0", "value_type": "float"}},
            {
                "id": "nuisance",
                "block": "NuisanceALSConsensus",
                "params": {
                    "n_iters": 2,
                    "seeds": "0,1",
                    "bias_weight_mode": "binary",
                    "row_bias_column": "row_bias",
                    "output_prefix": "program_",
                },
            },
        ],
        "edges": [
            {"source": "matrix", "target": "nuisance", "target_input": 0},
            {"source": "weights", "target": "nuisance", "target_input": 1},
            {"source": "k", "target": "nuisance", "target_input": 2},
            {"source": "lambda", "target": "nuisance", "target_input": 3},
        ],
    }

    legacy_pipeline = deepcopy(pipeline)
    legacy_pipeline["nodes"][4]["params"].update(
        {
            "use_best_from_sweep": False,
            "k": 25,
            "lambda_value": 1,
        }
    )

    runner = _build_runner(tmp_path)
    base_hashes = runner.compute_history_hashes(pipeline)
    legacy_hashes = runner.compute_history_hashes(legacy_pipeline)

    assert base_hashes["nuisance"] == legacy_hashes["nuisance"]
