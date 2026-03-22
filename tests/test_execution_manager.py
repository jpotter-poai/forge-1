from __future__ import annotations

from pathlib import Path
import time

import pytest

from backend.engine.execution_manager import ExecutionManager
from backend.settings import Settings


def _slow_worker(
    settings: Settings,
    pipeline_id: str,
    pipeline: dict,
    run_id: str,
    event_queue,
) -> None:
    del settings, pipeline_id, pipeline, run_id
    event_queue.put(
        {"kind": "event", "payload": {"type": "run_status", "status": "started"}}
    )
    time.sleep(30.0)


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        checkpoint_dir=str(tmp_path / "checkpoints"),
        pipeline_dir=str(tmp_path / "pipelines"),
        blocks_dir="blocks",
        cors_origins=["http://localhost:5173"],
    )


def test_cancel_pipeline_force_terminates_running_process(tmp_path: Path) -> None:
    manager = ExecutionManager(_settings(tmp_path), worker_target=_slow_worker)
    run = manager.start_execution("pipe-a", {"name": "x", "nodes": [], "edges": []})

    assert run.process.is_alive()
    assert manager.cancel_pipeline("pipe-a") is True

    # Wait briefly for process teardown to settle.
    for _ in range(20):
        if not run.process.is_alive():
            break
        time.sleep(0.05)

    assert not run.process.is_alive()
    assert manager.is_cancel_requested(run.run_id) is True

    manager.finalize_run(run.run_id)
    assert manager.cancel_pipeline("pipe-a") is False


def test_start_execution_rejects_second_run_for_same_pipeline(tmp_path: Path) -> None:
    manager = ExecutionManager(_settings(tmp_path), worker_target=_slow_worker)
    run = manager.start_execution("pipe-a", {"name": "x", "nodes": [], "edges": []})
    try:
        with pytest.raises(RuntimeError):
            manager.start_execution("pipe-a", {"name": "x", "nodes": [], "edges": []})
    finally:
        manager.cancel_run(run.run_id)
        manager.finalize_run(run.run_id)
