from __future__ import annotations

import json
from pathlib import Path
import time

import pytest

from backend.engine.execution_manager import (
    ExecutionManager,
    FileEventQueueWriter,
    _execute_pipeline_worker,
)
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


def _settings(
    tmp_path: Path,
    *,
    custom_blocks_dir: Path | None = None,
) -> Settings:
    return Settings(
        checkpoint_dir=str(tmp_path / "checkpoints"),
        pipeline_dir=str(tmp_path / "pipelines"),
        blocks_dir="blocks",
        custom_blocks_dir=str(custom_blocks_dir or (tmp_path / "custom_blocks")),
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


def test_execute_pipeline_worker_loads_custom_blocks(tmp_path: Path) -> None:
    custom_blocks_dir = tmp_path / "custom_blocks"
    custom_blocks_dir.mkdir()
    (custom_blocks_dir / "special.py").write_text(
        """
from backend.block import BaseBlock, BlockOutput
import pandas as pd


class ReferenceNoveltyScore(BaseBlock):
    name = "ReferenceNoveltyScore"
    version = "1.0.0"
    category = "Custom"
    n_inputs = 0

    def execute(self, data, params=None):
        return BlockOutput(data=pd.DataFrame([{"score": 1.0}]))
""".strip(),
        encoding="utf-8",
    )

    event_log = tmp_path / "events.jsonl"
    pipeline = {
        "name": "custom-block-pipeline",
        "nodes": [{"id": "custom", "block": "ReferenceNoveltyScore", "params": {}}],
        "edges": [],
    }

    _execute_pipeline_worker(
        _settings(tmp_path, custom_blocks_dir=custom_blocks_dir),
        "pipe-custom",
        pipeline,
        "run-custom",
        FileEventQueueWriter(event_log),
    )

    messages = [
        json.loads(line)
        for line in event_log.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    result = next(message for message in messages if message["kind"] == "result")
    assert result["payload"]["executed_nodes"] == ["custom"]
    assert "custom" in result["payload"]["node_results"]


class _FakePopen:
    def __init__(self, *args, **kwargs) -> None:
        self.args = args
        self.kwargs = kwargs
        self.pid = 12345

    def poll(self):
        return None

    def wait(self, timeout=None):
        del timeout
        return 0

    def terminate(self) -> None:
        return None

    def kill(self) -> None:
        return None


def test_start_execution_serializes_custom_blocks_dir_for_worker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    custom_blocks_dir = tmp_path / "custom_blocks"
    custom_blocks_dir.mkdir()

    monkeypatch.setattr(
        "backend.engine.execution_manager.subprocess.Popen",
        _FakePopen,
    )

    manager = ExecutionManager(_settings(tmp_path, custom_blocks_dir=custom_blocks_dir))
    run = manager.start_execution("pipe-a", {"name": "x", "nodes": [], "edges": []})
    try:
        request_payload = json.loads(
            (run.run_dir / "request.json").read_text(encoding="utf-8")
        )
        assert request_payload["settings"]["custom_blocks_dir"] == str(custom_blocks_dir)
    finally:
        manager.finalize_run(run.run_id)
