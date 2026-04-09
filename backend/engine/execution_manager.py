from __future__ import annotations

from collections import deque
from dataclasses import dataclass
import json
import multiprocessing as mp
import os
from pathlib import Path
import queue
import shutil
import subprocess
import sys
from threading import Lock
import time
from typing import Any, Callable
import uuid

from backend.engine.checkpoint_store import CheckpointStore
from backend.engine.runner import PipelineRunner
from backend.registry import BlockRegistry
from backend.settings import Settings

WorkerMessage = dict[str, Any]
WorkerTarget = Callable[
    [Settings, str, dict[str, Any], str, Any],
    None,
]


class FileEventQueueWriter:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def put(self, message: WorkerMessage) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(message))
            handle.write("\n")


class FileEventQueueReader:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._offset = 0
        self._partial = ""
        self._pending: deque[WorkerMessage] = deque()

    def get(self, block: bool = True, timeout: float | None = None) -> WorkerMessage:
        deadline = None if timeout is None else (time.monotonic() + timeout)
        while True:
            self._fill_pending()
            if self._pending:
                return self._pending.popleft()
            if not block:
                raise queue.Empty()
            if deadline is not None and time.monotonic() >= deadline:
                raise queue.Empty()
            time.sleep(0.01)

    def _fill_pending(self) -> None:
        if not self.path.exists():
            return
        with self.path.open("r", encoding="utf-8") as handle:
            handle.seek(self._offset)
            chunk = handle.read()
            self._offset = handle.tell()
        if not chunk:
            return

        text = self._partial + chunk
        lines = text.splitlines(keepends=True)
        self._partial = ""
        for line in lines:
            if line.endswith("\n") or line.endswith("\r"):
                stripped = line.strip()
                if stripped:
                    self._pending.append(json.loads(stripped))
            else:
                self._partial = line


class ProcessHandle:
    def __init__(self, process: mp.Process | subprocess.Popen[bytes]) -> None:
        self._process = process

    @property
    def pid(self) -> int | None:
        return getattr(self._process, "pid", None)

    def is_alive(self) -> bool:
        process = self._process
        if hasattr(process, "poll"):
            return process.poll() is None  # pyright: ignore[reportAttributeAccessIssue]
        return process.is_alive()  # pyright: ignore[reportAttributeAccessIssue]

    def terminate(self) -> None:
        self._process.terminate()

    def join(self, timeout: float | None = None) -> None:
        process = self._process
        if hasattr(process, "wait"):
            try:
                process.wait(timeout=timeout)  # pyright: ignore[reportAttributeAccessIssue]
            except subprocess.TimeoutExpired:
                return
            return
        process.join(timeout=timeout)  # pyright: ignore[reportAttributeAccessIssue]

    def kill(self) -> None:
        process = self._process
        if hasattr(process, "kill"):
            process.kill()


@dataclass(slots=True)
class ActiveExecution:
    run_id: str
    pipeline_id: str
    process: ProcessHandle
    event_queue: FileEventQueueReader
    run_dir: Path
    cancel_requested: bool = False


def _serialize_node_results(node_results: dict[str, Any]) -> dict[str, dict[str, Any]]:
    serialized: dict[str, dict[str, Any]] = {}
    for node_id, result in node_results.items():
        serialized[node_id] = {
            "node_id": result.node_id,
            "checkpoint_id": result.checkpoint_id,
            "history_hash": result.history_hash,
            "status": result.status,
        }
    return serialized


def _execute_pipeline_worker(
    settings: Settings,
    pipeline_id: str,
    pipeline: dict[str, Any],
    _run_id: str,
    event_queue: FileEventQueueWriter,
) -> None:
    if settings.workspace_dir:
        os.environ["FORGE_WORKSPACE_DIR"] = settings.workspace_dir

    registry = BlockRegistry(
        blocks_dir=settings.blocks_dir,
        package_name="blocks",
        custom_blocks_dir=settings.custom_blocks_dir,
    )
    registry.discover(force_reload=True)
    checkpoint_store = CheckpointStore(settings.checkpoint_dir)
    runner = PipelineRunner(registry=registry, checkpoint_store=checkpoint_store)

    def on_event(event: dict[str, Any]) -> None:
        event_queue.put({"kind": "event", "payload": event})

    try:
        result = runner.run_pipeline(pipeline, on_event)
        event_queue.put(
            {
                "kind": "result",
                "payload": {
                    "pipeline_id": pipeline_id,
                    "topological_order": result.topological_order,
                    "executed_nodes": result.executed_nodes,
                    "reused_nodes": result.reused_nodes,
                    "node_results": _serialize_node_results(result.node_results),
                },
            }
        )
    except Exception as exc:
        event_queue.put({"kind": "error", "message": str(exc)})
    finally:
        event_queue.put({"kind": "done"})


class ExecutionManager:
    def __init__(
        self,
        settings: Settings,
        *,
        worker_target: WorkerTarget | None = None,
    ) -> None:
        self._settings = settings
        self._worker_target = worker_target
        self._ctx = mp.get_context("spawn")
        self._lock = Lock()
        self._runs_by_id: dict[str, ActiveExecution] = {}
        self._pipeline_to_run: dict[str, str] = {}
        self._run_root = (
            Path(settings.pipeline_dir).resolve().parent / ".execution_manager"
        )
        self._run_root.mkdir(parents=True, exist_ok=True)

    def start_execution(
        self,
        pipeline_id: str,
        pipeline: dict[str, Any],
    ) -> ActiveExecution:
        with self._lock:
            self._prune_dead_locked()
            existing_run_id = self._pipeline_to_run.get(pipeline_id)
            if existing_run_id is not None:
                existing = self._runs_by_id.get(existing_run_id)
                if existing is not None and existing.process.is_alive():
                    raise RuntimeError(f"Pipeline '{pipeline_id}' is already running.")

            run_id = uuid.uuid4().hex
            run_dir = self._run_root / run_id
            run_dir.mkdir(parents=True, exist_ok=True)
            event_log = run_dir / "events.jsonl"
            process = self._start_worker_process(
                run_id=run_id,
                pipeline_id=pipeline_id,
                pipeline=pipeline,
                run_dir=run_dir,
                event_log=event_log,
            )

            run = ActiveExecution(
                run_id=run_id,
                pipeline_id=pipeline_id,
                process=ProcessHandle(process),
                event_queue=FileEventQueueReader(event_log),
                run_dir=run_dir,
            )
            self._runs_by_id[run_id] = run
            self._pipeline_to_run[pipeline_id] = run_id
            return run

    def _start_worker_process(
        self,
        *,
        run_id: str,
        pipeline_id: str,
        pipeline: dict[str, Any],
        run_dir: Path,
        event_log: Path,
    ) -> mp.Process | subprocess.Popen[bytes]:
        if self._worker_target is not None:
            event_queue = FileEventQueueWriter(event_log)
            process = self._ctx.Process(
                target=self._worker_target,
                args=(self._settings, pipeline_id, pipeline, run_id, event_queue),
                daemon=True,
            )
            process.start()
            return process  # pyright: ignore[reportReturnType]

        request_path = run_dir / "request.json"
        stderr_path = run_dir / "worker.stderr.log"
        request_payload = {
            "settings": {
                "checkpoint_dir": self._settings.checkpoint_dir,
                "pipeline_dir": self._settings.pipeline_dir,
                "blocks_dir": self._settings.blocks_dir,
                "custom_blocks_dir": self._settings.custom_blocks_dir,
                "default_file_path": self._settings.default_file_path,
                "workspace_dir": self._settings.workspace_dir,
                "log_level": self._settings.log_level,
                "cors_origins": self._settings.cors_origins,
            },
            "pipeline_id": pipeline_id,
            "pipeline": pipeline,
            "run_id": run_id,
            "event_log": str(event_log),
        }
        request_path.write_text(json.dumps(request_payload), encoding="utf-8")
        stderr_handle = stderr_path.open("ab")
        creationflags = (
            getattr(subprocess, "CREATE_NO_WINDOW", 0) if os.name == "nt" else 0
        )
        try:
            process = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "backend.engine.execution_worker",
                    "--request",
                    str(request_path),
                ],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=stderr_handle,
                close_fds=True,
                creationflags=creationflags,
                env=os.environ.copy(),
            )
            stderr_handle.close()
            return process
        except Exception:
            stderr_handle.close()
            raise

    def cancel_pipeline(self, pipeline_id: str) -> bool:
        with self._lock:
            run_id = self._pipeline_to_run.get(pipeline_id)
            if run_id is None:
                return False
            run = self._runs_by_id.get(run_id)
            if run is None:
                self._pipeline_to_run.pop(pipeline_id, None)
                return False
            return self._cancel_locked(run)

    def cancel_run(self, run_id: str) -> bool:
        with self._lock:
            run = self._runs_by_id.get(run_id)
            if run is None:
                return False
            return self._cancel_locked(run)

    def is_cancel_requested(self, run_id: str) -> bool:
        with self._lock:
            run = self._runs_by_id.get(run_id)
            return bool(run.cancel_requested) if run is not None else False

    def finalize_run(self, run_id: str) -> None:
        with self._lock:
            run = self._runs_by_id.pop(run_id, None)
            if run is None:
                return
            self._pipeline_to_run.pop(run.pipeline_id, None)
            shutil.rmtree(run.run_dir, ignore_errors=True)

    def _cancel_locked(self, run: ActiveExecution) -> bool:
        run.cancel_requested = True
        if run.process.is_alive():
            run.process.terminate()
            run.process.join(timeout=1.0)
            if run.process.is_alive() and hasattr(run.process, "kill"):
                run.process.kill()
                run.process.join(timeout=1.0)
        return True

    def _prune_dead_locked(self) -> None:
        dead_run_ids = [
            run_id
            for run_id, run in self._runs_by_id.items()
            if not run.process.is_alive()
        ]
        for run_id in dead_run_ids:
            run = self._runs_by_id.pop(run_id, None)
            if run is None:
                continue
            self._pipeline_to_run.pop(run.pipeline_id, None)
            shutil.rmtree(run.run_dir, ignore_errors=True)
