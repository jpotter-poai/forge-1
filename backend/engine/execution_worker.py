from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

from backend.engine.execution_manager import FileEventQueueWriter, _execute_pipeline_worker
from backend.settings import Settings


def _load_request(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Forge pipeline execution worker")
    parser.add_argument("--request", required=True, help="Path to the execution request JSON.")
    args = parser.parse_args(argv)

    payload = _load_request(Path(args.request))
    settings = Settings(**payload["settings"])
    _execute_pipeline_worker(
        settings,
        str(payload["pipeline_id"]),
        dict(payload["pipeline"]),
        str(payload["run_id"]),
        FileEventQueueWriter(payload["event_log"]),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
