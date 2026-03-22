from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

from backend.engine.checkpoint_store import CheckpointStore
from backend.mcp_server import run_mcp_stdio
from backend.engine.runner import PipelineRunner
from backend.registry import BlockRegistry
from backend.services import build_services
from backend.settings import Settings, env_or_default


def _build_runner(checkpoint_dir: str, blocks_dir: str) -> tuple[PipelineRunner, CheckpointStore]:
    blocks_path = Path(blocks_dir)
    package_name = blocks_path.name

    registry = BlockRegistry(blocks_dir=blocks_path, package_name=package_name)
    registry.discover(force_reload=True)
    checkpoint_store = CheckpointStore(checkpoint_dir)
    return PipelineRunner(registry, checkpoint_store), checkpoint_store


def _load_pipeline(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Pipeline file not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def cmd_run(args: argparse.Namespace) -> int:
    runner, _ = _build_runner(args.checkpoint_dir, args.blocks_dir)
    pipeline = _load_pipeline(Path(args.pipeline))
    result = runner.run_pipeline(pipeline)

    print(f"Pipeline: {pipeline.get('name', Path(args.pipeline).stem)}")
    print(f"Executed nodes: {len(result.executed_nodes)}")
    print(f"Reused nodes: {len(result.reused_nodes)}")
    for node_id in result.topological_order:
        node_result = result.node_results[node_id]
        print(f"- {node_id}: {node_result.status} [{node_result.checkpoint_id[:12]}]")
    return 0


def cmd_gc(args: argparse.Namespace) -> int:
    runner, checkpoint_store = _build_runner(args.checkpoint_dir, args.blocks_dir)
    pipeline_dir = Path(args.pipeline_dir)
    keep_checkpoint_ids: set[str] = set(args.keep or [])

    if pipeline_dir.exists():
        for pipeline_file in pipeline_dir.glob("*.json"):
            pipeline = _load_pipeline(pipeline_file)
            hashes = runner.compute_history_hashes(pipeline)
            for history_hash in hashes.values():
                checkpoint_id = checkpoint_store.get_checkpoint_id_by_hash(history_hash)
                if checkpoint_id:
                    keep_checkpoint_ids.add(checkpoint_id)

    removed = checkpoint_store.gc(keep_checkpoint_ids)
    print(f"Removed checkpoints: {len(removed)}")
    for checkpoint_id in removed:
        print(f"- {checkpoint_id}")
    return 0


def cmd_mcp(args: argparse.Namespace) -> int:
    settings = Settings(
        checkpoint_dir=args.checkpoint_dir,
        pipeline_dir=args.pipeline_dir,
        blocks_dir=args.blocks_dir,
        default_file_path=args.default_file_path,
        log_level=args.log_level,
        cors_origins=[origin.strip() for origin in args.cors_origins.split(",") if origin.strip()],
    )
    services = build_services(settings)
    run_mcp_stdio(services)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="Forge", description="Forge Phase 1 CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Execute a pipeline JSON file.")
    run_parser.add_argument("pipeline", help="Path to pipeline JSON.")
    run_parser.add_argument(
        "--checkpoint-dir",
        default=env_or_default("CHECKPOINT_DIR", "./checkpoints"),
        help="Checkpoint directory.",
    )
    run_parser.add_argument(
        "--blocks-dir",
        default=env_or_default("BLOCKS_DIR", "./blocks"),
        help="Blocks directory.",
    )
    run_parser.set_defaults(handler=cmd_run)

    gc_parser = subparsers.add_parser(
        "gc",
        help="Delete checkpoints not referenced by currently saved pipeline definitions.",
    )
    gc_parser.add_argument(
        "--checkpoint-dir",
        default=env_or_default("CHECKPOINT_DIR", "./checkpoints"),
        help="Checkpoint directory.",
    )
    gc_parser.add_argument(
        "--pipeline-dir",
        default=env_or_default("PIPELINE_DIR", "./pipelines"),
        help="Directory containing pipeline JSON files.",
    )
    gc_parser.add_argument(
        "--blocks-dir",
        default=env_or_default("BLOCKS_DIR", "./blocks"),
        help="Blocks directory.",
    )
    gc_parser.add_argument(
        "--keep",
        nargs="*",
        default=None,
        help="Optional checkpoint IDs to preserve.",
    )
    gc_parser.set_defaults(handler=cmd_gc)

    mcp_parser = subparsers.add_parser(
        "mcp",
        help="Run the Forge MCP server over stdio.",
    )
    mcp_parser.add_argument(
        "--checkpoint-dir",
        default=env_or_default("CHECKPOINT_DIR", "./checkpoints"),
        help="Checkpoint directory.",
    )
    mcp_parser.add_argument(
        "--pipeline-dir",
        default=env_or_default("PIPELINE_DIR", "./pipelines"),
        help="Directory containing pipeline JSON files.",
    )
    mcp_parser.add_argument(
        "--blocks-dir",
        default=env_or_default("BLOCKS_DIR", "./blocks"),
        help="Blocks directory.",
    )
    mcp_parser.add_argument(
        "--default-file-path",
        default=env_or_default("DEFAULT_FILE_PATH", ""),
        help="Default starting path for file browser dialogs.",
    )
    mcp_parser.add_argument(
        "--log-level",
        default=env_or_default("LOG_LEVEL", "INFO"),
        help="MCP server log level.",
    )
    mcp_parser.add_argument(
        "--cors-origins",
        default=env_or_default("CORS_ORIGINS", "http://localhost:5173"),
        help="Comma-separated CORS origins for shared settings parity.",
    )
    mcp_parser.set_defaults(handler=cmd_mcp)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.handler(args)


if __name__ == "__main__":
    sys.exit(main())
