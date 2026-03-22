from __future__ import annotations

from dataclasses import dataclass

from backend.document_service import DraftService
from backend.engine.execution_manager import ExecutionManager
from backend.engine.checkpoint_store import CheckpointStore
from backend.engine.runner import PipelineRunner
from backend.pipeline_store import PipelineStore
from backend.registry import BlockRegistry
from backend.settings import Settings


@dataclass(slots=True)
class AppServices:
    settings: Settings
    registry: BlockRegistry
    checkpoint_store: CheckpointStore
    pipeline_store: PipelineStore
    runner: PipelineRunner
    execution_manager: ExecutionManager
    document_service: DraftService


def build_services(settings: Settings) -> AppServices:
    registry = BlockRegistry(blocks_dir=settings.blocks_dir, package_name="blocks")
    registry.discover(force_reload=True)

    checkpoint_store = CheckpointStore(settings.checkpoint_dir)
    pipeline_store = PipelineStore(settings.pipeline_dir)
    runner = PipelineRunner(registry=registry, checkpoint_store=checkpoint_store)
    execution_manager = ExecutionManager(settings)
    document_service = DraftService(
        settings=settings,
        registry=registry,
        checkpoint_store=checkpoint_store,
        pipeline_store=pipeline_store,
        runner=runner,
        execution_manager=execution_manager,
    )
    return AppServices(
        settings=settings,
        registry=registry,
        checkpoint_store=checkpoint_store,
        pipeline_store=pipeline_store,
        runner=runner,
        execution_manager=execution_manager,
        document_service=document_service,
    )
