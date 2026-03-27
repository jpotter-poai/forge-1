from __future__ import annotations

from dataclasses import dataclass
import logging

from backend.custom_blocks import CustomBlockManager
from backend.document_service import DraftService
from backend.engine.execution_manager import ExecutionManager
from backend.engine.checkpoint_store import CheckpointStore
from backend.engine.runner import PipelineRunner
from backend.pipeline_store import PipelineStore
from backend.registry import BlockRegistry
from backend.settings import Settings

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class AppServices:
    settings: Settings
    registry: BlockRegistry
    checkpoint_store: CheckpointStore
    pipeline_store: PipelineStore
    runner: PipelineRunner
    execution_manager: ExecutionManager
    document_service: DraftService
    custom_block_manager: CustomBlockManager


def build_services(settings: Settings) -> AppServices:
    logger.info(
        "[services] Building app services with blocks_dir=%s custom_blocks_dir=%s pipeline_dir=%s checkpoint_dir=%s",
        settings.blocks_dir,
        settings.custom_blocks_dir,
        settings.pipeline_dir,
        settings.checkpoint_dir,
    )
    custom_block_manager = CustomBlockManager(settings.custom_blocks_dir)
    custom_block_manager.ensure_dir()

    registry = BlockRegistry(
        blocks_dir=settings.blocks_dir,
        package_name="blocks",
        custom_blocks_dir=settings.custom_blocks_dir,
    )
    registry.discover(force_reload=True)
    specs = registry.all_specs()
    logger.info(
        "[services] Block registry discovered %s blocks (%s custom)",
        len(specs),
        sum(1 for spec in specs if spec.is_custom),
    )
    logger.info(
        "[services] First block keys: %s",
        ", ".join(spec.key for spec in specs[:10]) or "<none>",
    )

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
        custom_block_manager=custom_block_manager,
    )
