from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Response

from backend.api.deps import get_services
from backend.pipeline_export import build_pipeline_export_archive
from backend.pipeline_layout import prettify_pipeline_layout
from backend.schemas import (
    PipelineEnvelopeModel,
    PipelineModel,
    PipelineSummaryModel,
    StalenessResponseModel,
)
from backend.services import AppServices

router = APIRouter(prefix="/pipelines", tags=["pipelines"])


@router.get("", response_model=list[PipelineSummaryModel])
def list_pipelines(services: AppServices = Depends(get_services)) -> list[PipelineSummaryModel]:
    return [PipelineSummaryModel(**item) for item in services.pipeline_store.list()]


@router.get("/{pipeline_id}", response_model=PipelineEnvelopeModel)
def get_pipeline(
    pipeline_id: str,
    services: AppServices = Depends(get_services),
) -> PipelineEnvelopeModel:
    try:
        payload = services.pipeline_store.read(pipeline_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return PipelineEnvelopeModel(id=pipeline_id, pipeline=PipelineModel.model_validate(payload))


@router.post("", response_model=PipelineEnvelopeModel, status_code=201)
def create_pipeline(
    pipeline: PipelineModel,
    pipeline_id: str | None = Query(default=None),
    services: AppServices = Depends(get_services),
) -> PipelineEnvelopeModel:
    created_id, payload = services.pipeline_store.create(
        pipeline.model_dump(mode="json"),
        pipeline_id=pipeline_id,
    )
    return PipelineEnvelopeModel(id=created_id, pipeline=PipelineModel.model_validate(payload))


@router.put("/{pipeline_id}", response_model=PipelineEnvelopeModel)
def update_pipeline(
    pipeline_id: str,
    pipeline: PipelineModel,
    services: AppServices = Depends(get_services),
) -> PipelineEnvelopeModel:
    try:
        payload = services.pipeline_store.update(pipeline_id, pipeline.model_dump(mode="json"))
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return PipelineEnvelopeModel(id=pipeline_id, pipeline=PipelineModel.model_validate(payload))


@router.post("/{pipeline_id}/prettify", response_model=PipelineEnvelopeModel)
def prettify_pipeline(
    pipeline_id: str,
    services: AppServices = Depends(get_services),
) -> PipelineEnvelopeModel:
    try:
        pipeline = services.pipeline_store.read(pipeline_id)
        payload = services.pipeline_store.update(
            pipeline_id,
            prettify_pipeline_layout(pipeline),
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return PipelineEnvelopeModel(id=pipeline_id, pipeline=PipelineModel.model_validate(payload))


@router.delete("/{pipeline_id}", status_code=204, response_class=Response)
def delete_pipeline(
    pipeline_id: str,
    services: AppServices = Depends(get_services),
) -> Response:
    try:
        services.pipeline_store.delete(pipeline_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return Response(status_code=204)


@router.get("/{pipeline_id}/staleness", response_model=StalenessResponseModel)
def get_staleness(
    pipeline_id: str,
    services: AppServices = Depends(get_services),
) -> StalenessResponseModel:
    try:
        pipeline = services.pipeline_store.read(pipeline_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    history_hashes = services.runner.compute_history_hashes(pipeline)
    stale = {
        node_id: not services.checkpoint_store.exists_by_hash(history_hash)
        for node_id, history_hash in history_hashes.items()
    }
    return StalenessResponseModel(
        pipeline_id=pipeline_id,
        stale=stale,
        history_hashes=history_hashes,
    )


@router.get("/{pipeline_id}/export")
def export_pipeline(
    pipeline_id: str,
    format: Literal["python", "notebook"] = Query(...),
    services: AppServices = Depends(get_services),
) -> Response:
    try:
        pipeline = services.pipeline_store.read(pipeline_id)
        bundle = build_pipeline_export_archive(
            pipeline_id=pipeline_id,
            pipeline=pipeline,
            settings=services.settings,
            registry=services.registry,
            runner=services.runner,
            export_format=format,
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return Response(
        content=bundle.content,
        media_type="application/zip",
        headers={
            "Content-Disposition": f'attachment; filename="{bundle.archive_name}"',
        },
    )
