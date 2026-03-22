from __future__ import annotations

import asyncio
import queue
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, WebSocket, WebSocketDisconnect

from backend.api.deps import get_services
from backend.schemas import CancelExecutionResponseModel, ExecuteResponseModel
from backend.services import AppServices

router = APIRouter(tags=["execution"])


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


@router.post("/pipelines/{pipeline_id}/execute", response_model=ExecuteResponseModel)
def execute_pipeline(
    pipeline_id: str,
    services: AppServices = Depends(get_services),
) -> ExecuteResponseModel:
    try:
        pipeline = services.pipeline_store.read(pipeline_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    try:
        result = services.runner.run_pipeline(pipeline)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return ExecuteResponseModel(
        pipeline_id=pipeline_id,
        topological_order=result.topological_order,
        executed_nodes=result.executed_nodes,
        reused_nodes=result.reused_nodes,
        node_results=_serialize_node_results(result.node_results),
    )


@router.post(
    "/pipelines/{pipeline_id}/cancel",
    response_model=CancelExecutionResponseModel,
)
def cancel_pipeline_execution(
    pipeline_id: str,
    services: AppServices = Depends(get_services),
) -> CancelExecutionResponseModel:
    cancelled = services.execution_manager.cancel_pipeline(pipeline_id)
    return CancelExecutionResponseModel(
        pipeline_id=pipeline_id,
        status="cancelled" if cancelled else "not_running",
    )


@router.websocket("/ws/execute/{pipeline_id}")
async def execute_pipeline_ws(websocket: WebSocket, pipeline_id: str) -> None:
    services: AppServices = websocket.app.state.services  # type: ignore[assignment]
    await websocket.accept()

    try:
        pipeline = services.pipeline_store.read(pipeline_id)
    except FileNotFoundError:
        await websocket.send_json(
            {"type": "run_status", "status": "error", "message": f"Pipeline not found: {pipeline_id}"}
        )
        await websocket.close(code=1008)
        return

    try:
        run = services.execution_manager.start_execution(pipeline_id, pipeline)
    except RuntimeError as exc:
        await websocket.send_json(
            {"type": "run_status", "status": "error", "message": str(exc)}
        )
        await websocket.close(code=1013)
        return

    final_status_sent = False
    try:
        while True:
            try:
                message = await asyncio.to_thread(run.event_queue.get, True, 0.1)
            except queue.Empty:
                message = None

            if message is not None:
                kind = str(message.get("kind", ""))
                if kind == "event":
                    event_payload = message.get("payload")
                    if isinstance(event_payload, dict):
                        await websocket.send_json(event_payload)
                elif kind == "result":
                    result_payload = message.get("payload")
                    if isinstance(result_payload, dict):
                        await websocket.send_json({"type": "run_result", **result_payload})
                    await websocket.send_json({"type": "run_status", "status": "complete"})
                    final_status_sent = True
                    break
                elif kind == "error":
                    await websocket.send_json(
                        {
                            "type": "run_status",
                            "status": "error",
                            "message": str(message.get("message", "Execution failed")),
                        }
                    )
                    final_status_sent = True
                    break

            if not run.process.is_alive():
                if services.execution_manager.is_cancel_requested(run.run_id):
                    await websocket.send_json(
                        {
                            "type": "run_status",
                            "status": "cancelled",
                            "message": "Execution cancelled.",
                        }
                    )
                    final_status_sent = True
                elif not final_status_sent:
                    await websocket.send_json(
                        {
                            "type": "run_status",
                            "status": "error",
                            "message": "Execution process exited unexpectedly.",
                        }
                    )
                    final_status_sent = True
                break
    except WebSocketDisconnect:
        services.execution_manager.cancel_run(run.run_id)
    except Exception as exc:
        await websocket.send_json(
            {"type": "run_status", "status": "error", "message": str(exc)}
        )
    finally:
        services.execution_manager.finalize_run(run.run_id)
        try:
            await websocket.close()
        except RuntimeError:
            # Already closed by client.
            pass
