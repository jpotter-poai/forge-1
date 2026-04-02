from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.api.blocks import router as blocks_router
from backend.api.checkpoints import router as checkpoints_router
from backend.api.custom_blocks import router as custom_blocks_router
from backend.api.execution import router as execution_router
from backend.api.files import router as files_router
from backend.api.mcp_config import router as mcp_config_router
from backend.api.pipelines import router as pipelines_router
from backend.mcp_server import build_mcp_server
from backend.services import build_services
from backend.settings import Settings
from backend.version import get_forge_version


def create_app(settings: Settings | None = None) -> FastAPI:
    cfg = settings or Settings.from_env()
    services = build_services(cfg)
    mcp_server = build_mcp_server(services)
    mcp_http_app = mcp_server.streamable_http_app()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.services = services
        app.state.mcp_server = mcp_server
        async with mcp_server.session_manager.run():
            yield

    app = FastAPI(title="Forge API", version=get_forge_version(), lifespan=lifespan)
    app.state.services = services
    app.state.mcp_server = mcp_server
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cfg.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(blocks_router, prefix="/api")
    app.include_router(custom_blocks_router, prefix="/api")
    app.include_router(mcp_config_router, prefix="/api")
    app.include_router(pipelines_router, prefix="/api")
    app.include_router(execution_router, prefix="/api")
    app.include_router(checkpoints_router, prefix="/api")
    app.include_router(files_router, prefix="/api")
    app.mount("/mcp", mcp_http_app)
    return app


app = create_app()
