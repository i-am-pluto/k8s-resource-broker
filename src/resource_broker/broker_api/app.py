from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from kubernetes import client as k8s_client
from structlog import get_logger

from resource_broker.broker_api.controllers import health, profiles, recommendations, webhook
from resource_broker.broker_api.controllers.crd_watcher import run_crd_watch_loop
from resource_broker.broker_api.services.profile_registry import ProfileRegistry
from resource_broker.broker_api.services.recommendation_service import RecommendationService
from resource_broker.common.config import settings
from resource_broker.common.dao.database import check_connection
from resource_broker.common.k8s_client import create_k8s_api
from resource_broker.common.logging import configure_logging

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    configure_logging()
    db_ok = await check_connection()
    if not db_ok:
        logger.warning("starting without database connectivity")
    else:
        logger.info("database connected")

    # Build stateful services
    registry = ProfileRegistry()
    svc = RecommendationService(registry=registry)

    try:
        api = create_k8s_api(k8s_client.CustomObjectsApi)
        await registry.bootstrap(api)
        watch_task = asyncio.create_task(run_crd_watch_loop(api, svc))
    except Exception:
        logger.exception("failed to connect to kubernetes; profile registry disabled")
        api = None
        watch_task = None

    _app.state.recommendation_svc = svc

    yield

    if watch_task is not None:
        watch_task.cancel()
        try:
            await watch_task
        except asyncio.CancelledError:
            pass


def create_app() -> FastAPI:
    app = FastAPI(
        title="k8s Resource Broker",
        description="Profile-driven Kubernetes resource patching with pluggable recommendation algorithms",
        version="0.1.0",
        lifespan=lifespan,
        root_path=settings.api_root_path,
        docs_url="/docs",
        redoc_url="/redoc",
    )

    app.include_router(health.router, prefix="/api/v1", tags=["health"])
    app.include_router(profiles.router, prefix="/api/v1/profiles", tags=["profiles"])
    app.include_router(recommendations.router, prefix="/api/v1/recommendations", tags=["recommendations"])
    app.include_router(webhook.router, prefix="/api/v1/webhook", tags=["webhook"])

    return app


app = create_app()
