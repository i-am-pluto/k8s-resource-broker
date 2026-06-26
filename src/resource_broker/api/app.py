from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from kubernetes import client as k8s_client
from structlog import get_logger

from resource_broker.api.controllers import health, profiles, recommendations, webhook
from resource_broker.common.dao.database import check_connection
from resource_broker.common.k8s_client import create_k8s_api
from resource_broker.common.logging import configure_logging
from resource_broker.common.services.profile_registry import ProfileRegistry
from resource_broker.common.services.recommendation_service import RecommendationService
from resource_broker.common.services.strategy_registry import StrategyRegistry
from resource_broker.config import settings
from resource_broker.watcher.controllers.crd_watcher import run_crd_watch_loop, run_resync_loop
from resource_broker.watcher.controllers.periodic_worker import PeriodicCheckWorker
from resource_broker.watcher.controllers.strategy_watcher import run_strategy_watch_loop

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
    profile_registry = ProfileRegistry()
    strategy_registry = StrategyRegistry()
    svc = RecommendationService(registry=profile_registry)

    background_tasks: list[asyncio.Task] = []

    try:
        api = create_k8s_api(k8s_client.CustomObjectsApi)

        # Populate both registries from the Kubernetes API (DB fallback on failure).
        await profile_registry.bootstrap(api)
        await strategy_registry.bootstrap(api)

        # Watch streams: Profile CRDs + Strategy CRDs (dual watch).
        background_tasks.append(asyncio.create_task(
            run_crd_watch_loop(api, svc),
            name="profile-crd-watch",
        ))
        background_tasks.append(asyncio.create_task(
            run_strategy_watch_loop(api, strategy_registry, svc),
            name="strategy-crd-watch",
        ))

        # Periodic resync: full re-list to catch any events missed during watch reconnects.
        background_tasks.append(asyncio.create_task(
            run_resync_loop(api, svc, strategy_registry),
            name="crd-resync",
        ))

        # Schedule-driven periodic check worker.
        periodic_worker = PeriodicCheckWorker(
            profile_registry=profile_registry,
            strategy_registry=strategy_registry,
            recommendation_svc=svc,
        )
        background_tasks.append(asyncio.create_task(
            periodic_worker.run_forever(),
            name="periodic-check-worker",
        ))

        logger.info(
            "kubernetes background tasks started",
            tasks=[t.get_name() for t in background_tasks],
        )
    except Exception:
        logger.exception("failed to connect to kubernetes; profile registry disabled")

    _app.state.recommendation_svc = svc
    _app.state.strategy_registry = strategy_registry

    yield

    for task in background_tasks:
        task.cancel()
        try:
            await task
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
