# Service 3 — Service-Level Deployment: Engine + Mutating Webhook Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Precompute service-level resource recommendations for active Deployments using the percentile algorithm, store them keyed by the three content hashes, and serve them from a Deployment-admission mutating webhook that only *looks up* a precomputed row (never computes), passing through unchanged on a miss or stale recommendation.

**Architecture:** A Deployment watch upserts `active_services` (service id + profile/strategy/resource-type hashes + mode). An `engine` cron joins `active_services` with `pod_performance_metric`, resolves Profile + Strategy from the CRD cache, runs the percentile algorithm per managed field with min/max + staleness guards, and upserts `service_recommendations` keyed by `(namespace, service_name, profile_hash, strategy_hash, resource_type_hash)`. The `api` webhook builds the service id + the three *current* hashes from the cache, looks up a matching fresh recommendation, and patches `/spec/template` of the Deployment; a miss or stale row passes through unchanged. A retention sweep deletes orphaned recommendations.

**Tech Stack:** Python 3.12, FastAPI, SQLAlchemy 2 async + asyncpg, Alembic, kubernetes client, structlog, pytest + pytest-asyncio.

## Global Constraints

- Python `>=3.12`; `uv` for deps/commands.
- Ruff line-length `120`, rules `E,F,I,N,W,UP,ANN,B,SIM,ARG`. `uv run ruff check src tests` clean before each commit.
- Config via `BROKER_`-prefixed env vars.
- Content hash = **SHA-256 hex (64 chars)** via `resource_broker.common.hashing.content_hash` (Service 1).
- **Invariant: the webhook never computes.** It only looks up a precomputed row.
- A recommendation is valid only while **all three** hashes equal the current cache hashes (cascade-on-change → miss → pass-through).
- Unit tests run against `sqlite+aiosqlite://` (`tests/conftest.py`); no live Postgres/k8s required for unit tests.
- **Depends on Service 1** (CRD cache, models, hashing, `resolve_field_strategy`, `resource_type_hash`) **and Service 2** (`PodPerformanceRepository.get_percentiles`, `OwnerResolver`).
- TDD: failing test → verify fail → minimal impl → verify pass → commit per task.

---

## File Structure

| File | Responsibility |
|---|---|
| `src/resource_broker/common/dao/orm_models.py` (modify) | Add `ActiveServiceModel` + `ServiceRecommendationModel` |
| `alembic/versions/0006_active_services_recommendations.py` (new) | Create `active_services` + `service_recommendations` |
| `src/resource_broker/common/dao/repositories/active_services.py` (new) | `ActiveServicesRepository` — upsert + list |
| `src/resource_broker/common/dao/repositories/recommendations.py` (new) | `ServiceRecommendationsRepository` — upsert + lookup + sweep |
| `src/resource_broker/common/services/service_identity.py` (new) | `build_service_hashes(profile, strategies, rtype) -> ServiceHashes` |
| `src/resource_broker/watcher/controllers/deployment_watch.py` (new) | Deployment watch → upsert `active_services` |
| `src/resource_broker/engine/percentile_engine.py` (new) | Per-service percentile recommendation compute |
| `src/resource_broker/engine/runner.py` (new) | Engine cron loop + retention sweep |
| `src/resource_broker/api/controllers/webhook.py` (rewrite) | Deployment lookup + pass-through |
| `src/resource_broker/watcher/controllers/handler.py` (rewrite) | `build_deployment_patch_response(body, cache, lookup)` |
| `src/resource_broker/__main__.py` (modify) | Add `engine` subcommand; Deployment watch into `watcher` |
| `src/resource_broker/config.py` (modify) | Engine/webhook/staleness config vars |
| `deploy/resource-broker/mutating-webhook.yaml` (rewrite) | Target Deployments |
| `deploy/resource-broker/engine-deployment.yaml` (new) | Engine process Deployment |
| `deploy/samples/hello-world/*` (modify) | Annotated Deployment sample |
| `scripts/test-minikube.sh` (modify) | Integration assertion (covered fully in the integration plan) |
| Tests under `tests/unit/` | One module per new unit |

---

## Design Decisions

### D1 — Staleness mitigation (task 3.c)

Postgres/cache data can be stale for both performance and profile/strategy. Layered defenses, all in this plan:

1. **Hash-gated lookup (correctness).** The webhook computes the *current* `profile_hash`/`strategy_hash`/`resource_type_hash` from the live CRD cache and selects only a recommendation row matching all three. If a Profile or Strategy changed since the engine ran, the hashes differ → no match → pass-through. A stale profile/strategy *cannot* be applied.
2. **Freshness TTL.** A matched row is used only if `computed_at >= now - BROKER_REC_MAX_AGE_HOURS` (default 24). An old recommendation (engine stalled, metrics stale) is treated as a miss.
3. **Minimum-sample guard.** The engine emits a field recommendation only when `sample_count >= BROKER_PERCENTILE_MIN_SAMPLES` (default 50). Too little performance data → no recommendation for that field (keeps the declared value).
4. **Guardrail clamps.** `min`/`max` from the Profile field are applied after compute (hard bounds).
5. **Bounded-delta clamp.** The engine caps movement vs the configured value: never above `configured * BROKER_REC_MAX_INCREASE_FACTOR` (default 2.0) nor below `configured * BROKER_REC_MAX_DECREASE_FACTOR` (default 0.5). Limits the blast radius of a bad recommendation. (Skipped when the deployment declares no value for that field.)
6. **Mode gating.** `mode=recommendation` records the recommendation but the webhook does **not** patch (logs only); only `mode=enforce` patches. Operators validate before enforcing.

### D2 — Engine join (task 3.b)

`engine` iterates `active_services`, and for each row joins `pod_performance_metric` on `service_name` + `resource_type_hash` (so performance from a different resource-type version is excluded). It resolves the Profile + per-field Strategy from the cache, maps each managed field to a percentile bucket (`cpu_request`/`cpu_limit` → `cpu_pNN`, `memory_request`/`memory_limit` → `mem_pNN`), applies guards (D1.3–D1.5), and writes the recommendation JSON (`{field_name: value}`, locator-independent). Only **builtin** strategies compute; `algo: image` rows are recorded with status `skipped-image` (P2 runner).

---

## Task 1: active_services + service_recommendations — migration + ORM

**Files:**
- Modify: `src/resource_broker/common/dao/orm_models.py`
- Create: `alembic/versions/0006_active_services_recommendations.py`
- Test: `tests/unit/test_service_tables.py`

**Interfaces:**
- Produces:
  - `ActiveServiceModel` — PK `(namespace, service_name)`; cols `service_uid`, `profile_name`, `profile_hash`, `strategy_hash`, `resource_type_hash`, `mode`, `status`, `updated_at`.
  - `ServiceRecommendationModel` — PK `(namespace, service_name, profile_hash, strategy_hash, resource_type_hash)`; cols `recommendation JSONB`, `status`, `computed_at`.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_service_tables.py
from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import select

from resource_broker.common.dao.orm_models import ActiveServiceModel, ServiceRecommendationModel


@pytest.mark.asyncio
async def test_active_service_row(db_session) -> None:
    db_session.add(ActiveServiceModel(
        namespace="default", service_name="default/web", service_uid="uid-1", profile_name="web-profile",
        profile_hash="ph", strategy_hash="sh", resource_type_hash="rth", mode="enforce", status="active",
        updated_at=datetime.now(UTC),
    ))
    await db_session.commit()
    got = (await db_session.execute(select(ActiveServiceModel))).scalars().one()
    assert got.service_name == "default/web"
    assert got.mode == "enforce"


@pytest.mark.asyncio
async def test_service_recommendation_row(db_session) -> None:
    db_session.add(ServiceRecommendationModel(
        namespace="default", service_name="default/web", profile_hash="ph", strategy_hash="sh",
        resource_type_hash="rth", recommendation={"cpu_request": "0.4"}, status="ok", computed_at=datetime.now(UTC),
    ))
    await db_session.commit()
    got = (await db_session.execute(select(ServiceRecommendationModel))).scalars().one()
    assert got.recommendation["cpu_request"] == "0.4"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_service_tables.py -v`
Expected: FAIL with `ImportError: cannot import name 'ActiveServiceModel'`

- [ ] **Step 3a: ORM models** — add to `orm_models.py`:

```python
class ActiveServiceModel(Base):
    __tablename__ = "active_services"

    namespace: Mapped[str] = mapped_column(String(253), primary_key=True)
    service_name: Mapped[str] = mapped_column(String(253), primary_key=True)
    service_uid: Mapped[str | None] = mapped_column(String(64), nullable=True)
    profile_name: Mapped[str] = mapped_column(String(253), nullable=False)
    profile_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    strategy_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    resource_type_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    mode: Mapped[str] = mapped_column(String(64), nullable=False, server_default="recommendation")
    status: Mapped[str] = mapped_column(String(32), nullable=False, server_default="active")
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class ServiceRecommendationModel(Base):
    __tablename__ = "service_recommendations"

    namespace: Mapped[str] = mapped_column(String(253), primary_key=True)
    service_name: Mapped[str] = mapped_column(String(253), primary_key=True)
    profile_hash: Mapped[str] = mapped_column(String(64), primary_key=True)
    strategy_hash: Mapped[str] = mapped_column(String(64), primary_key=True)
    resource_type_hash: Mapped[str] = mapped_column(String(64), primary_key=True)
    recommendation: Mapped[dict] = mapped_column(JSONB, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, server_default="ok")
    computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (Index("idx_rec_lookup", "namespace", "service_name"),)
```

- [ ] **Step 3b: Migration**

```python
# alembic/versions/0006_active_services_recommendations.py
"""Active services + service recommendations.

Revision ID: 0006
Revises: 0005
Create Date: 2026-06-20
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0006"
down_revision: Union[str, None] = "0005"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "active_services",
        sa.Column("namespace", sa.String(253), nullable=False),
        sa.Column("service_name", sa.String(253), nullable=False),
        sa.Column("service_uid", sa.String(64), nullable=True),
        sa.Column("profile_name", sa.String(253), nullable=False),
        sa.Column("profile_hash", sa.String(64), nullable=False),
        sa.Column("strategy_hash", sa.String(64), nullable=False),
        sa.Column("resource_type_hash", sa.String(64), nullable=False),
        sa.Column("mode", sa.String(64), nullable=False, server_default="recommendation"),
        sa.Column("status", sa.String(32), nullable=False, server_default="active"),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("namespace", "service_name"),
    )
    op.create_table(
        "service_recommendations",
        sa.Column("namespace", sa.String(253), nullable=False),
        sa.Column("service_name", sa.String(253), nullable=False),
        sa.Column("profile_hash", sa.String(64), nullable=False),
        sa.Column("strategy_hash", sa.String(64), nullable=False),
        sa.Column("resource_type_hash", sa.String(64), nullable=False),
        sa.Column("recommendation", postgresql.JSONB(), nullable=False),
        sa.Column("status", sa.String(32), nullable=False, server_default="ok"),
        sa.Column("computed_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint(
            "namespace", "service_name", "profile_hash", "strategy_hash", "resource_type_hash"
        ),
    )
    op.create_index("idx_rec_lookup", "service_recommendations", ["namespace", "service_name"])


def downgrade() -> None:
    op.drop_index("idx_rec_lookup", table_name="service_recommendations")
    op.drop_table("service_recommendations")
    op.drop_table("active_services")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_service_tables.py -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(dao): migration 0006 — active_services + service_recommendations"
```

---

## Task 2: Service hashes helper + repositories

**Files:**
- Create: `src/resource_broker/common/services/service_identity.py`
- Create: `src/resource_broker/common/dao/repositories/active_services.py`
- Create: `src/resource_broker/common/dao/repositories/recommendations.py`
- Test: `tests/unit/test_service_identity.py`, `tests/unit/test_recommendation_repository.py`

**Interfaces:**
- Produces:
  - `ServiceHashes(profile_hash: str, strategy_hash: str, resource_type_hash: str)` dataclass.
  - `build_service_hashes(profile: Profile, strategies: StrategyRegistry, rtype: ResourceType) -> ServiceHashes` (service-level strategy hash = the resolved default/first-field strategy, matching Service 2's tagging).
  - `ActiveServicesRepository(session)`: `async upsert(...) -> None`, `async list_active() -> list[ActiveServiceModel]`, `async delete(namespace, service_name) -> None`.
  - `ServiceRecommendationsRepository(session)`: `async upsert(namespace, service_name, hashes: ServiceHashes, recommendation: dict, status: str) -> None`, `async get(namespace, service_name, hashes: ServiceHashes) -> ServiceRecommendationModel | None`, `async sweep_orphans(active_keys: set, older_than: datetime) -> int`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/test_service_identity.py
from __future__ import annotations

from resource_broker.common.models.profile import Profile
from resource_broker.common.services.service_identity import build_service_hashes
from resource_broker.common.services.strategy_registry import StrategyRegistry
from resource_broker.resource_types.registry import resource_type_registry


def test_build_service_hashes() -> None:
    strat = StrategyRegistry()
    strat.upsert_from_crd(
        {"metadata": {"name": "p75", "namespace": "default"}, "spec": {"algo": "percentile", "args": {"percentile": 75}}}
    )
    p = Profile.from_crd(
        {"metadata": {"name": "web", "namespace": "default"},
         "spec": {"resource-type": "k8s-pod", "default-strategy": "p75", "fields": {"cpu_request": {}}}}
    )
    h = build_service_hashes(p, strat, resource_type_registry.get("k8s-pod"))
    assert h.profile_hash == p.profile_hash
    assert len(h.strategy_hash) == 64
    assert h.resource_type_hash == resource_type_registry.get("k8s-pod").resource_type_hash
```

```python
# tests/unit/test_recommendation_repository.py
from __future__ import annotations

import pytest

from resource_broker.common.dao.repositories.recommendations import ServiceRecommendationsRepository
from resource_broker.common.services.service_identity import ServiceHashes


@pytest.mark.asyncio
async def test_upsert_then_get(db_session) -> None:
    repo = ServiceRecommendationsRepository(db_session)
    h = ServiceHashes("ph", "sh", "rth")
    await repo.upsert("default", "default/web", h, {"cpu_request": "0.4"}, status="ok")
    await db_session.commit()

    got = await repo.get("default", "default/web", h)
    assert got is not None
    assert got.recommendation["cpu_request"] == "0.4"

    miss = await repo.get("default", "default/web", ServiceHashes("ph", "sh", "OTHER"))
    assert miss is None


@pytest.mark.asyncio
async def test_upsert_overwrites_same_key(db_session) -> None:
    repo = ServiceRecommendationsRepository(db_session)
    h = ServiceHashes("ph", "sh", "rth")
    await repo.upsert("default", "default/web", h, {"cpu_request": "0.4"}, status="ok")
    await repo.upsert("default", "default/web", h, {"cpu_request": "0.9"}, status="ok")
    await db_session.commit()
    got = await repo.get("default", "default/web", h)
    assert got.recommendation["cpu_request"] == "0.9"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_service_identity.py tests/unit/test_recommendation_repository.py -v`
Expected: FAIL with import errors.

- [ ] **Step 3a: `service_identity.py`**

```python
# src/resource_broker/common/services/service_identity.py
from __future__ import annotations

from dataclasses import dataclass

from resource_broker.common.models.profile import Profile
from resource_broker.common.services.strategy_registry import StrategyRegistry
from resource_broker.common.services.strategy_resolver import resolve_field_strategy
from resource_broker.resource_types.base import ResourceType


@dataclass(frozen=True)
class ServiceHashes:
    profile_hash: str
    strategy_hash: str
    resource_type_hash: str


def build_service_hashes(profile: Profile, strategies: StrategyRegistry, rtype: ResourceType) -> ServiceHashes:
    strategy_hash = ""
    for field_name in profile.fields:
        s = resolve_field_strategy(profile, field_name, strategies)
        if s is not None:
            strategy_hash = s.strategy_hash
            break
    return ServiceHashes(
        profile_hash=profile.profile_hash,
        strategy_hash=strategy_hash,
        resource_type_hash=rtype.resource_type_hash,
    )
```

- [ ] **Step 3b: `active_services.py`**

```python
# src/resource_broker/common/dao/repositories/active_services.py
from __future__ import annotations

from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from resource_broker.common.dao.orm_models import ActiveServiceModel
from resource_broker.common.services.service_identity import ServiceHashes


class ActiveServicesRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def upsert(
        self,
        namespace: str,
        service_name: str,
        service_uid: str | None,
        profile_name: str,
        hashes: ServiceHashes,
        mode: str,
        status: str = "active",
    ) -> None:
        dialect = self._session.bind.dialect.name if self._session.bind else "postgresql"
        insert = sqlite_insert if dialect == "sqlite" else pg_insert
        stmt = insert(ActiveServiceModel).values(
            namespace=namespace, service_name=service_name, service_uid=service_uid, profile_name=profile_name,
            profile_hash=hashes.profile_hash, strategy_hash=hashes.strategy_hash,
            resource_type_hash=hashes.resource_type_hash, mode=mode, status=status,
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["namespace", "service_name"],
            set_={
                "service_uid": stmt.excluded.service_uid, "profile_name": stmt.excluded.profile_name,
                "profile_hash": stmt.excluded.profile_hash, "strategy_hash": stmt.excluded.strategy_hash,
                "resource_type_hash": stmt.excluded.resource_type_hash, "mode": stmt.excluded.mode,
                "status": stmt.excluded.status,
            },
        )
        await self._session.execute(stmt)

    async def list_active(self) -> list[ActiveServiceModel]:
        stmt = select(ActiveServiceModel).where(ActiveServiceModel.status == "active")
        return list((await self._session.execute(stmt)).scalars().all())

    async def delete(self, namespace: str, service_name: str) -> None:
        await self._session.execute(
            delete(ActiveServiceModel).where(
                ActiveServiceModel.namespace == namespace, ActiveServiceModel.service_name == service_name
            )
        )
```

- [ ] **Step 3c: `recommendations.py`**

```python
# src/resource_broker/common/dao/repositories/recommendations.py
from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from resource_broker.common.dao.orm_models import ServiceRecommendationModel
from resource_broker.common.services.service_identity import ServiceHashes


class ServiceRecommendationsRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def upsert(
        self, namespace: str, service_name: str, hashes: ServiceHashes, recommendation: dict[str, Any], status: str
    ) -> None:
        dialect = self._session.bind.dialect.name if self._session.bind else "postgresql"
        insert = sqlite_insert if dialect == "sqlite" else pg_insert
        stmt = insert(ServiceRecommendationModel).values(
            namespace=namespace, service_name=service_name, profile_hash=hashes.profile_hash,
            strategy_hash=hashes.strategy_hash, resource_type_hash=hashes.resource_type_hash,
            recommendation=recommendation, status=status,
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["namespace", "service_name", "profile_hash", "strategy_hash", "resource_type_hash"],
            set_={"recommendation": stmt.excluded.recommendation, "status": stmt.excluded.status,
                  "computed_at": __import__("sqlalchemy").func.now()},
        )
        await self._session.execute(stmt)

    async def get(
        self, namespace: str, service_name: str, hashes: ServiceHashes
    ) -> ServiceRecommendationModel | None:
        stmt = select(ServiceRecommendationModel).where(
            ServiceRecommendationModel.namespace == namespace,
            ServiceRecommendationModel.service_name == service_name,
            ServiceRecommendationModel.profile_hash == hashes.profile_hash,
            ServiceRecommendationModel.strategy_hash == hashes.strategy_hash,
            ServiceRecommendationModel.resource_type_hash == hashes.resource_type_hash,
        )
        return (await self._session.execute(stmt)).scalar_one_or_none()

    async def sweep_orphans(self, active_keys: set[tuple[str, str, str, str, str]], older_than: datetime) -> int:
        rows = (await self._session.execute(select(ServiceRecommendationModel))).scalars().all()
        removed = 0
        for r in rows:
            key = (r.namespace, r.service_name, r.profile_hash, r.strategy_hash, r.resource_type_hash)
            if key not in active_keys and r.computed_at < older_than:
                await self._session.execute(
                    delete(ServiceRecommendationModel).where(
                        ServiceRecommendationModel.namespace == r.namespace,
                        ServiceRecommendationModel.service_name == r.service_name,
                        ServiceRecommendationModel.profile_hash == r.profile_hash,
                        ServiceRecommendationModel.strategy_hash == r.strategy_hash,
                        ServiceRecommendationModel.resource_type_hash == r.resource_type_hash,
                    )
                )
                removed += 1
        return removed
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_service_identity.py tests/unit/test_recommendation_repository.py -v`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(engine): service hashes + active_services + recommendations repos"
```

---

## Task 3: Deployment watch → upsert active_services

**Files:**
- Create: `src/resource_broker/watcher/controllers/deployment_watch.py`
- Test: `tests/unit/test_deployment_watch.py`

**Interfaces:**
- Consumes: `CrdCache` (S1), `build_service_hashes` (T2), `ActiveServicesRepository` (T2).
- Produces:
  - `deployment_to_active_service(deploy: dict, cache: CrdCache) -> dict | None` (pure) — reads the profile annotation/label from the Deployment's **pod template** or its metadata, resolves Profile + hashes, returns the upsert kwargs (or `None` when no profile / unknown profile).
  - `async run_deployment_watch_loop(apps_api, cache: CrdCache) -> None` — watch Deployments; ADDED/MODIFIED → upsert `active_services`; DELETED → delete row.

- [ ] **Step 1: Write the failing test** (pure mapping)

```python
# tests/unit/test_deployment_watch.py
from __future__ import annotations

from resource_broker.common.services.crd_cache import CrdCache
from resource_broker.common.services.profile_registry import ProfileRegistry
from resource_broker.common.services.strategy_registry import StrategyRegistry
from resource_broker.config import settings
from resource_broker.watcher.controllers.deployment_watch import deployment_to_active_service


def _cache() -> CrdCache:
    strat = StrategyRegistry()
    strat.upsert_from_crd(
        {"metadata": {"name": "p75", "namespace": "default"}, "spec": {"algo": "percentile", "args": {"percentile": 75}}}
    )
    prof = ProfileRegistry()
    prof.upsert_from_crd(
        {"metadata": {"name": "web-profile", "namespace": "default"},
         "spec": {"resource-type": "k8s-pod", "mode": "enforce", "default-strategy": "p75",
                  "fields": {"cpu_request": {}}}}
    )
    return CrdCache(prof, strat)


def _deploy() -> dict:
    return {
        "metadata": {"name": "web", "namespace": "default", "uid": "uid-1"},
        "spec": {"template": {"metadata": {"labels": {settings.profile_annotation_key: "web-profile"}}}},
    }


def test_maps_deployment_to_active_service() -> None:
    out = deployment_to_active_service(_deploy(), _cache())
    assert out is not None
    assert out["namespace"] == "default"
    assert out["service_name"] == "default/web"
    assert out["profile_name"] == "web-profile"
    assert out["mode"] == "enforce"
    assert len(out["hashes"].profile_hash) == 64


def test_no_annotation_returns_none() -> None:
    d = _deploy()
    d["spec"]["template"]["metadata"]["labels"] = {}
    assert deployment_to_active_service(d, _cache()) is None


def test_unknown_profile_returns_none() -> None:
    d = _deploy()
    d["spec"]["template"]["metadata"]["labels"] = {settings.profile_annotation_key: "ghost"}
    assert deployment_to_active_service(d, _cache()) is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_deployment_watch.py -v`
Expected: FAIL with import error.

- [ ] **Step 3: Write minimal implementation**

```python
# src/resource_broker/watcher/controllers/deployment_watch.py
from __future__ import annotations

import asyncio
from typing import Any

from kubernetes import client as k8s_client, watch as k8s_watch
from structlog import get_logger

from resource_broker.common.dao.database import get_session
from resource_broker.common.dao.repositories.active_services import ActiveServicesRepository
from resource_broker.common.services.crd_cache import CrdCache
from resource_broker.common.services.service_identity import build_service_hashes
from resource_broker.config import settings
from resource_broker.resource_types.registry import resource_type_registry

logger = get_logger(__name__)


def _profile_name(deploy: dict[str, Any]) -> str | None:
    key = settings.profile_annotation_key
    template_meta = (((deploy.get("spec", {}) or {}).get("template", {}) or {}).get("metadata", {}) or {})
    labels = template_meta.get("labels", {}) or {}
    annotations = template_meta.get("annotations", {}) or {}
    top_meta = deploy.get("metadata", {}) or {}
    return (
        labels.get(key)
        or annotations.get(key)
        or (top_meta.get("labels", {}) or {}).get(key)
        or (top_meta.get("annotations", {}) or {}).get(key)
    )


def deployment_to_active_service(deploy: dict[str, Any], cache: CrdCache) -> dict[str, Any] | None:
    meta = deploy.get("metadata", {}) or {}
    namespace = meta.get("namespace", "default")
    name = meta.get("name", "")
    profile_name = _profile_name(deploy)
    if not profile_name:
        return None
    profile = cache.profiles.get(profile_name, namespace)
    if profile is None:
        logger.warning("deployment references unknown profile", deploy=name, profile=profile_name)
        return None
    try:
        rtype = resource_type_registry.get(profile.resource_type)
    except ValueError:
        logger.warning("deployment profile has unknown resource type", profile=profile_name, rt=profile.resource_type)
        return None
    hashes = build_service_hashes(profile, cache.strategies, rtype)
    return {
        "namespace": namespace,
        "service_name": f"{namespace}/{name}",
        "service_uid": meta.get("uid"),
        "profile_name": profile_name,
        "hashes": hashes,
        "mode": profile.mode,
    }


async def run_deployment_watch_loop(apps_api: k8s_client.AppsV1Api, cache: CrdCache) -> None:
    loop = asyncio.get_running_loop()
    while True:
        try:
            await loop.run_in_executor(None, _watch_once, apps_api, cache, loop)
        except Exception:
            logger.exception("deployment watch crashed, restarting in 5s")
            await asyncio.sleep(5)


def _watch_once(apps_api: k8s_client.AppsV1Api, cache: CrdCache, loop: asyncio.AbstractEventLoop) -> None:
    w = k8s_watch.Watch()
    if settings.watch_namespace:
        stream = w.stream(apps_api.list_namespaced_deployment, namespace=settings.watch_namespace, timeout_seconds=0)
    else:
        stream = w.stream(apps_api.list_deployment_for_all_namespaces, timeout_seconds=0)
    for event in stream:
        asyncio.run_coroutine_threadsafe(_handle(event, cache), loop)


async def _handle(event: dict[str, Any], cache: CrdCache) -> None:
    etype = event.get("type", "")
    obj = event.get("raw_object", {}) or {}
    meta = obj.get("metadata", {}) or {}
    namespace = meta.get("namespace", "default")
    name = meta.get("name", "")
    try:
        async with get_session() as session:
            repo = ActiveServicesRepository(session)
            if etype == "DELETED":
                await repo.delete(namespace, f"{namespace}/{name}")
                return
            mapped = deployment_to_active_service(obj, cache)
            if mapped is None:
                await repo.delete(namespace, f"{namespace}/{name}")
                return
            await repo.upsert(
                namespace=mapped["namespace"], service_name=mapped["service_name"],
                service_uid=mapped["service_uid"], profile_name=mapped["profile_name"],
                hashes=mapped["hashes"], mode=mapped["mode"],
            )
    except Exception:
        logger.exception("failed handling deployment event", deploy=name, type=etype)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_deployment_watch.py -v`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
git add src/resource_broker/watcher/controllers/deployment_watch.py tests/unit/test_deployment_watch.py
git commit -m "feat(engine): Deployment watch -> active_services upsert"
```

---

## Task 4: Percentile engine (compute one service's recommendation)

**Files:**
- Create: `src/resource_broker/engine/__init__.py`, `src/resource_broker/engine/percentile_engine.py`
- Modify: `src/resource_broker/config.py`
- Test: `tests/unit/test_percentile_engine.py`

**Interfaces:**
- Consumes: `Profile`/`Strategy` (S1), `resolve_field_strategy` (S1), `PodPerformanceRepository.get_percentiles` (S2), staleness config.
- Produces:
  - `FIELD_TO_METRIC: dict[str, str]` mapping field → `"cpu"`/`"mem"`.
  - `compute_field_value(field_name, strategy, percentiles, configured_value, field_min, field_max, cfg) -> str | None` (pure; applies sample guard, bucket pick, min/max clamp, bounded-delta clamp; returns a Kubernetes quantity string or `None`).
  - `compute_recommendation(profile, strategies, percentiles, configured, cfg) -> dict[str, str]` (per-field loop).
- New config: `percentile_min_samples` (50), `rec_max_increase_factor` (2.0), `rec_max_decrease_factor` (0.5), `rec_max_age_hours` (24), `recommend_interval_seconds` (3600), `rec_retention_days` (7).

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_percentile_engine.py
from __future__ import annotations

from resource_broker.engine.percentile_engine import EngineConfig, compute_field_value


def _cfg(min_samples=50) -> EngineConfig:
    return EngineConfig(
        min_samples=min_samples, max_increase_factor=2.0, max_decrease_factor=0.5
    )


def test_below_min_samples_returns_none() -> None:
    pct = {"cpu_p75": 0.4, "sample_count": 10}
    out = compute_field_value("cpu_request", percentile=75, metric="cpu", percentiles=pct,
                              configured_value=None, field_min=None, field_max=None, cfg=_cfg())
    assert out is None


def test_cpu_value_formatted_as_millicores() -> None:
    pct = {"cpu_p75": 0.4, "sample_count": 100}
    out = compute_field_value("cpu_request", percentile=75, metric="cpu", percentiles=pct,
                              configured_value=None, field_min=None, field_max=None, cfg=_cfg())
    assert out == "400m"


def test_min_max_clamp() -> None:
    pct = {"cpu_p75": 0.05, "sample_count": 100}
    out = compute_field_value("cpu_request", percentile=75, metric="cpu", percentiles=pct,
                              configured_value=None, field_min="100m", field_max="2", cfg=_cfg())
    assert out == "100m"  # clamped up to min


def test_bounded_delta_caps_increase() -> None:
    # p75 = 1.0 core but configured 250m -> capped at 2x = 500m
    pct = {"cpu_p75": 1.0, "sample_count": 100}
    out = compute_field_value("cpu_request", percentile=75, metric="cpu", percentiles=pct,
                              configured_value="250m", field_min=None, field_max=None, cfg=_cfg())
    assert out == "500m"


def test_memory_value_formatted_as_mi() -> None:
    pct = {"mem_p90": 268435456, "sample_count": 100}  # 256Mi
    out = compute_field_value("memory_request", percentile=90, metric="mem", percentiles=pct,
                              configured_value=None, field_min=None, field_max=None, cfg=_cfg())
    assert out == "256Mi"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_percentile_engine.py -v`
Expected: FAIL with import error for `engine.percentile_engine`.

- [ ] **Step 3a: Config vars** — in `config.py`, add a new block:

```python
    # ── Engine / recommendations ─────────────────────────────────────────
    recommend_interval_seconds: int = 3600
    rec_retention_days: int = 7
    rec_max_age_hours: int = 24
    percentile_min_samples: int = 50
    rec_max_increase_factor: float = 2.0
    rec_max_decrease_factor: float = 0.5
    webhook_target_kinds: str = "Deployment"
```

- [ ] **Step 3b: `engine/__init__.py`** — empty file:

```python
```

- [ ] **Step 3c: `percentile_engine.py`**

```python
# src/resource_broker/engine/percentile_engine.py
from __future__ import annotations

from dataclasses import dataclass

from structlog import get_logger

from resource_broker.algorithms.percentile import _parse_resource_value
from resource_broker.common.models.profile import Profile
from resource_broker.common.models.strategy import Strategy
from resource_broker.common.services.strategy_registry import StrategyRegistry
from resource_broker.common.services.strategy_resolver import resolve_field_strategy

logger = get_logger(__name__)

FIELD_TO_METRIC: dict[str, str] = {
    "cpu_request": "cpu",
    "cpu_limit": "cpu",
    "memory_request": "mem",
    "memory_limit": "mem",
}


@dataclass
class EngineConfig:
    min_samples: int = 50
    max_increase_factor: float = 2.0
    max_decrease_factor: float = 0.5


def _format_cpu(cores: float) -> str:
    millicores = round(cores * 1000)
    return f"{millicores}m"


def _format_mem(num_bytes: float) -> str:
    mi = max(1, round(num_bytes / (1024 * 1024)))
    return f"{mi}Mi"


def compute_field_value(
    field_name: str,
    *,
    percentile: int,
    metric: str,
    percentiles: dict[str, float],
    configured_value: str | None,
    field_min: str | None,
    field_max: str | None,
    cfg: EngineConfig,
) -> str | None:
    if percentiles.get("sample_count", 0) < cfg.min_samples:
        return None
    bucket = f"{metric}_p{percentile}"
    raw = percentiles.get(bucket)
    if raw is None:
        return None

    value = float(raw)  # cpu cores or memory bytes

    if field_min is not None:
        value = max(value, _parse_resource_value(field_min, field=field_name))
    if field_max is not None:
        value = min(value, _parse_resource_value(field_max, field=field_name))

    if configured_value is not None:
        conf = _parse_resource_value(configured_value, field=field_name)
        if conf > 0:
            value = min(value, conf * cfg.max_increase_factor)
            value = max(value, conf * cfg.max_decrease_factor)

    return _format_cpu(value) if metric == "cpu" else _format_mem(value)


def _percentile_of(strategy: Strategy) -> int:
    p = strategy.args.get("percentile", 75)
    if isinstance(p, str) and p.startswith("p"):
        return int(p[1:])
    return int(p)


def compute_recommendation(
    profile: Profile,
    strategies: StrategyRegistry,
    percentiles: dict[str, float],
    configured: dict[str, str | None],
    cfg: EngineConfig,
) -> dict[str, str]:
    """Return {field_name: quantity} for every managed field that produced a value."""
    out: dict[str, str] = {}
    for field_name, field_spec in profile.fields.items():
        metric = FIELD_TO_METRIC.get(field_name)
        if metric is None:
            continue
        strategy = resolve_field_strategy(profile, field_name, strategies)
        if strategy is None or not strategy.is_builtin or strategy.algo != "percentile":
            continue
        value = compute_field_value(
            field_name,
            percentile=_percentile_of(strategy),
            metric=metric,
            percentiles=percentiles,
            configured_value=configured.get(field_name),
            field_min=field_spec.min,
            field_max=field_spec.max,
            cfg=cfg,
        )
        if value is not None:
            out[field_name] = value
    return out
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_percentile_engine.py -v`
Expected: PASS (5 passed)

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(engine): percentile compute with sample guard + min/max + bounded-delta"
```

---

## Task 5: Engine runner (cron loop + retention sweep) + `engine` subcommand

**Files:**
- Create: `src/resource_broker/engine/runner.py`
- Modify: `src/resource_broker/__main__.py`
- Test: `tests/unit/test_engine_runner.py`

**Interfaces:**
- Consumes: `CrdCache` (S1), `ActiveServicesRepository`/`ServiceRecommendationsRepository` (T2), `PodPerformanceRepository` (S2), `compute_recommendation` (T4), `build_service_hashes` (T2).
- Produces:
  - `EngineRunner(cache, co_api)` with `async run_once() -> int` (returns recommendations written), `async sweep() -> int`, `async run_forever()`.
  - `engine` subcommand in `__main__`.
  - Helper `extract_configured(active_service, ...)` not needed — configured values come from the latest `pod_performance_metric.configured_resources` for the service (engine reads one recent row per service).

- [ ] **Step 1: Write the failing test** (drive `run_once` against the in-memory DB with a routed session + a seeded active service and metrics)

```python
# tests/unit/test_engine_runner.py
from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import UTC, datetime

import pytest

import resource_broker.engine.runner as runner_mod
from resource_broker.common.dao.orm_models import ActiveServiceModel, PodPerformanceMetricModel
from resource_broker.common.dao.repositories.recommendations import ServiceRecommendationsRepository
from resource_broker.common.services.crd_cache import CrdCache
from resource_broker.common.services.profile_registry import ProfileRegistry
from resource_broker.common.services.service_identity import ServiceHashes, build_service_hashes
from resource_broker.common.services.strategy_registry import StrategyRegistry
from resource_broker.engine.runner import EngineRunner
from resource_broker.resource_types.registry import resource_type_registry


def _cache() -> CrdCache:
    strat = StrategyRegistry()
    strat.upsert_from_crd(
        {"metadata": {"name": "p75", "namespace": "default"}, "spec": {"algo": "percentile", "args": {"percentile": 75}}}
    )
    prof = ProfileRegistry()
    prof.upsert_from_crd(
        {"metadata": {"name": "web-profile", "namespace": "default"},
         "spec": {"resource-type": "k8s-pod", "default-strategy": "p75", "fields": {"cpu_request": {}}}}
    )
    return CrdCache(prof, strat)


@pytest.mark.asyncio
async def test_run_once_writes_recommendation(db_session, monkeypatch) -> None:
    @asynccontextmanager
    async def _fake_session():
        yield db_session

    monkeypatch.setattr(runner_mod, "get_session", _fake_session)

    cache = _cache()
    profile = cache.profiles.get("web-profile", "default")
    hashes = build_service_hashes(profile, cache.strategies, resource_type_registry.get("k8s-pod"))

    db_session.add(ActiveServiceModel(
        namespace="default", service_name="default/web", service_uid="u", profile_name="web-profile",
        profile_hash=hashes.profile_hash, strategy_hash=hashes.strategy_hash,
        resource_type_hash=hashes.resource_type_hash, mode="enforce", status="active", updated_at=datetime.now(UTC),
    ))
    for i in range(100):
        db_session.add(PodPerformanceMetricModel(
            namespace="default", service_name="default/web", pod_name=f"p{i}", container="app",
            resource_type="k8s-pod", resource_type_hash=hashes.resource_type_hash,
            profile_hash=hashes.profile_hash, strategy_hash=hashes.strategy_hash,
            cpu_usage_cores=0.4, mem_usage_bytes=200_000_000,
            configured_resources={"requests": {"cpu": "250m"}}, scraped_at=datetime.now(UTC),
        ))
    await db_session.commit()

    runner = EngineRunner(cache=cache, co_api=None)
    written = await runner.run_once()
    assert written == 1

    rec = await ServiceRecommendationsRepository(db_session).get("default", "default/web", hashes)
    assert rec is not None
    # p75 of 0.4 cores = 400m, but configured 250m caps increase at 2x = 500m -> 400m < 500m so 400m
    assert rec.recommendation["cpu_request"] == "400m"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_engine_runner.py -v`
Expected: FAIL with import error for `engine.runner`.

- [ ] **Step 3a: `runner.py`**

```python
# src/resource_broker/engine/runner.py
from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select
from structlog import get_logger

from resource_broker.common.dao.database import get_session
from resource_broker.common.dao.orm_models import PodPerformanceMetricModel
from resource_broker.common.dao.repositories.active_services import ActiveServicesRepository
from resource_broker.common.dao.repositories.performance import PodPerformanceRepository
from resource_broker.common.dao.repositories.recommendations import ServiceRecommendationsRepository
from resource_broker.common.services.crd_cache import CrdCache
from resource_broker.common.services.service_identity import ServiceHashes
from resource_broker.config import settings
from resource_broker.engine.percentile_engine import EngineConfig, FIELD_TO_METRIC, compute_recommendation
from resource_broker.resource_types.registry import resource_type_registry

logger = get_logger(__name__)


def _engine_config() -> EngineConfig:
    return EngineConfig(
        min_samples=settings.percentile_min_samples,
        max_increase_factor=settings.rec_max_increase_factor,
        max_decrease_factor=settings.rec_max_decrease_factor,
    )


def _extract_configured(configured_resources: dict[str, Any]) -> dict[str, str | None]:
    """Map a pod's configured_resources JSON to per-field configured values."""
    req = (configured_resources or {}).get("requests", {}) or {}
    lim = (configured_resources or {}).get("limits", {}) or {}
    return {
        "cpu_request": req.get("cpu"),
        "memory_request": req.get("memory"),
        "cpu_limit": lim.get("cpu"),
        "memory_limit": lim.get("memory"),
    }


class EngineRunner:
    def __init__(self, cache: CrdCache, co_api: Any) -> None:
        self._cache = cache
        self._co_api = co_api

    async def run_once(self) -> int:
        cfg = _engine_config()
        written = 0
        async with get_session() as session:
            actives = await ActiveServicesRepository(session).list_active()

        for svc in actives:
            profile = self._cache.profiles.get(svc.profile_name, svc.namespace)
            if profile is None:
                continue
            hashes = ServiceHashes(svc.profile_hash, svc.strategy_hash, svc.resource_type_hash)

            async with get_session() as session:
                perf = PodPerformanceRepository(session)
                lookback = _lookback_hours(profile, self._cache)
                percentiles = await perf.get_percentiles(svc.service_name, svc.resource_type_hash, lookback)
                configured = await _latest_configured(session, svc.service_name)

            recommendation = compute_recommendation(profile, self._cache.strategies, percentiles, configured, cfg)
            status = "ok" if recommendation else "no-data"

            async with get_session() as session:
                await ServiceRecommendationsRepository(session).upsert(
                    svc.namespace, svc.service_name, hashes, recommendation, status
                )
            if recommendation:
                written += 1
        logger.info("engine run complete", services=len(actives), written=written)
        return written

    async def sweep(self) -> int:
        async with get_session() as session:
            actives = await ActiveServicesRepository(session).list_active()
            active_keys = {
                (a.namespace, a.service_name, a.profile_hash, a.strategy_hash, a.resource_type_hash) for a in actives
            }
            older_than = datetime.now(UTC) - timedelta(days=settings.rec_retention_days)
            removed = await ServiceRecommendationsRepository(session).sweep_orphans(active_keys, older_than)
        logger.info("recommendation sweep complete", removed=removed)
        return removed

    async def run_forever(self) -> None:
        logger.info("engine started", interval=settings.recommend_interval_seconds)
        while True:
            try:
                await self.run_once()
                await self.sweep()
            except Exception:
                logger.exception("engine cycle failed")
            await asyncio.sleep(settings.recommend_interval_seconds)


def _lookback_hours(profile: Any, cache: CrdCache) -> int:
    from resource_broker.common.services.strategy_resolver import resolve_field_strategy

    for field_name in profile.fields:
        if field_name not in FIELD_TO_METRIC:
            continue
        s = resolve_field_strategy(profile, field_name, cache.strategies)
        if s is not None:
            return int(s.args.get("lookback_hours", 24))
    return 24


async def _latest_configured(session: Any, service_name: str) -> dict[str, str | None]:
    stmt = (
        select(PodPerformanceMetricModel.configured_resources)
        .where(PodPerformanceMetricModel.service_name == service_name)
        .order_by(PodPerformanceMetricModel.scraped_at.desc())
        .limit(1)
    )
    row = (await session.execute(stmt)).scalar_one_or_none()
    return _extract_configured(row or {})
```

- [ ] **Step 3b: `engine` subcommand** — in `__main__.py`:

```python
@app.command()
def engine() -> None:
    """Run the recommendation engine cron."""
    configure_logging()
    from kubernetes import client as k8s_client

    from resource_broker.common.k8s_client import create_k8s_api
    from resource_broker.common.services.crd_cache import CrdCache
    from resource_broker.common.services.profile_registry import ProfileRegistry
    from resource_broker.common.services.strategy_registry import StrategyRegistry
    from resource_broker.engine.runner import EngineRunner
    from resource_broker.watcher.controllers.crd_watcher import run_crd_watch_loops

    async def _main() -> None:
        cache = CrdCache(ProfileRegistry(), StrategyRegistry())
        co_api = create_k8s_api(k8s_client.CustomObjectsApi)
        await cache.bootstrap(co_api)
        watch_task = asyncio.create_task(run_crd_watch_loops(co_api, cache, settings.cache_resync_seconds))
        try:
            await EngineRunner(cache=cache, co_api=co_api).run_forever()
        finally:
            watch_task.cancel()

    asyncio.run(_main())
```

(Add `from resource_broker.config import settings` at top of `__main__.py` if not already imported — it is.)

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_engine_runner.py -v`
Expected: PASS (1 passed)

Run: `uv run python -c "import resource_broker.__main__"` → no error.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(engine): cron runner + retention sweep + engine subcommand"
```

---

## Task 6: Service-level webhook (Deployment lookup + pass-through)

**Files:**
- Rewrite: `src/resource_broker/watcher/controllers/handler.py`
- Rewrite: `src/resource_broker/api/controllers/webhook.py`
- Modify: `src/resource_broker/api/app.py` (Deployment watch in lifespan; expose lookup)
- Test: `tests/unit/test_webhook_handler.py`

**Interfaces:**
- Consumes: `CrdCache` (S1), `build_service_hashes` (T2), `ServiceRecommendationsRepository.get` (T2), `resource_type_registry`.
- Produces:
  - `recommendation_to_patches(recommendation: dict, rtype: ResourceType, base_path="/spec/template") -> list[dict]` (pure — maps `{field: value}` to RFC-6902 replace ops at template-relative locators with `/spec/template` prepended).
  - `async build_deployment_response(body: dict, cache: CrdCache, lookup) -> dict` — admission response; pass-through on no-profile / unknown-profile / miss / stale / `mode != enforce`. `lookup` is `async (namespace, service_name, hashes) -> ServiceRecommendationModel | None`.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_webhook_handler.py
from __future__ import annotations

import base64
import json
from datetime import UTC, datetime

import pytest

from resource_broker.common.services.crd_cache import CrdCache
from resource_broker.common.services.profile_registry import ProfileRegistry
from resource_broker.common.services.strategy_registry import StrategyRegistry
from resource_broker.config import settings
from resource_broker.resource_types.registry import resource_type_registry
from resource_broker.watcher.controllers.handler import (
    build_deployment_response,
    recommendation_to_patches,
)


def _cache(mode="enforce") -> CrdCache:
    strat = StrategyRegistry()
    strat.upsert_from_crd(
        {"metadata": {"name": "p75", "namespace": "default"}, "spec": {"algo": "percentile", "args": {"percentile": 75}}}
    )
    prof = ProfileRegistry()
    prof.upsert_from_crd(
        {"metadata": {"name": "web-profile", "namespace": "default"},
         "spec": {"resource-type": "k8s-pod", "mode": mode, "default-strategy": "p75",
                  "fields": {"cpu_request": {}}}}
    )
    return CrdCache(prof, strat)


def _admission_body() -> dict:
    return {
        "request": {
            "uid": "uid-1",
            "object": {
                "metadata": {"name": "web", "namespace": "default"},
                "spec": {"template": {"metadata": {"labels": {settings.profile_annotation_key: "web-profile"}},
                                      "spec": {"containers": [{"name": "app"}]}}},
            },
        }
    }


def test_recommendation_to_patches_prepends_template() -> None:
    patches = recommendation_to_patches({"cpu_request": "400m"}, resource_type_registry.get("k8s-pod"))
    assert patches[0]["op"] == "replace"
    assert patches[0]["path"] == "/spec/template/spec/containers/0/resources/requests/cpu"
    assert patches[0]["value"] == "400m"


class _Rec:
    def __init__(self, rec):
        self.recommendation = rec
        self.computed_at = datetime.now(UTC)


@pytest.mark.asyncio
async def test_hit_patches_deployment() -> None:
    cache = _cache(mode="enforce")

    async def _lookup(namespace, service_name, hashes):  # noqa: ARG001
        return _Rec({"cpu_request": "400m"})

    resp = await build_deployment_response(_admission_body(), cache, _lookup)
    assert resp["allowed"] is True
    assert resp["patchType"] == "JSONPatch"
    patches = json.loads(base64.b64decode(resp["patch"]))
    assert patches[0]["path"] == "/spec/template/spec/containers/0/resources/requests/cpu"


@pytest.mark.asyncio
async def test_miss_passes_through() -> None:
    cache = _cache(mode="enforce")

    async def _lookup(namespace, service_name, hashes):  # noqa: ARG001
        return None

    resp = await build_deployment_response(_admission_body(), cache, _lookup)
    assert resp == {"uid": "uid-1", "allowed": True}


@pytest.mark.asyncio
async def test_recommendation_mode_does_not_patch() -> None:
    cache = _cache(mode="recommendation")

    async def _lookup(namespace, service_name, hashes):  # noqa: ARG001
        return _Rec({"cpu_request": "400m"})

    resp = await build_deployment_response(_admission_body(), cache, _lookup)
    assert resp == {"uid": "uid-1", "allowed": True}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_webhook_handler.py -v`
Expected: FAIL — `build_deployment_response`/`recommendation_to_patches` not defined.

- [ ] **Step 3a: Rewrite `handler.py`**

```python
# src/resource_broker/watcher/controllers/handler.py
from __future__ import annotations

import base64
import json
from datetime import UTC, datetime, timedelta
from typing import Any, Awaitable, Callable

from structlog import get_logger

from resource_broker.common.services.crd_cache import CrdCache
from resource_broker.common.services.service_identity import ServiceHashes, build_service_hashes
from resource_broker.config import settings
from resource_broker.resource_types.base import ResourceType
from resource_broker.resource_types.registry import resource_type_registry

logger = get_logger(__name__)

LookupFn = Callable[[str, str, ServiceHashes], Awaitable[Any]]


def recommendation_to_patches(
    recommendation: dict[str, str], rtype: ResourceType, base_path: str = "/spec/template"
) -> list[dict[str, Any]]:
    patches: list[dict[str, Any]] = []
    for field_name, value in recommendation.items():
        fd = rtype.fields.get(field_name)
        if fd is None:
            continue
        patches.append({"op": "replace", "path": f"{base_path}{fd.path}", "value": value})
    return patches


def _profile_name_from_deploy(obj: dict[str, Any]) -> str | None:
    key = settings.profile_annotation_key
    tmpl_meta = (((obj.get("spec", {}) or {}).get("template", {}) or {}).get("metadata", {}) or {})
    top_meta = obj.get("metadata", {}) or {}
    for meta in (tmpl_meta, top_meta):
        for bag in ("labels", "annotations"):
            v = (meta.get(bag, {}) or {}).get(key)
            if v:
                return v
    return None


def _allow(uid: str) -> dict[str, Any]:
    return {"uid": uid, "allowed": True}


async def build_deployment_response(body: dict[str, Any], cache: CrdCache, lookup: LookupFn) -> dict[str, Any]:
    req = body.get("request", {}) or {}
    uid = req.get("uid", "")
    obj = req.get("object", {}) or {}
    meta = obj.get("metadata", {}) or {}
    namespace = meta.get("namespace", "default")
    name = meta.get("name", "")

    try:
        profile_name = _profile_name_from_deploy(obj)
        if not profile_name:
            return _allow(uid)
        profile = cache.profiles.get(profile_name, namespace)
        if profile is None or not profile.is_enforce():
            return _allow(uid)  # unknown profile, or recommendation-mode: never patch

        try:
            rtype = resource_type_registry.get(profile.resource_type)
        except ValueError:
            return _allow(uid)

        hashes = build_service_hashes(profile, cache.strategies, rtype)
        rec = await lookup(namespace, f"{namespace}/{name}", hashes)
        if rec is None:
            logger.info("webhook miss, pass-through", deploy=name, namespace=namespace)
            return _allow(uid)

        max_age = timedelta(hours=settings.rec_max_age_hours)
        if datetime.now(UTC) - _aware(rec.computed_at) > max_age:
            logger.info("webhook stale recommendation, pass-through", deploy=name)
            return _allow(uid)

        patches = recommendation_to_patches(rec.recommendation, rtype)
        if not patches:
            return _allow(uid)

        patch_b64 = base64.b64encode(json.dumps(patches).encode()).decode()
        return {"uid": uid, "allowed": True, "patchType": "JSONPatch", "patch": patch_b64}
    except Exception:
        logger.exception("webhook handler failed, failing open", uid=uid)
        return _allow(uid)


def _aware(dt: datetime) -> datetime:
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=UTC)
```

- [ ] **Step 3b: Rewrite `webhook.py`**

```python
# src/resource_broker/api/controllers/webhook.py
from __future__ import annotations

from fastapi import APIRouter, Request
from structlog import get_logger

from resource_broker.api.schemas import AdmissionReviewResponse
from resource_broker.common.dao.database import get_session
from resource_broker.common.dao.repositories.recommendations import ServiceRecommendationsRepository
from resource_broker.common.services.service_identity import ServiceHashes
from resource_broker.watcher.controllers.handler import build_deployment_response

logger = get_logger(__name__)

router = APIRouter()


@router.post("/mutate")
async def mutate(request: Request) -> AdmissionReviewResponse:
    body = await request.json()
    cache = request.app.state.crd_cache

    async def _lookup(namespace: str, service_name: str, hashes: ServiceHashes):
        async with get_session() as session:
            return await ServiceRecommendationsRepository(session).get(namespace, service_name, hashes)

    response = await build_deployment_response(body, cache, _lookup)
    return AdmissionReviewResponse(response=response)
```

- [ ] **Step 3c: Deployment watch in `app.py` lifespan**

In `api/app.py`, after starting the CRD watch task, also start the Deployment watch so `active_services` is maintained by the `serve` process too (single-process dev). Add:

```python
        from kubernetes import client as k8s_client
        from resource_broker.watcher.controllers.deployment_watch import run_deployment_watch_loop

        apps_api = create_k8s_api(k8s_client.AppsV1Api)
        deploy_watch_task = asyncio.create_task(run_deployment_watch_loop(apps_api, cache))
```

Cancel `deploy_watch_task` alongside `watch_task` in the shutdown block. (In a multi-process deployment the `watcher` process owns this loop; running it in `serve` too is idempotent because the upsert is keyed by `(namespace, service_name)`.)

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_webhook_handler.py -v`
Expected: PASS (4 passed)

Run: `uv run python -c "import resource_broker.api.app"` → no error.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(webhook): Deployment lookup + pass-through + freshness TTL"
```

---

## Task 7: Manifests — webhook on Deployment, engine deployment, sample

**Files:**
- Rewrite: `deploy/resource-broker/mutating-webhook.yaml`
- Create: `deploy/resource-broker/engine-deployment.yaml`
- Modify: `deploy/samples/hello-world/deployment.yaml`, `deploy/samples/hello-world/profile.yaml`
- Modify: `.env.example`
- Test: `tests/unit/test_webhook_manifest.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_webhook_manifest.py
from __future__ import annotations

from pathlib import Path

import yaml


def test_webhook_targets_deployments() -> None:
    doc = yaml.safe_load(Path("deploy/resource-broker/mutating-webhook.yaml").read_text())
    rule = doc["webhooks"][0]["rules"][0]
    assert "apps" in rule["apiGroups"]
    assert "deployments" in rule["resources"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_webhook_manifest.py -v`
Expected: FAIL — current manifest targets `pods`.

- [ ] **Step 3a: Rewrite the webhook manifest** (change `rules` + keep objectSelector):

```yaml
    rules:
      - operations: ["CREATE", "UPDATE"]
        apiGroups: ["apps"]
        apiVersions: ["v1"]
        resources: ["deployments"]
        scope: "Namespaced"
```

(Keep the rest of `deploy/resource-broker/mutating-webhook.yaml` as-is: clientConfig path `/api/v1/webhook/mutate`, `failurePolicy: Ignore` recommended for pass-through safety — change `failurePolicy: Fail` to `Ignore` so a broker outage never blocks Deployments. `objectSelector` matches the profile label.)

- [ ] **Step 3b: Engine deployment manifest**

```yaml
# deploy/resource-broker/engine-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: resource-broker-engine
  namespace: resource-broker
  labels: { app.kubernetes.io/name: resource-broker, app.kubernetes.io/component: engine }
spec:
  replicas: 1
  selector: { matchLabels: { app.kubernetes.io/name: resource-broker, app.kubernetes.io/component: engine } }
  template:
    metadata:
      labels: { app.kubernetes.io/name: resource-broker, app.kubernetes.io/component: engine }
    spec:
      serviceAccountName: resource-broker
      containers:
        - name: engine
          image: resource-broker:latest
          command: ["python", "-m", "resource_broker", "engine"]
          envFrom: [{ configMapRef: { name: resource-broker-config } }]
```

- [ ] **Step 3c: Sample Deployment + profile**

In `deploy/samples/hello-world/deployment.yaml`, ensure the pod template carries the profile label:

```yaml
spec:
  template:
    metadata:
      labels:
        resource-broker/profile: hello-world
```

Replace `deploy/samples/hello-world/profile.yaml` with a `Profile` (kind change from `ResourceProfile`):

```yaml
apiVersion: resource-broker.io/v1alpha1
kind: Profile
metadata: { name: hello-world, namespace: default }
spec:
  resource-type: k8s-pod
  mode: enforce
  default-strategy: percentile-p75
  fields:
    cpu_request: { min: "50m", max: "2" }
    memory_request: { min: "64Mi", max: "2Gi" }
```

- [ ] **Step 3d: `.env.example`** — add:

```bash
# Engine / recommendations
BROKER_RECOMMEND_INTERVAL_SECONDS=3600
BROKER_REC_RETENTION_DAYS=7
BROKER_REC_MAX_AGE_HOURS=24
BROKER_PERCENTILE_MIN_SAMPLES=50
BROKER_REC_MAX_INCREASE_FACTOR=2.0
BROKER_REC_MAX_DECREASE_FACTOR=0.5
BROKER_WEBHOOK_TARGET_KINDS=Deployment
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_webhook_manifest.py -v`
Expected: PASS (1 passed)

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(deploy): webhook targets Deployments, engine manifest, sample profile"
```

---

## Task 8: Service-3 cleanup + README refresh + full verification

**Files:**
- Remove: `src/resource_broker/common/services/recommendation_service.py` (on-demand compute — replaced by lookup), `src/resource_broker/algorithms/percentile.py` per-pod DB path already neutered in Service 2 (keep `_parse_resource_value`, used by the engine).
- Modify: `README.md`, `src/resource_broker/api/controllers/recommendations.py` / `profiles.py` if they import removed symbols.

- [ ] **Step 1: Find dead references**

Run: `grep -rn "recommendation_service\|RecommendationService\|ProfileRepository\|compute_patches" src`
Expected: identify importers. `compute_patches` (old patcher) is now unused by the webhook; `recommendation_service` is unused after Task 6.

- [ ] **Step 2: Remove on-demand compute + fix importers**

```bash
git rm src/resource_broker/common/services/recommendation_service.py
git rm src/resource_broker/watcher/services/patcher.py
```

If `api/controllers/recommendations.py` or `profiles.py` import removed modules, replace their bodies with a minimal read-only endpoint over `app.state.crd_cache` (list profiles/strategies) or delete the route and drop it from `app.py`'s `include_router`. Keep `_parse_resource_value` in `algorithms/percentile.py` (the engine imports it); leave the rest of that file as the neutered stub from Service 2.

Run: `uv run python -c "import resource_broker.api.app; import resource_broker.__main__"` → no error.

- [ ] **Step 3: README refresh**

Rewrite the README architecture section to the precompute spine:
- Three processes from one image: `serve` (webhook lookup + REST), `watcher` (Profile/Strategy/Deployment watch + metric scrape), `engine` (cron precompute).
- Two CRDs (`Strategy`, `Profile`) with ref/inline strategy.
- Hash-keyed `service_recommendations`; webhook never computes; pass-through on miss/stale.
- Tables: `profile_snapshot`, `strategy_snapshot`, `pod_performance_metric`, `scrape_state`, `active_services`, `service_recommendations`.
- Staleness mitigations (D1 list).
- Update "Current status / known gaps" to reflect the new spine; remove stale SCD/`watcher.py` sections.

- [ ] **Step 4: Full verification**

Run: `uv run ruff check src tests`
Expected: clean.

Run: `uv run pytest tests/unit/ -q`
Expected: all green.

Run: `uv run alembic upgrade head` (against the compose Postgres) then `uv run alembic downgrade base` and back up, to confirm migrations 0004→0006 apply cleanly. Expected: no errors.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "chore(spine): remove on-demand compute, refresh README, verify migrations"
```

---

## Self-Review

- **Spec coverage (3.a–3.c):** `active_services` + `service_recommendations` tables with the spec's columns + the 3-hash PK — T1. Percentile algo only: fetch active services → join `pod_performance_metric` → resolve profile/strategy from cache → generate recommendation — T4/T5 (D2). Staleness mitigation — hash-gated lookup, freshness TTL, min-samples guard, min/max clamp, bounded-delta clamp, mode gating — D1, implemented across T4 (guards) and T6 (lookup TTL + mode). ✓
- **Webhook invariant:** `build_deployment_response` only calls `lookup` (a `SELECT`) and never invokes any algorithm — compute is engine-only. ✓
- **Dependencies:** consumes Service 1 (`CrdCache`, `build_service_hashes` via `resolve_field_strategy`, `resource_type_hash`, `Profile.is_enforce`) and Service 2 (`PodPerformanceRepository.get_percentiles` with `sample_count`, `configured_resources` shape). Names match those plans' Produces blocks. ✓
- **Type consistency:** `ServiceHashes(profile_hash, strategy_hash, resource_type_hash)` is the single hash-triple type used by `build_service_hashes`, both repos, the engine, and the webhook lookup. `recommendation` JSON is `{field_name: quantity_string}` produced by `compute_recommendation` and consumed by `recommendation_to_patches`. ✓
- **No placeholders:** every code step is complete; every run step lists the command + expected result. ✓
