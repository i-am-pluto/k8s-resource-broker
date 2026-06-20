# Service 2 — Pod Performance Data Collector Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist per-pod resource performance (usage from the metric store + the resources the pod was configured with) into a `pod_performance_metric` table, tagged with the owning service id and the profile/strategy/resource-type hashes, populated by an incremental cron scrape that only fetches data newer than its last run.

**Architecture:** A `watcher`/`scrape` process runs a periodic collector. Each cycle it (1) reads `last_scraped_at` from a `scrape_state` row, (2) queries the metric store for the window `(last_scraped_at, now]`, (3) for each pod sample resolves the owning Deployment via `ownerReferences` to a `service_name`, reads the pod's profile annotation, resolves Profile + default Strategy from the CRD cache (Service 1) to get the three hashes and `configured_resources`, and (4) inserts `pod_performance_metric` rows and advances `scrape_state`.

**Tech Stack:** Python 3.12, SQLAlchemy 2 async + asyncpg, Alembic, kubernetes client, the metric adapter from `common/services/metrics_adapter.py`, structlog, pytest + pytest-asyncio.

## Global Constraints

- Python `>=3.12`; `uv` for deps/commands.
- Ruff line-length `120`, rules `E,F,I,N,W,UP,ANN,B,SIM,ARG`. `uv run ruff check src tests` clean before each commit.
- Config via `BROKER_`-prefixed env vars.
- Content hash = **SHA-256 hex (64 chars)** via `resource_broker.common.hashing.content_hash` (built in Service 1).
- Unit tests run against `sqlite+aiosqlite://` (`tests/conftest.py`); no live Postgres/k8s/Prometheus.
- **Depends on Service 1** for: `content_hash`, `Profile`/`Strategy` models, `ProfileRegistry`/`StrategyRegistry`/`CrdCache`, `resolve_field_strategy`, `ResourceType.resource_type_hash`.
- TDD: failing test → verify fail → minimal impl → verify pass → commit per task.

---

## File Structure

| File | Responsibility |
|---|---|
| `src/resource_broker/common/dao/orm_models.py` (modify) | Drop `PodMetricModel`; add `PodPerformanceMetricModel` + `ScrapeStateModel` |
| `alembic/versions/0005_pod_performance_metric.py` (new) | Drop `pod_metrics`; create `pod_performance_metric` + `scrape_state` |
| `src/resource_broker/common/dao/repositories/performance.py` (new) | `PodPerformanceRepository` — insert + percentiles by service |
| `src/resource_broker/common/dao/repositories/scrape_state.py` (new) | `ScrapeStateRepository` — get/advance last-run watermark |
| `src/resource_broker/common/services/owner_resolver.py` (new) | Pod → ReplicaSet → Deployment → `service_name` resolver |
| `src/resource_broker/watcher/services/collector.py` (rewrite) | Incremental window scrape → `pod_performance_metric` |
| `src/resource_broker/watcher/controllers/scrape_runner.py` (new) | Owns cache + collector loop for the scrape process |
| `src/resource_broker/__main__.py` (modify) | `watcher`/`scrape` subcommand runs the new runner |
| `src/resource_broker/config.py` (modify) | Add scrape window/percentile config vars |
| `deploy/resource-broker/rbac.yaml` (modify) | Add `replicasets`/`deployments` get for owner resolution |
| Tests under `tests/unit/` | One module per new unit |

---

## Design Decisions

### D1 — How the data arrives and is keyed (task 2.a)

- **`performance`** = actual usage pulled from the metric store: `cpu_usage_cores` (double) and `mem_usage_bytes` (bigint). Stored as explicit numeric columns (not a JSONB blob) so Postgres `percentile_cont` runs server-side for the engine (Service 3).
- **`configured_resources`** = the requests/limits the pod was *created with*, read from the live pod spec at scrape time, stored as JSONB (`{"requests": {...}, "limits": {...}}`).
- **Identity columns:** `service_name` (resolved via ownerRef), `pod_name`, `container`, `namespace`, plus `profile_hash`, `strategy_hash`, `resource_type_hash`, `resource_type`. The hashes are resolved from the CRD cache at scrape time so the engine can later join performance to the exact profile/strategy version that was live.
- A pod with no broker profile annotation is skipped (no row) — we only collect for services the broker manages.

### D2 — Incremental fetch (task 2.b — "fetch from last run")

A `scrape_state` table holds one row per `(scraper_name)` with `last_scraped_at`. Each cycle queries the metric store `query_range(start=last_scraped_at, end=now, step=...)` and, on success, advances `last_scraped_at = now`. First run (no row) seeds `last_scraped_at = now - BROKER_SCRAPER_BACKFILL_MINUTES`. This avoids re-pulling the whole history every cycle and makes the scrape idempotent under restarts (a crash before advancing simply re-pulls the same window — duplicate rows are acceptable for percentile aggregation, and a `(service_name, scraped_at)` index keeps reads fast).

---

## Task 1: pod_performance_metric + scrape_state — migration + ORM

**Files:**
- Modify: `src/resource_broker/common/dao/orm_models.py`
- Create: `alembic/versions/0005_pod_performance_metric.py`
- Test: `tests/unit/test_performance_models.py`

**Interfaces:**
- Produces:
  - `PodPerformanceMetricModel` — `id uuid pk`, `namespace`, `service_name`, `pod_name`, `container`, `resource_type`, `resource_type_hash`, `profile_hash`, `strategy_hash`, `cpu_usage_cores double|None`, `mem_usage_bytes bigint|None`, `configured_resources JSONB`, `scraped_at timestamptz` (indexed `(service_name, scraped_at)`).
  - `ScrapeStateModel` — `scraper_name str pk`, `last_scraped_at timestamptz`.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_performance_models.py
from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import select

from resource_broker.common.dao.orm_models import PodPerformanceMetricModel, ScrapeStateModel


@pytest.mark.asyncio
async def test_insert_and_read_performance_row(db_session) -> None:
    row = PodPerformanceMetricModel(
        namespace="default",
        service_name="default/web",
        pod_name="web-abc",
        container="app",
        resource_type="k8s-pod",
        resource_type_hash="rth",
        profile_hash="ph",
        strategy_hash="sh",
        cpu_usage_cores=0.42,
        mem_usage_bytes=123456,
        configured_resources={"requests": {"cpu": "250m"}},
        scraped_at=datetime.now(UTC),
    )
    db_session.add(row)
    await db_session.commit()

    got = (await db_session.execute(select(PodPerformanceMetricModel))).scalars().one()
    assert got.service_name == "default/web"
    assert got.cpu_usage_cores == 0.42
    assert got.configured_resources["requests"]["cpu"] == "250m"


@pytest.mark.asyncio
async def test_scrape_state_row(db_session) -> None:
    db_session.add(ScrapeStateModel(scraper_name="performance", last_scraped_at=datetime.now(UTC)))
    await db_session.commit()
    got = (await db_session.execute(select(ScrapeStateModel))).scalars().one()
    assert got.scraper_name == "performance"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_performance_models.py -v`
Expected: FAIL with `ImportError: cannot import name 'PodPerformanceMetricModel'`

- [ ] **Step 3a: ORM models**

In `src/resource_broker/common/dao/orm_models.py`, **delete** `PodMetricModel` and add:

```python
class PodPerformanceMetricModel(Base):
    __tablename__ = "pod_performance_metric"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    namespace: Mapped[str] = mapped_column(String(253), nullable=False)
    service_name: Mapped[str] = mapped_column(String(253), nullable=False)
    pod_name: Mapped[str] = mapped_column(String(253), nullable=False)
    container: Mapped[str] = mapped_column(String(253), nullable=False)
    resource_type: Mapped[str] = mapped_column(String(253), nullable=False)
    resource_type_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    profile_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    strategy_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    cpu_usage_cores: Mapped[float | None] = mapped_column(Double, nullable=True)
    mem_usage_bytes: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    configured_resources: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    scraped_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        Index("idx_perf_service_time", "service_name", "scraped_at"),
        Index("idx_perf_hashes", "service_name", "resource_type_hash"),
    )


class ScrapeStateModel(Base):
    __tablename__ = "scrape_state"

    scraper_name: Mapped[str] = mapped_column(String(64), primary_key=True)
    last_scraped_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
```

Keep the `text` import (used by `server_default`).

- [ ] **Step 3b: Migration**

```python
# alembic/versions/0005_pod_performance_metric.py
"""Pod performance metric + scrape state; drop legacy pod_metrics.

Revision ID: 0005
Revises: 0004
Create Date: 2026-06-20
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0005"
down_revision: Union[str, None] = "0004"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_table("pod_metrics")

    op.create_table(
        "pod_performance_metric",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("namespace", sa.String(253), nullable=False),
        sa.Column("service_name", sa.String(253), nullable=False),
        sa.Column("pod_name", sa.String(253), nullable=False),
        sa.Column("container", sa.String(253), nullable=False),
        sa.Column("resource_type", sa.String(253), nullable=False),
        sa.Column("resource_type_hash", sa.String(64), nullable=False),
        sa.Column("profile_hash", sa.String(64), nullable=False),
        sa.Column("strategy_hash", sa.String(64), nullable=False),
        sa.Column("cpu_usage_cores", sa.Double(), nullable=True),
        sa.Column("mem_usage_bytes", sa.BigInteger(), nullable=True),
        sa.Column("configured_resources", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("scraped_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_perf_service_time", "pod_performance_metric", ["service_name", "scraped_at"])
    op.create_index("idx_perf_hashes", "pod_performance_metric", ["service_name", "resource_type_hash"])

    op.create_table(
        "scrape_state",
        sa.Column("scraper_name", sa.String(64), nullable=False),
        sa.Column("last_scraped_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("scraper_name"),
    )


def downgrade() -> None:
    op.drop_table("scrape_state")
    op.drop_index("idx_perf_hashes", table_name="pod_performance_metric")
    op.drop_index("idx_perf_service_time", table_name="pod_performance_metric")
    op.drop_table("pod_performance_metric")
```

- [ ] **Step 4: Run test + fix the old metrics repo/tests that referenced `PodMetricModel`**

Run: `uv run pytest tests/unit/test_performance_models.py -v`
Expected: PASS (2 passed)

The old `MetricsRepository` (`repositories/metrics.py`) and the `percentile` algo reference `PodMetricModel`. They are replaced in Tasks 2 and in Service 3. For now keep collection green:

```bash
git rm src/resource_broker/common/dao/repositories/metrics.py
```

Then in `src/resource_broker/algorithms/percentile.py`, replace the body of `PercentileAlgorithm.compute` so it no longer imports `MetricsRepository` — return no-data (the engine in Service 3 owns service-level percentile; the per-pod algo is retired):

```python
    async def compute(self, field, config, context=None):  # noqa: ANN001
        return StrategyResult(value=None, source="percentile-retired")
```

Run: `uv run pytest tests/unit/ -q` → green (collection no longer imports a removed model).

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(dao): migration 0005 — pod_performance_metric + scrape_state"
```

---

## Task 2: Performance repository (insert + percentiles by service)

**Files:**
- Create: `src/resource_broker/common/dao/repositories/performance.py`
- Test: `tests/unit/test_performance_repository.py`

**Interfaces:**
- Produces `PodPerformanceRepository(session)`:
  - `async bulk_insert(rows: list[PodPerformanceMetricModel]) -> None`
  - `async get_percentiles(service_name: str, resource_type_hash: str, lookback_hours: int) -> dict[str, float]` returning keys `cpu_p50/75/90/95`, `mem_p50/75/90/95`, and `sample_count` (int).

`sample_count` lets Service 3 enforce a minimum-samples guard.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_performance_repository.py
from __future__ import annotations

from datetime import UTC, datetime

import pytest

from resource_broker.common.dao.orm_models import PodPerformanceMetricModel
from resource_broker.common.dao.repositories.performance import PodPerformanceRepository


def _row(cpu: float, mem: int) -> PodPerformanceMetricModel:
    return PodPerformanceMetricModel(
        namespace="default", service_name="default/web", pod_name="p", container="app",
        resource_type="k8s-pod", resource_type_hash="rth", profile_hash="ph", strategy_hash="sh",
        cpu_usage_cores=cpu, mem_usage_bytes=mem, configured_resources={}, scraped_at=datetime.now(UTC),
    )


@pytest.mark.asyncio
async def test_percentiles_over_inserted_rows(db_session) -> None:
    repo = PodPerformanceRepository(db_session)
    await repo.bulk_insert([_row(c, c_mem) for c, c_mem in [(0.1, 100), (0.2, 200), (0.3, 300), (0.4, 400)]])
    await db_session.commit()

    pct = await repo.get_percentiles("default/web", "rth", lookback_hours=24)
    assert pct["sample_count"] == 4
    assert 0.1 <= pct["cpu_p50"] <= 0.4
    assert pct["cpu_p95"] >= pct["cpu_p50"]


@pytest.mark.asyncio
async def test_percentiles_empty_returns_zero_samples(db_session) -> None:
    repo = PodPerformanceRepository(db_session)
    pct = await repo.get_percentiles("nope", "rth", lookback_hours=24)
    assert pct["sample_count"] == 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_performance_repository.py -v`
Expected: FAIL with import error for `repositories.performance`.

- [ ] **Step 3: Write minimal implementation**

```python
# src/resource_broker/common/dao/repositories/performance.py
from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from resource_broker.common.dao.orm_models import PodPerformanceMetricModel as M


class PodPerformanceRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def bulk_insert(self, rows: list[M]) -> None:
        self._session.add_all(rows)
        await self._session.flush()

    async def get_percentiles(
        self, service_name: str, resource_type_hash: str, lookback_hours: int = 24
    ) -> dict[str, float]:
        since = datetime.now(UTC) - timedelta(hours=lookback_hours)
        cpu = M.cpu_usage_cores
        mem = M.mem_usage_bytes
        stmt = select(
            func.percentile_cont(0.50).within_group(cpu).label("cpu_p50"),
            func.percentile_cont(0.75).within_group(cpu).label("cpu_p75"),
            func.percentile_cont(0.90).within_group(cpu).label("cpu_p90"),
            func.percentile_cont(0.95).within_group(cpu).label("cpu_p95"),
            func.percentile_cont(0.50).within_group(mem).label("mem_p50"),
            func.percentile_cont(0.75).within_group(mem).label("mem_p75"),
            func.percentile_cont(0.90).within_group(mem).label("mem_p90"),
            func.percentile_cont(0.95).within_group(mem).label("mem_p95"),
            func.count().label("n"),
        ).where(
            M.service_name == service_name,
            M.resource_type_hash == resource_type_hash,
            M.scraped_at >= since,
            cpu.isnot(None),
            mem.isnot(None),
        )
        row = (await self._session.execute(stmt)).one_or_none()
        if row is None or row.n == 0:
            return {"sample_count": 0}
        return {
            "cpu_p50": float(row.cpu_p50), "cpu_p75": float(row.cpu_p75),
            "cpu_p90": float(row.cpu_p90), "cpu_p95": float(row.cpu_p95),
            "mem_p50": float(row.mem_p50), "mem_p75": float(row.mem_p75),
            "mem_p90": float(row.mem_p90), "mem_p95": float(row.mem_p95),
            "sample_count": int(row.n),
        }
```

Note: SQLite's `percentile_cont` is unavailable, but aiosqlite + SQLAlchemy emit the function and SQLite returns `NULL`/errors only if the function is unknown. To keep this test backend-portable, the test asserts ranges; if the CI sqlite build lacks `percentile_cont`, gate this test with `@pytest.mark.postgres` and run it against the compose Postgres. **Default:** keep the test; if it errors on sqlite, wrap the percentile selects with a sqlite fallback that uses `func.avg`/`func.max` — but prefer running this repo test against Postgres in CI.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_performance_repository.py -v`
Expected: PASS (2 passed) on Postgres-backed CI, or the documented sqlite handling.

- [ ] **Step 5: Commit**

```bash
git add src/resource_broker/common/dao/repositories/performance.py tests/unit/test_performance_repository.py
git commit -m "feat(dao): PodPerformanceRepository with by-service percentiles"
```

---

## Task 3: Scrape-state repository (incremental watermark)

**Files:**
- Create: `src/resource_broker/common/dao/repositories/scrape_state.py`
- Test: `tests/unit/test_scrape_state_repository.py`

**Interfaces:**
- Produces `ScrapeStateRepository(session)`:
  - `async get_last(scraper_name: str) -> datetime | None`
  - `async advance(scraper_name: str, watermark: datetime) -> None` (upsert)

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_scrape_state_repository.py
from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from resource_broker.common.dao.repositories.scrape_state import ScrapeStateRepository


@pytest.mark.asyncio
async def test_get_last_none_then_advance(db_session) -> None:
    repo = ScrapeStateRepository(db_session)
    assert await repo.get_last("performance") is None

    t1 = datetime.now(UTC) - timedelta(minutes=5)
    await repo.advance("performance", t1)
    await db_session.commit()
    assert await repo.get_last("performance") is not None

    t2 = datetime.now(UTC)
    await repo.advance("performance", t2)
    await db_session.commit()
    got = await repo.get_last("performance")
    assert got >= t1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_scrape_state_repository.py -v`
Expected: FAIL with import error.

- [ ] **Step 3: Write minimal implementation**

```python
# src/resource_broker/common/dao/repositories/scrape_state.py
from __future__ import annotations

from datetime import datetime

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from resource_broker.common.dao.orm_models import ScrapeStateModel


class ScrapeStateRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_last(self, scraper_name: str) -> datetime | None:
        stmt = select(ScrapeStateModel.last_scraped_at).where(ScrapeStateModel.scraper_name == scraper_name)
        return (await self._session.execute(stmt)).scalar_one_or_none()

    async def advance(self, scraper_name: str, watermark: datetime) -> None:
        dialect = self._session.bind.dialect.name if self._session.bind else "postgresql"
        insert = sqlite_insert if dialect == "sqlite" else pg_insert
        stmt = insert(ScrapeStateModel).values(scraper_name=scraper_name, last_scraped_at=watermark)
        stmt = stmt.on_conflict_do_update(
            index_elements=["scraper_name"], set_={"last_scraped_at": stmt.excluded.last_scraped_at}
        )
        await self._session.execute(stmt)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_scrape_state_repository.py -v`
Expected: PASS (1 passed)

- [ ] **Step 5: Commit**

```bash
git add src/resource_broker/common/dao/repositories/scrape_state.py tests/unit/test_scrape_state_repository.py
git commit -m "feat(dao): ScrapeStateRepository incremental watermark"
```

---

## Task 4: Owner resolver (Pod → ReplicaSet → Deployment → service_name)

**Files:**
- Create: `src/resource_broker/common/services/owner_resolver.py`
- Test: `tests/unit/test_owner_resolver.py`

**Interfaces:**
- Produces `OwnerResolver(apps_api, core_api=None)`:
  - `resolve_service_name(pod: dict) -> str | None` — walk `ownerReferences` Pod→ReplicaSet→Deployment; return `"{namespace}/{deployment_name}"` or `None` if not Deployment-owned. ReplicaSet→Deployment lookups are cached in-process per cycle.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_owner_resolver.py
from __future__ import annotations

from resource_broker.common.services.owner_resolver import OwnerResolver


class _RS:
    def __init__(self, owner_kind, owner_name):
        self.metadata = type("M", (), {"owner_references": [type("O", (), {"kind": owner_kind, "name": owner_name})()]})


class _AppsApi:
    def __init__(self, rs_owner=("Deployment", "web")):
        self._rs = _RS(*rs_owner)

    def read_namespaced_replica_set(self, name, namespace):  # noqa: ARG002
        return self._rs


def _pod(owner_kind="ReplicaSet", owner_name="web-5d8") -> dict:
    return {
        "metadata": {
            "name": "web-5d8-abc", "namespace": "default",
            "ownerReferences": [{"kind": owner_kind, "name": owner_name}],
        }
    }


def test_resolve_via_replicaset() -> None:
    r = OwnerResolver(apps_api=_AppsApi())
    assert r.resolve_service_name(_pod()) == "default/web"


def test_no_owner_returns_none() -> None:
    r = OwnerResolver(apps_api=_AppsApi())
    assert r.resolve_service_name({"metadata": {"name": "bare", "namespace": "default"}}) is None


def test_replicaset_not_deployment_owned_returns_none() -> None:
    r = OwnerResolver(apps_api=_AppsApi(rs_owner=("Something", "x")))
    assert r.resolve_service_name(_pod()) is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_owner_resolver.py -v`
Expected: FAIL with import error.

- [ ] **Step 3: Write minimal implementation**

```python
# src/resource_broker/common/services/owner_resolver.py
from __future__ import annotations

from typing import Any

from structlog import get_logger

logger = get_logger(__name__)


class OwnerResolver:
    def __init__(self, apps_api: Any) -> None:
        self._apps = apps_api
        self._rs_cache: dict[tuple[str, str], str | None] = {}

    def reset_cycle_cache(self) -> None:
        self._rs_cache.clear()

    def resolve_service_name(self, pod: dict[str, Any]) -> str | None:
        meta = pod.get("metadata", {}) or {}
        namespace = meta.get("namespace", "default")
        owners = meta.get("ownerReferences", []) or []
        rs_name = next((o.get("name") for o in owners if o.get("kind") == "ReplicaSet"), None)
        if rs_name is None:
            return None

        deploy_name = self._replicaset_deployment(rs_name, namespace)
        if deploy_name is None:
            return None
        return f"{namespace}/{deploy_name}"

    def _replicaset_deployment(self, rs_name: str, namespace: str) -> str | None:
        cache_key = (namespace, rs_name)
        if cache_key in self._rs_cache:
            return self._rs_cache[cache_key]
        deploy_name: str | None = None
        try:
            rs = self._apps.read_namespaced_replica_set(name=rs_name, namespace=namespace)
            for o in (rs.metadata.owner_references or []):
                if o.kind == "Deployment":
                    deploy_name = o.name
                    break
        except Exception:
            logger.warning("failed to read replicaset for owner resolution", rs=rs_name, namespace=namespace)
        self._rs_cache[cache_key] = deploy_name
        return deploy_name
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_owner_resolver.py -v`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
git add src/resource_broker/common/services/owner_resolver.py tests/unit/test_owner_resolver.py
git commit -m "feat(collector): ownerRef Pod->ReplicaSet->Deployment service resolver"
```

---

## Task 5: Collector — incremental window scrape → pod_performance_metric

**Files:**
- Rewrite: `src/resource_broker/watcher/services/collector.py`
- Modify: `src/resource_broker/config.py`
- Test: `tests/unit/test_collector.py`

**Interfaces:**
- Consumes: `MetricsAdapter`, `OwnerResolver` (T4), `CrdCache` (Service 1), `PodPerformanceRepository` (T2), `ScrapeStateRepository` (T3), `resolve_field_strategy` (Service 1), `ResourceType.resource_type_hash` (Service 1).
- Produces: `PerformanceCollector(adapter, cache, owner_resolver, core_api)` with:
  - `build_rows(pods: list[dict], cpu_by_pod: dict, mem_by_pod: dict) -> list[PodPerformanceMetricModel]` (pure, unit-tested)
  - `async run_cycle() -> int` (returns rows written)
  - `async run_forever() -> None`
- New config: `scraper_backfill_minutes` (default 60), `scraper_step_seconds` (default 60). `scraper_interval_seconds` already exists.

- [ ] **Step 1: Write the failing test** (pure `build_rows` — the IO loop is covered by the integration plan)

```python
# tests/unit/test_collector.py
from __future__ import annotations

from resource_broker.common.services.crd_cache import CrdCache
from resource_broker.common.services.profile_registry import ProfileRegistry
from resource_broker.common.services.strategy_registry import StrategyRegistry
from resource_broker.watcher.services.collector import PerformanceCollector
from resource_broker.config import settings


class _AppsApi:
    def read_namespaced_replica_set(self, name, namespace):  # noqa: ARG002
        return type("RS", (), {"metadata": type("M", (), {
            "owner_references": [type("O", (), {"kind": "Deployment", "name": "web"})()]
        })})


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


def _pod() -> dict:
    return {
        "metadata": {
            "name": "web-5d8-abc", "namespace": "default",
            "annotations": {settings.profile_annotation_key: "web-profile"},
            "ownerReferences": [{"kind": "ReplicaSet", "name": "web-5d8"}],
        },
        "spec": {"containers": [{"name": "app", "resources": {"requests": {"cpu": "250m", "memory": "256Mi"}}}]},
    }


def test_build_rows_tags_service_and_hashes() -> None:
    cache = _cache()
    from resource_broker.common.services.owner_resolver import OwnerResolver

    collector = PerformanceCollector(adapter=None, cache=cache, owner_resolver=OwnerResolver(_AppsApi()), core_api=None)
    rows = collector.build_rows(
        pods=[_pod()],
        cpu_by_pod={("default", "web-5d8-abc", "app"): 0.4},
        mem_by_pod={("default", "web-5d8-abc", "app"): 268435456},
    )
    assert len(rows) == 1
    r = rows[0]
    assert r.service_name == "default/web"
    assert r.resource_type == "k8s-pod"
    assert len(r.profile_hash) == 64
    assert len(r.strategy_hash) == 64
    assert len(r.resource_type_hash) == 64
    assert r.cpu_usage_cores == 0.4
    assert r.configured_resources["requests"]["cpu"] == "250m"


def test_build_rows_skips_unannotated_pod() -> None:
    cache = _cache()
    from resource_broker.common.services.owner_resolver import OwnerResolver

    collector = PerformanceCollector(adapter=None, cache=cache, owner_resolver=OwnerResolver(_AppsApi()), core_api=None)
    pod = _pod()
    pod["metadata"]["annotations"] = {}
    assert collector.build_rows([pod], {}, {}) == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_collector.py -v`
Expected: FAIL — `PerformanceCollector` not defined (old `MetricsCollector` still in file).

- [ ] **Step 3a: Config vars**

In `src/resource_broker/config.py`, under the `# ── Scraper ──` block add:

```python
    scraper_backfill_minutes: int = 60
    scraper_step_seconds: int = 60
```

- [ ] **Step 3b: Rewrite the collector**

```python
# src/resource_broker/watcher/services/collector.py
from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

from structlog import get_logger

from resource_broker.common.dao.database import get_session
from resource_broker.common.dao.orm_models import PodPerformanceMetricModel
from resource_broker.common.dao.repositories.performance import PodPerformanceRepository
from resource_broker.common.dao.repositories.scrape_state import ScrapeStateRepository
from resource_broker.common.services.crd_cache import CrdCache
from resource_broker.common.services.metrics_adapter import MetricsAdapter
from resource_broker.common.services.owner_resolver import OwnerResolver
from resource_broker.common.services.strategy_resolver import resolve_field_strategy
from resource_broker.config import settings
from resource_broker.resource_types.registry import resource_type_registry

logger = get_logger(__name__)

_SCRAPER_NAME = "performance"


def _cpu_query() -> str:
    return "rate(container_cpu_usage_seconds_total[5m])"


def _mem_query() -> str:
    return "container_memory_working_set_bytes"


def _index_by_pod(samples: list[Any]) -> dict[tuple[str, str, str], float]:
    out: dict[tuple[str, str, str], float] = {}
    for s in samples:
        key = (s.metric.get("namespace", ""), s.metric.get("pod", ""), s.metric.get("container", ""))
        out[key] = s.value
    return out


class PerformanceCollector:
    def __init__(
        self,
        adapter: MetricsAdapter | None,
        cache: CrdCache,
        owner_resolver: OwnerResolver,
        core_api: Any,
    ) -> None:
        self._adapter = adapter
        self._cache = cache
        self._owner = owner_resolver
        self._core = core_api

    def _service_strategy_hash(self, profile) -> str:  # noqa: ANN001
        """Hash of the profile's default strategy (the service-level strategy)."""
        for field_name in profile.fields:
            s = resolve_field_strategy(profile, field_name, self._cache.strategies)
            if s is not None:
                return s.strategy_hash
        return ""

    def build_rows(
        self,
        pods: list[dict[str, Any]],
        cpu_by_pod: dict[tuple[str, str, str], float],
        mem_by_pod: dict[tuple[str, str, str], float],
    ) -> list[PodPerformanceMetricModel]:
        rows: list[PodPerformanceMetricModel] = []
        now = datetime.now(UTC)
        for pod in pods:
            meta = pod.get("metadata", {}) or {}
            annotations = meta.get("annotations", {}) or {}
            profile_name = annotations.get(settings.profile_annotation_key)
            if not profile_name:
                continue
            namespace = meta.get("namespace", "default")
            profile = self._cache.profiles.get(profile_name, namespace)
            if profile is None:
                continue
            service_name = self._owner.resolve_service_name(pod)
            if service_name is None:
                continue

            rtype = resource_type_registry.get(profile.resource_type)
            rt_hash = rtype.resource_type_hash
            profile_hash = profile.profile_hash
            strategy_hash = self._service_strategy_hash(profile)

            for container in (pod.get("spec", {}) or {}).get("containers", []) or []:
                cname = container.get("name", "")
                key = (namespace, meta.get("name", ""), cname)
                rows.append(
                    PodPerformanceMetricModel(
                        namespace=namespace,
                        service_name=service_name,
                        pod_name=meta.get("name", ""),
                        container=cname,
                        resource_type=profile.resource_type,
                        resource_type_hash=rt_hash,
                        profile_hash=profile_hash,
                        strategy_hash=strategy_hash,
                        cpu_usage_cores=cpu_by_pod.get(key),
                        mem_usage_bytes=int(mem_by_pod[key]) if key in mem_by_pod else None,
                        configured_resources=container.get("resources", {}) or {},
                        scraped_at=now,
                    )
                )
        return rows

    async def run_cycle(self) -> int:
        async with get_session() as session:
            state = ScrapeStateRepository(session)
            last = await state.get_last(_SCRAPER_NAME)
        now = datetime.now(UTC)
        start = last or (now - timedelta(minutes=settings.scraper_backfill_minutes))

        cpu_samples = await self._adapter.query_range(
            _cpu_query(), start=start.timestamp(), end=now.timestamp(), step=f"{settings.scraper_step_seconds}s"
        )
        mem_samples = await self._adapter.query_range(
            _mem_query(), start=start.timestamp(), end=now.timestamp(), step=f"{settings.scraper_step_seconds}s"
        )

        self._owner.reset_cycle_cache()
        pods = self._list_pods()
        rows = self.build_rows(pods, _index_by_pod(cpu_samples), _index_by_pod(mem_samples))

        if rows:
            async with get_session() as session:
                await PodPerformanceRepository(session).bulk_insert(rows)
        async with get_session() as session:
            await ScrapeStateRepository(session).advance(_SCRAPER_NAME, now)

        logger.info("performance scrape cycle complete", rows=len(rows), window_start=start.isoformat())
        return len(rows)

    def _list_pods(self) -> list[dict[str, Any]]:
        if settings.watch_namespace:
            resp = self._core.list_namespaced_pod(namespace=settings.watch_namespace, _preload_content=False)
        else:
            resp = self._core.list_pod_for_all_namespaces(_preload_content=False)
        import json

        return json.loads(resp.data).get("items", [])

    async def run_forever(self) -> None:
        logger.info("performance collector started", interval=settings.scraper_interval_seconds)
        while True:
            try:
                await self.run_cycle()
            except Exception:
                logger.exception("performance scrape cycle failed")
            await asyncio.sleep(settings.scraper_interval_seconds)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_collector.py -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(collector): incremental window scrape -> pod_performance_metric"
```

---

## Task 6: Scrape process runner + `__main__` wiring + RBAC

**Files:**
- Create: `src/resource_broker/watcher/controllers/scrape_runner.py`
- Modify: `src/resource_broker/__main__.py`
- Modify: `deploy/resource-broker/rbac.yaml`, `.env.example`
- Test: `tests/unit/test_scrape_runner_build.py`

**Interfaces:**
- Produces `ScrapeRunner` with `async run()` that: builds `CrdCache`, bootstraps it, starts the Service-1 watch loops (so hashes stay current), and runs `PerformanceCollector.run_forever()`.

- [ ] **Step 1: Write the failing test** (constructor wiring, no live IO)

```python
# tests/unit/test_scrape_runner_build.py
from __future__ import annotations

from resource_broker.watcher.controllers.scrape_runner import ScrapeRunner


def test_runner_constructs_with_cache_and_collector() -> None:
    runner = ScrapeRunner.build_for_test()
    assert runner.cache is not None
    assert runner.collector is not None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_scrape_runner_build.py -v`
Expected: FAIL with import error for `scrape_runner`.

- [ ] **Step 3a: Runner**

```python
# src/resource_broker/watcher/controllers/scrape_runner.py
from __future__ import annotations

import asyncio

from kubernetes import client as k8s_client
from structlog import get_logger

from resource_broker.common.k8s_client import create_k8s_api
from resource_broker.common.services.crd_cache import CrdCache
from resource_broker.common.services.metrics_factory import create_metrics_adapter
from resource_broker.common.services.owner_resolver import OwnerResolver
from resource_broker.common.services.profile_registry import ProfileRegistry
from resource_broker.common.services.strategy_registry import StrategyRegistry
from resource_broker.config import settings
from resource_broker.watcher.controllers.crd_watcher import run_crd_watch_loops
from resource_broker.watcher.services.collector import PerformanceCollector

logger = get_logger(__name__)


class ScrapeRunner:
    def __init__(self, cache: CrdCache, collector: PerformanceCollector, co_api) -> None:  # noqa: ANN001
        self.cache = cache
        self.collector = collector
        self._co_api = co_api

    @classmethod
    def build(cls) -> ScrapeRunner:
        cache = CrdCache(ProfileRegistry(), StrategyRegistry())
        adapter = create_metrics_adapter(settings)
        apps_api = create_k8s_api(k8s_client.AppsV1Api)
        core_api = create_k8s_api(k8s_client.CoreV1Api)
        co_api = create_k8s_api(k8s_client.CustomObjectsApi)
        collector = PerformanceCollector(
            adapter=adapter, cache=cache, owner_resolver=OwnerResolver(apps_api), core_api=core_api
        )
        return cls(cache=cache, collector=collector, co_api=co_api)

    @classmethod
    def build_for_test(cls) -> ScrapeRunner:
        cache = CrdCache(ProfileRegistry(), StrategyRegistry())
        collector = PerformanceCollector(adapter=None, cache=cache, owner_resolver=OwnerResolver(None), core_api=None)
        return cls(cache=cache, collector=collector, co_api=None)

    async def run(self) -> None:
        await self.cache.bootstrap(self._co_api)
        watch_task = asyncio.create_task(
            run_crd_watch_loops(self._co_api, self.cache, settings.cache_resync_seconds)
        )
        try:
            await self.collector.run_forever()
        finally:
            watch_task.cancel()
            if self.collector._adapter is not None:
                await self.collector._adapter.close()
```

- [ ] **Step 3b: `__main__` wiring**

In `src/resource_broker/__main__.py`, replace the `controller`/`scrape` commands with:

```python
@app.command()
def watcher() -> None:
    """Run the metric-performance scrape process."""
    configure_logging()
    from resource_broker.watcher.controllers.scrape_runner import ScrapeRunner

    asyncio.run(ScrapeRunner.build().run())


@app.command()
def scrape() -> None:
    """Alias for `watcher`."""
    watcher()
```

Remove the old `controller` command and its `PodWatcher` import.

- [ ] **Step 3c: RBAC + env**

In `deploy/resource-broker/rbac.yaml`, add to the ClusterRole rules:

```yaml
  - apiGroups: ["apps"]
    resources: ["deployments", "replicasets"]
    verbs: ["get", "list", "watch"]
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list", "watch"]
```

In `.env.example`, add:

```bash
# Performance scrape
BROKER_SCRAPER_INTERVAL_SECONDS=60
BROKER_SCRAPER_BACKFILL_MINUTES=60
BROKER_SCRAPER_STEP_SECONDS=60
```

- [ ] **Step 4: Run test + import check**

Run: `uv run pytest tests/unit/test_scrape_runner_build.py -v`
Expected: PASS (1 passed)

Run: `uv run python -c "import resource_broker.__main__"` → no error.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(scrape): scrape process runner + __main__ watcher subcommand + RBAC"
```

---

## Task 7: Service-2 cleanup + verification

**Files:**
- Remove: `src/resource_broker/watcher/controllers/watcher.py` (legacy per-pod enforcement — superseded; pod-level enforce is P3) **only if** nothing imports it.
- Modify: `README.md`

- [ ] **Step 1: Confirm no live import of `watcher.py`**

Run: `grep -rn "controllers.watcher\b\|PodWatcher" src` — expected: no hits after Task 6 (only the deleted `controller` command referenced it).

- [ ] **Step 2: Remove the legacy watcher**

```bash
git rm src/resource_broker/watcher/controllers/watcher.py
```

Run: `uv run python -c "import resource_broker.__main__; import resource_broker.api.app"` → no error.

- [ ] **Step 3: README note**

In `README.md`, replace the "How `watcher.py` works" section with a short "Performance collector" section: the `watcher`/`scrape` process incrementally pulls usage from the metric store for the window since its last run, resolves each pod's owning Deployment + profile/strategy hashes from the CRD cache, and writes `pod_performance_metric`. Mention `scrape_state` tracks the watermark.

- [ ] **Step 4: Full lint + unit run**

Run: `uv run ruff check src tests`
Expected: clean.

Run: `uv run pytest tests/unit/ -q`
Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "chore(scrape): retire legacy per-pod watcher, doc collector"
```

---

## Self-Review

- **Spec coverage (2.a, 2.b):** `pod_performance_metric` columns cover profile/strategy/resource-type hashes + service id + pod + configured-resources + performance (cpu/mem usage) — D1, T1. Data sourcing: usage from metric store, configured from pod spec, hashes from CRD cache — T5. Incremental "fetch from last run" via `scrape_state` watermark + `query_range(start=last, end=now)` — D2, T3/T5. Cron loop `run_forever` + process wiring — T6. ✓
- **Dependency on Service 1:** uses `content_hash` (via model `*_hash` props), `CrdCache`, `resolve_field_strategy`, `resource_type_hash`. All exist after Service 1. ✓
- **Type consistency:** `PodPerformanceRepository.get_percentiles(service_name, resource_type_hash, lookback_hours)` returns `sample_count` consumed by Service 3; `PodPerformanceMetricModel` columns match the migration; `OwnerResolver.resolve_service_name(pod) -> "{ns}/{name}"` matches the engine's `service_name` join key in Service 3. ✓
- **No placeholders:** every code step is complete; every run step lists command + expected output. ✓
