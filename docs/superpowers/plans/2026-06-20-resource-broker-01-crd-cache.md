# Service 1 — Profile/Strategy CRD Cache Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split the single `ResourceProfile` CRD into two CRDs (`Strategy` + `Profile`), and build a per-process in-memory cache for both that bootstraps from the Kubernetes API, stays current via watch streams + periodic resync, and falls back to a Postgres snapshot when the Kubernetes API is unavailable at cold start.

**Architecture:** Each broker process (`serve`, later `watcher`/`engine`) holds its own `ProfileRegistry` + `StrategyRegistry`. On startup it lists both CRD kinds from the k8s API, computes a SHA-256 content hash per object, and write-through-persists each as a JSONB snapshot row. If the k8s `list` fails, it rebuilds the cache from the last snapshot rows. Two watch loops keep the caches live; a resync timer re-lists every `CACHE_RESYNC_SECONDS`. A `Profile` references a `Strategy` by name, with an inline-strategy fallback so a user can interact with `Profile` alone.

**Tech Stack:** Python 3.12, FastAPI, SQLAlchemy 2 async + asyncpg, Alembic, kubernetes client, Pydantic v2, structlog, pytest + pytest-asyncio.

## Global Constraints

- Python `>=3.12`; dependency management via `uv` (`uv sync`, `uv run ...`).
- Ruff line-length `120`, target `py312`; lint rules `E,F,I,N,W,UP,ANN,B,SIM,ARG`. Run `uv run ruff check src tests` clean before every commit.
- All config via env vars prefixed `BROKER_` (pydantic-settings `env_prefix="BROKER_"`).
- CRD API group `resource-broker.io`, version `v1alpha1`.
- A content hash is **SHA-256 hex (64 chars) of the canonical JSON** of an object: `json.dumps(obj, sort_keys=True, separators=(",", ":"))`. One helper produces every hash in the system.
- Unit tests run against `sqlite+aiosqlite://` (see `tests/conftest.py`); they must not require a live Postgres or k8s cluster.
- TDD: write the failing test first, watch it fail, implement minimally, watch it pass, commit. Commit after each task.

---

## File Structure

| File | Responsibility |
|---|---|
| `src/resource_broker/common/hashing.py` (new) | `canonical_json()` + `content_hash()` — the single hashing primitive |
| `src/resource_broker/common/models/strategy.py` (modify) | New `Strategy` domain model (`from_crd`, `to_canonical`, `strategy_hash`); keep `StrategyResult` |
| `src/resource_broker/common/models/profile.py` (modify) | New `Profile` + `FieldSpec` models with ref/inline strategy + `profile_hash`; keep legacy `ResourceProfile` until consumers migrate |
| `src/resource_broker/resource_types/base.py` (modify) | Add `version`, `resource_type_hash` to `ResourceType` |
| `src/resource_broker/resource_types/k8s_resources.py` (modify) | Set `version` on `k8s-pod` |
| `src/resource_broker/common/dao/orm_models.py` (modify) | Add `ProfileSnapshotModel` + `StrategySnapshotModel`; drop the 3 SCD models |
| `src/resource_broker/common/dao/repositories/snapshots.py` (new) | `SnapshotRepository` — upsert + list profile/strategy snapshots |
| `src/resource_broker/common/services/strategy_registry.py` (new) | `StrategyRegistry` — in-memory `{ns/name -> Strategy}` cache |
| `src/resource_broker/common/services/profile_registry.py` (rewrite) | `ProfileRegistry` — in-memory `{ns/name -> Profile}` cache |
| `src/resource_broker/common/services/crd_cache.py` (new) | `CrdCache` — owns both registries; bootstrap/snapshot/resync orchestration |
| `src/resource_broker/common/services/strategy_resolver.py` (new) | Resolve a field's effective `Strategy` (inline > ref > default) |
| `src/resource_broker/watcher/controllers/crd_watcher.py` (rewrite) | Two watch loops (Profile + Strategy) feeding the cache |
| `src/resource_broker/api/app.py` (modify) | Lifespan builds `CrdCache`, bootstraps, starts watch + resync tasks |
| `src/resource_broker/config.py` (modify) | Add `cache_resync_seconds` |
| `alembic/versions/0004_crd_cache_snapshots.py` (new) | Drop SCD tables; create `profile_snapshot` + `strategy_snapshot` |
| `deploy/crd/strategy-crd.yaml` (new) | `Strategy` CRD |
| `deploy/crd/profile-crd.yaml` (new) | `Profile` CRD (replaces `resourceprofile-crd.yaml`) |
| `deploy/samples/strategy-percentile-p75.yaml`, `deploy/samples/profile-k8s-aggressive.yaml` (new) | Samples |
| `deploy/resource-broker/rbac.yaml` (modify) | Add `strategies`/`profiles` to the watch ClusterRole |
| Tests under `tests/unit/` | One test module per new unit |

---

## Design Decisions

### D1 — Two CRDs vs one (exploration 1.a)

**Chosen: two CRDs (`Strategy` + `Profile`), Profile references Strategy by name, with an inline-strategy fallback.**

| | Separate CRDs (chosen) | Single CRD (rejected) |
|---|---|---|
| Reuse | One `percentile-p75` Strategy shared by many Profiles (DRY) | Strategy copy-pasted into every Profile |
| Blast radius on change | Editing a Strategy recomputes only its dependents; Profile hash unchanged otherwise | Any tweak rewrites the whole Profile, invalidates everything |
| Hash granularity | `strategy_hash` + `profile_hash` are independent cache keys | One coarse hash |
| RBAC / lifecycle | Strategy and Profile can have separate owners | Coupled |
| `algo: image` future | Clean home on Strategy | Bloats Profile |
| Cost | Two objects, dangling-ref risk, two watch streams, apply ordering | One object, no dangling refs, simplest UX |

**"User interacts with Profile only" mitigation:** a Profile's `default-strategy` (and any `fields.<f>.strategy`) may be **either** a string name (`strategyRef`) **or** an inline strategy object. When inline, the broker synthesizes an anonymous `Strategy` and hashes it exactly like a named one. This gives single-CRD ergonomics on top of the two-CRD model — a user who never wants to manage Strategy objects writes everything inline in the Profile and it still works.

### D2 — Snapshot store (task 1.d)

Cold-start fallback uses two plain JSONB tables (`profile_snapshot`, `strategy_snapshot`) keyed by `(namespace, name)`, each storing the canonical spec + its hash. This **replaces** the SCD-Type-2 profile tables from migration 0003 (history is out of scope for the cache; the snapshot only needs the latest spec to rebuild the cache cold). Write-through happens on every successful bootstrap and every watch upsert, so the snapshot is never more than one watch event behind k8s.

---

## Task 1: Canonical hashing primitive

**Files:**
- Create: `src/resource_broker/common/hashing.py`
- Test: `tests/unit/test_hashing.py`

**Interfaces:**
- Produces: `canonical_json(obj: Any) -> str`, `content_hash(obj: Any) -> str` (64-char sha256 hex). Every other task imports `content_hash` from here.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_hashing.py
from __future__ import annotations

from resource_broker.common.hashing import canonical_json, content_hash


def test_canonical_json_is_key_order_stable() -> None:
    a = {"b": 1, "a": {"d": 4, "c": 3}}
    b = {"a": {"c": 3, "d": 4}, "b": 1}
    assert canonical_json(a) == canonical_json(b)


def test_content_hash_is_64_char_hex_and_stable() -> None:
    h1 = content_hash({"x": 1, "y": [1, 2, 3]})
    h2 = content_hash({"y": [1, 2, 3], "x": 1})
    assert h1 == h2
    assert len(h1) == 64
    assert all(c in "0123456789abcdef" for c in h1)


def test_content_hash_changes_when_content_changes() -> None:
    assert content_hash({"x": 1}) != content_hash({"x": 2})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_hashing.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'resource_broker.common.hashing'`

- [ ] **Step 3: Write minimal implementation**

```python
# src/resource_broker/common/hashing.py
from __future__ import annotations

import hashlib
import json
from typing import Any


def canonical_json(obj: Any) -> str:
    """Deterministic JSON: sorted keys, no insignificant whitespace."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)


def content_hash(obj: Any) -> str:
    """SHA-256 hex (64 chars) of the canonical JSON of ``obj``."""
    return hashlib.sha256(canonical_json(obj).encode()).hexdigest()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_hashing.py -v`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
git add src/resource_broker/common/hashing.py tests/unit/test_hashing.py
git commit -m "feat(common): canonical JSON hashing primitive"
```

---

## Task 2: Strategy domain model

**Files:**
- Modify: `src/resource_broker/common/models/strategy.py`
- Test: `tests/unit/test_strategy_model.py`

**Interfaces:**
- Consumes: `content_hash` (Task 1).
- Produces:
  - `Strategy(name: str, namespace: str, algo: str, args: dict[str, Any], image: str | None = None)`
  - `Strategy.from_crd(crd: dict) -> Strategy`
  - `Strategy.from_inline(d: dict, namespace: str, name: str = "<inline>") -> Strategy`
  - `Strategy.to_canonical() -> dict` → `{"algo", "args", "image"}`
  - `Strategy.strategy_hash` (property) → 64-char hex
  - `Strategy.is_builtin` (property) → `algo != "image"`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_strategy_model.py
from __future__ import annotations

from resource_broker.common.models.strategy import Strategy


def _crd() -> dict:
    return {
        "metadata": {"name": "percentile-p75", "namespace": "default"},
        "spec": {"algo": "percentile", "args": {"percentile": 75, "lookback_hours": 168}},
    }


def test_from_crd_parses_fields() -> None:
    s = Strategy.from_crd(_crd())
    assert s.name == "percentile-p75"
    assert s.namespace == "default"
    assert s.algo == "percentile"
    assert s.args == {"percentile": 75, "lookback_hours": 168}
    assert s.image is None
    assert s.is_builtin is True


def test_hash_ignores_identity_and_is_stable() -> None:
    a = Strategy(name="x", namespace="ns1", algo="percentile", args={"percentile": 75})
    b = Strategy(name="y", namespace="ns2", algo="percentile", args={"percentile": 75})
    assert a.strategy_hash == b.strategy_hash  # name/namespace excluded from identity
    assert len(a.strategy_hash) == 64


def test_hash_changes_with_args() -> None:
    a = Strategy(name="x", namespace="d", algo="percentile", args={"percentile": 75})
    b = Strategy(name="x", namespace="d", algo="percentile", args={"percentile": 90})
    assert a.strategy_hash != b.strategy_hash


def test_image_strategy_is_not_builtin() -> None:
    s = Strategy.from_crd(
        {"metadata": {"name": "ml", "namespace": "d"}, "spec": {"algo": "image", "image": "ex/algo:1"}}
    )
    assert s.is_builtin is False
    assert s.image == "ex/algo:1"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_strategy_model.py -v`
Expected: FAIL with `ImportError: cannot import name 'Strategy'`

- [ ] **Step 3: Write minimal implementation**

Append to `src/resource_broker/common/models/strategy.py` (keep the existing `StrategyResult` class):

```python
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from resource_broker.common.hashing import content_hash


class StrategyResult:
    def __init__(self, value: Any, source: str = "static") -> None:
        self.value: Any = value
        self.source: str = source


@dataclass
class Strategy:
    """A reusable recommendation strategy (the `Strategy` CRD, or an inline block)."""

    name: str
    namespace: str
    algo: str
    args: dict[str, Any] = field(default_factory=dict)
    image: str | None = None

    @classmethod
    def from_crd(cls, crd: dict[str, Any]) -> Strategy:
        meta = crd.get("metadata", {}) or {}
        spec = crd.get("spec", {}) or {}
        return cls(
            name=meta.get("name", "unknown"),
            namespace=meta.get("namespace", "default"),
            algo=spec.get("algo", "static"),
            args=dict(spec.get("args", {}) or {}),
            image=spec.get("image"),
        )

    @classmethod
    def from_inline(cls, d: dict[str, Any], namespace: str, name: str = "<inline>") -> Strategy:
        return cls(
            name=name,
            namespace=namespace,
            algo=d.get("algo", "static"),
            args={k: v for k, v in d.items() if k not in ("algo", "image")},
            image=d.get("image"),
        )

    def to_canonical(self) -> dict[str, Any]:
        """Identity for hashing — name/namespace excluded on purpose."""
        return {"algo": self.algo, "args": self.args, "image": self.image}

    @property
    def strategy_hash(self) -> str:
        return content_hash(self.to_canonical())

    @property
    def is_builtin(self) -> bool:
        return self.algo != "image"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_strategy_model.py -v`
Expected: PASS (4 passed)

- [ ] **Step 5: Commit**

```bash
git add src/resource_broker/common/models/strategy.py tests/unit/test_strategy_model.py
git commit -m "feat(models): Strategy domain model with hash + inline support"
```

---

## Task 3: Profile domain model (ref + inline strategy)

**Files:**
- Modify: `src/resource_broker/common/models/profile.py`
- Test: `tests/unit/test_profile_model.py`

**Interfaces:**
- Consumes: `content_hash` (Task 1), `Strategy` (Task 2).
- Produces:
  - `FieldSpec(strategy_ref: str | None, strategy_inline: Strategy | None, locator: str | None, min: str | None, max: str | None)`
  - `Profile(name, namespace, resource_type, mode, default_strategy_ref, default_strategy_inline, restart_strategy, fields: dict[str, FieldSpec])`
  - `Profile.from_crd(crd: dict) -> Profile`
  - `Profile.to_canonical() -> dict`
  - `Profile.profile_hash` (property) → 64-char hex
  - `Profile.is_enforce()` → bool

Note: the legacy `ResourceProfile`/`FieldEntry`/`FieldStrategy` classes stay in this file untouched until their consumers (patcher, legacy webhook) are migrated in Service 3. New code imports `Profile`.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_profile_model.py
from __future__ import annotations

from resource_broker.common.models.profile import Profile


def _crd() -> dict:
    return {
        "metadata": {"name": "k8s-aggressive", "namespace": "default"},
        "spec": {
            "resource-type": "k8s-pod",
            "mode": "enforce",
            "default-strategy": "percentile-p75",
            "fields": {
                "cpu_request": {"strategy": "percentile-p90", "min": "100m", "max": "4"},
                "memory_request": {"strategy": {"algo": "static", "value": "512Mi"}},
            },
        },
    }


def test_from_crd_parses_ref_and_inline() -> None:
    p = Profile.from_crd(_crd())
    assert p.name == "k8s-aggressive"
    assert p.resource_type == "k8s-pod"
    assert p.is_enforce() is True
    assert p.default_strategy_ref == "percentile-p75"
    assert p.fields["cpu_request"].strategy_ref == "percentile-p90"
    assert p.fields["cpu_request"].min == "100m"
    # Inline strategy block becomes a Strategy on the FieldSpec
    assert p.fields["memory_request"].strategy_inline is not None
    assert p.fields["memory_request"].strategy_inline.algo == "static"


def test_profile_hash_is_stable_and_64_hex() -> None:
    p1 = Profile.from_crd(_crd())
    p2 = Profile.from_crd(_crd())
    assert p1.profile_hash == p2.profile_hash
    assert len(p1.profile_hash) == 64


def test_profile_hash_changes_with_field_bounds() -> None:
    base = _crd()
    p1 = Profile.from_crd(base)
    base["spec"]["fields"]["cpu_request"]["max"] = "8"
    p2 = Profile.from_crd(base)
    assert p1.profile_hash != p2.profile_hash
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_profile_model.py -v`
Expected: FAIL with `ImportError: cannot import name 'Profile'`

- [ ] **Step 3: Write minimal implementation**

Append the new models to `src/resource_broker/common/models/profile.py` (do not remove the existing `ResourceProfile`/`FieldEntry`/`FieldStrategy`):

```python
from resource_broker.common.hashing import content_hash
from resource_broker.common.models.strategy import Strategy


@dataclass
class FieldSpec:
    strategy_ref: str | None = None
    strategy_inline: Strategy | None = None
    locator: str | None = None
    min: str | None = None
    max: str | None = None

    @classmethod
    def from_dict(cls, namespace: str, field_name: str, d: dict[str, Any]) -> FieldSpec:
        raw = d.get("strategy")
        ref: str | None = None
        inline: Strategy | None = None
        if isinstance(raw, str):
            ref = raw
        elif isinstance(raw, dict):
            inline = Strategy.from_inline(raw, namespace=namespace, name=f"<{field_name}>")
        return cls(
            strategy_ref=ref,
            strategy_inline=inline,
            locator=d.get("locator") or None,
            min=d.get("min"),
            max=d.get("max"),
        )

    def to_canonical(self) -> dict[str, Any]:
        return {
            "ref": self.strategy_ref,
            "inline": self.strategy_inline.to_canonical() if self.strategy_inline else None,
            "locator": self.locator,
            "min": self.min,
            "max": self.max,
        }


@dataclass
class Profile:
    name: str
    namespace: str
    resource_type: str
    mode: str = "recommendation"
    default_strategy_ref: str | None = None
    default_strategy_inline: Strategy | None = None
    restart_strategy: dict[str, Any] | None = None
    fields: dict[str, FieldSpec] = field(default_factory=dict)

    @classmethod
    def from_crd(cls, crd: dict[str, Any]) -> Profile:
        meta = crd.get("metadata", {}) or {}
        spec = crd.get("spec", {}) or {}
        namespace = meta.get("namespace", "default")

        raw_default = spec.get("default-strategy")
        default_ref: str | None = None
        default_inline: Strategy | None = None
        if isinstance(raw_default, str):
            default_ref = raw_default
        elif isinstance(raw_default, dict):
            default_inline = Strategy.from_inline(raw_default, namespace=namespace, name="<default>")

        fields = {
            fname: FieldSpec.from_dict(namespace, fname, fdict or {})
            for fname, fdict in (spec.get("fields", {}) or {}).items()
        }
        return cls(
            name=meta.get("name", "unknown"),
            namespace=namespace,
            resource_type=spec.get("resource-type", ""),
            mode=spec.get("mode", "recommendation"),
            default_strategy_ref=default_ref,
            default_strategy_inline=default_inline,
            restart_strategy=spec.get("restart-strategy"),
            fields=fields,
        )

    def to_canonical(self) -> dict[str, Any]:
        return {
            "resource_type": self.resource_type,
            "mode": self.mode,
            "default_ref": self.default_strategy_ref,
            "default_inline": self.default_strategy_inline.to_canonical() if self.default_strategy_inline else None,
            "restart_strategy": self.restart_strategy,
            "fields": {k: v.to_canonical() for k, v in self.fields.items()},
        }

    @property
    def profile_hash(self) -> str:
        return content_hash(self.to_canonical())

    def is_enforce(self) -> bool:
        return self.mode == "enforce"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_profile_model.py -v`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
git add src/resource_broker/common/models/profile.py tests/unit/test_profile_model.py
git commit -m "feat(models): Profile model with strategy ref/inline + profile_hash"
```

---

## Task 4: ResourceType versioning + hash

**Files:**
- Modify: `src/resource_broker/resource_types/base.py`
- Modify: `src/resource_broker/resource_types/k8s_resources.py`
- Test: `tests/unit/test_resource_type_hash.py`

**Interfaces:**
- Consumes: `content_hash` (Task 1).
- Produces: `ResourceType.version: str`, `ResourceType.resource_type_hash` (property) over `{version, fields}`. `k8s-pod` gets `version = "1"`.

Locators stay **pod-template-relative** (e.g. `/spec/containers/0/...`); the Service 3 Deployment webhook prepends `/spec/template`. No locator strings change in this task.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_resource_type_hash.py
from __future__ import annotations

from resource_broker.resource_types.registry import resource_type_registry


def test_k8s_pod_has_version_and_hash() -> None:
    rt = resource_type_registry.get("k8s-pod")
    assert rt.version == "1"
    h = rt.resource_type_hash
    assert len(h) == 64
    # Stable across instances
    assert resource_type_registry.get("k8s-pod").resource_type_hash == h
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_resource_type_hash.py -v`
Expected: FAIL with `AttributeError: 'K8sResources' object has no attribute 'version'`

- [ ] **Step 3: Write minimal implementation**

In `src/resource_broker/resource_types/base.py`, add to the `ResourceType` class body (right under `description: str = ""`):

```python
    version: str = "0"

    @property
    def resource_type_hash(self) -> str:
        from resource_broker.common.hashing import content_hash

        fields_canonical = {
            name: {"path": fd.path, "default_algorithm": fd.default_algorithm, "patch_type": fd.patch_type}
            for name, fd in self.fields.items()
        }
        return content_hash({"version": self.version, "fields": fields_canonical})
```

In `src/resource_broker/resource_types/k8s_resources.py`, add `version = "1"` under `description`:

```python
class K8sResources(ResourceType):
    name = "k8s-pod"
    description = "Standard Kubernetes container resource fields (CPU, memory, ephemeral-storage)"
    version = "1"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_resource_type_hash.py -v`
Expected: PASS (1 passed)

- [ ] **Step 5: Commit**

```bash
git add src/resource_broker/resource_types/ tests/unit/test_resource_type_hash.py
git commit -m "feat(resource-types): add version + resource_type_hash"
```

---

## Task 5: Snapshot tables — migration + ORM + repository

**Files:**
- Modify: `src/resource_broker/common/dao/orm_models.py`
- Create: `alembic/versions/0004_crd_cache_snapshots.py`
- Create: `src/resource_broker/common/dao/repositories/snapshots.py`
- Test: `tests/unit/test_snapshot_repository.py`

**Interfaces:**
- Produces:
  - ORM `ProfileSnapshotModel`, `StrategySnapshotModel` (PK `(namespace, name)`; cols `hash str`, `spec JSONB`, `updated_at`).
  - `SnapshotRepository(session)` with: `upsert_profile(namespace, name, profile_hash, spec) -> None`, `upsert_strategy(...) -> None`, `all_profiles() -> list[dict]`, `all_strategies() -> list[dict]` (each dict = `{"namespace","name","hash","spec"}`).

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_snapshot_repository.py
from __future__ import annotations

import pytest

from resource_broker.common.dao.repositories.snapshots import SnapshotRepository


@pytest.mark.asyncio
async def test_upsert_then_list_profiles(db_session) -> None:
    repo = SnapshotRepository(db_session)
    await repo.upsert_profile("default", "p1", "h1", {"resource_type": "k8s-pod"})
    await db_session.commit()

    rows = await repo.all_profiles()
    assert len(rows) == 1
    assert rows[0]["namespace"] == "default"
    assert rows[0]["name"] == "p1"
    assert rows[0]["hash"] == "h1"
    assert rows[0]["spec"] == {"resource_type": "k8s-pod"}


@pytest.mark.asyncio
async def test_upsert_is_idempotent_and_updates(db_session) -> None:
    repo = SnapshotRepository(db_session)
    await repo.upsert_profile("default", "p1", "h1", {"v": 1})
    await repo.upsert_profile("default", "p1", "h2", {"v": 2})
    await db_session.commit()

    rows = await repo.all_profiles()
    assert len(rows) == 1
    assert rows[0]["hash"] == "h2"
    assert rows[0]["spec"] == {"v": 2}


@pytest.mark.asyncio
async def test_strategy_snapshot_roundtrip(db_session) -> None:
    repo = SnapshotRepository(db_session)
    await repo.upsert_strategy("default", "s1", "sh1", {"algo": "percentile"})
    await db_session.commit()
    rows = await repo.all_strategies()
    assert rows[0]["name"] == "s1"
    assert rows[0]["spec"]["algo"] == "percentile"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_snapshot_repository.py -v`
Expected: FAIL with `ModuleNotFoundError: ... repositories.snapshots`

- [ ] **Step 3a: Add ORM models**

In `src/resource_broker/common/dao/orm_models.py`, **delete** `ProfileVersionModel`, `ProfileFieldStrategyModel`, `ProfileRecommendationModel`, and add:

```python
class ProfileSnapshotModel(Base):
    """Latest-known canonical spec of a Profile CRD — cold-start cache fallback."""

    __tablename__ = "profile_snapshot"

    namespace: Mapped[str] = mapped_column(String(253), primary_key=True)
    name: Mapped[str] = mapped_column(String(253), primary_key=True)
    profile_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    spec: Mapped[dict] = mapped_column(JSONB, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class StrategySnapshotModel(Base):
    """Latest-known canonical spec of a Strategy CRD — cold-start cache fallback."""

    __tablename__ = "strategy_snapshot"

    namespace: Mapped[str] = mapped_column(String(253), primary_key=True)
    name: Mapped[str] = mapped_column(String(253), primary_key=True)
    strategy_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    spec: Mapped[dict] = mapped_column(JSONB, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )
```

Remove now-unused imports (`Boolean`, `ForeignKey`, `Integer`, `text`, `relationship`) if ruff flags them.

- [ ] **Step 3b: Write the migration**

```python
# alembic/versions/0004_crd_cache_snapshots.py
"""CRD cache snapshots: drop SCD profile tables, add profile/strategy snapshots.

Revision ID: 0004
Revises: 0003
Create Date: 2026-06-20
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0004"
down_revision: Union[str, None] = "0003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Drop SCD profile tables — CRDs are the source of truth; snapshots replace history.
    op.drop_table("profile_recommendations")
    op.drop_table("resource_profile_field_strategies")
    op.drop_table("resource_profile_versions")

    for table, hash_col in (("profile_snapshot", "profile_hash"), ("strategy_snapshot", "strategy_hash")):
        op.create_table(
            table,
            sa.Column("namespace", sa.String(253), nullable=False),
            sa.Column("name", sa.String(253), nullable=False),
            sa.Column(hash_col, sa.String(64), nullable=False),
            sa.Column("spec", postgresql.JSONB(), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
            sa.PrimaryKeyConstraint("namespace", "name"),
        )


def downgrade() -> None:
    op.drop_table("strategy_snapshot")
    op.drop_table("profile_snapshot")
    # SCD tables are not recreated on downgrade (one-way redesign).
```

- [ ] **Step 3c: Write the repository**

```python
# src/resource_broker/common/dao/repositories/snapshots.py
from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from resource_broker.common.dao.orm_models import ProfileSnapshotModel, StrategySnapshotModel


def _upsert_stmt(session: AsyncSession, model: type, values: dict[str, Any], update_cols: list[str]):
    dialect = session.bind.dialect.name if session.bind else "postgresql"
    insert = sqlite_insert if dialect == "sqlite" else pg_insert
    stmt = insert(model).values(**values)
    return stmt.on_conflict_do_update(
        index_elements=["namespace", "name"],
        set_={c: getattr(stmt.excluded, c) for c in update_cols},
    )


class SnapshotRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def upsert_profile(self, namespace: str, name: str, profile_hash: str, spec: dict[str, Any]) -> None:
        stmt = _upsert_stmt(
            self._session,
            ProfileSnapshotModel,
            {"namespace": namespace, "name": name, "profile_hash": profile_hash, "spec": spec},
            ["profile_hash", "spec"],
        )
        await self._session.execute(stmt)

    async def upsert_strategy(self, namespace: str, name: str, strategy_hash: str, spec: dict[str, Any]) -> None:
        stmt = _upsert_stmt(
            self._session,
            StrategySnapshotModel,
            {"namespace": namespace, "name": name, "strategy_hash": strategy_hash, "spec": spec},
            ["strategy_hash", "spec"],
        )
        await self._session.execute(stmt)

    async def all_profiles(self) -> list[dict[str, Any]]:
        rows = (await self._session.execute(select(ProfileSnapshotModel))).scalars().all()
        return [{"namespace": r.namespace, "name": r.name, "hash": r.profile_hash, "spec": r.spec} for r in rows]

    async def all_strategies(self) -> list[dict[str, Any]]:
        rows = (await self._session.execute(select(StrategySnapshotModel))).scalars().all()
        return [{"namespace": r.namespace, "name": r.name, "hash": r.strategy_hash, "spec": r.spec} for r in rows]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_snapshot_repository.py -v`
Expected: PASS (3 passed)

Also delete the now-broken legacy test file and repo (they reference dropped models):

```bash
git rm src/resource_broker/common/dao/repositories/profiles.py tests/unit/test_profiles.py
```

Run: `uv run pytest tests/unit/ -q` — expected: green (no import errors from removed SCD code). If `crd_watcher.py` / `profile_registry.py` still import `ProfileRepository`, that is resolved in Tasks 6 and 8; for now confirm `tests/unit/` collects. If collection fails on those imports, proceed to Task 6 before re-running.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(dao): migration 0004 — snapshot tables replace SCD profile schema"
```

---

## Task 6: Strategy + Profile registries (cache with snapshot fallback)

**Files:**
- Create: `src/resource_broker/common/services/strategy_registry.py`
- Rewrite: `src/resource_broker/common/services/profile_registry.py`
- Test: `tests/unit/test_registries.py`

**Interfaces:**
- Consumes: `Strategy` (T2), `Profile` (T3), `SnapshotRepository` (T5).
- Produces (both registries share this shape):
  - `get(name, namespace) -> Strategy|Profile|None`
  - `upsert_from_crd(crd: dict) -> Strategy|Profile|None`
  - `remove(name, namespace) -> None`
  - `all() -> list[...]`
  - `load_from_items(items: list[dict]) -> None` (bootstrap from a k8s `list`)
  - `load_from_snapshot(rows: list[dict]) -> None` (cold-start fallback; rows from `SnapshotRepository`)

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_registries.py
from __future__ import annotations

from resource_broker.common.services.profile_registry import ProfileRegistry
from resource_broker.common.services.strategy_registry import StrategyRegistry


def _strategy_crd(name: str) -> dict:
    return {"metadata": {"name": name, "namespace": "default"}, "spec": {"algo": "percentile", "args": {"percentile": 75}}}


def _profile_crd(name: str) -> dict:
    return {
        "metadata": {"name": name, "namespace": "default"},
        "spec": {"resource-type": "k8s-pod", "default-strategy": "percentile-p75", "fields": {"cpu_request": {}}},
    }


def test_strategy_registry_upsert_get_remove() -> None:
    reg = StrategyRegistry()
    reg.upsert_from_crd(_strategy_crd("percentile-p75"))
    s = reg.get("percentile-p75", "default")
    assert s is not None and s.algo == "percentile"
    reg.remove("percentile-p75", "default")
    assert reg.get("percentile-p75", "default") is None


def test_profile_registry_bootstrap_from_items() -> None:
    reg = ProfileRegistry()
    reg.load_from_items([_profile_crd("a"), _profile_crd("b")])
    assert {p.name for p in reg.all()} == {"a", "b"}


def test_profile_registry_load_from_snapshot() -> None:
    reg = ProfileRegistry()
    # snapshot rows store the canonical spec under "spec" plus identity
    reg.load_from_snapshot(
        [{"namespace": "default", "name": "a", "spec": {"resource-type": "k8s-pod", "fields": {"cpu_request": {}}}}]
    )
    p = reg.get("a", "default")
    assert p is not None and p.resource_type == "k8s-pod"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_registries.py -v`
Expected: FAIL with import errors for the registry modules.

- [ ] **Step 3a: Strategy registry**

```python
# src/resource_broker/common/services/strategy_registry.py
from __future__ import annotations

from typing import Any

from structlog import get_logger

from resource_broker.common.models.strategy import Strategy

logger = get_logger(__name__)

STRATEGY_GROUP = "resource-broker.io"
STRATEGY_VERSION = "v1alpha1"
STRATEGY_PLURAL = "strategies"


class StrategyRegistry:
    """Per-process in-memory cache of Strategy CRDs. Not distributed."""

    def __init__(self) -> None:
        self._items: dict[str, Strategy] = {}

    @staticmethod
    def _key(name: str, namespace: str) -> str:
        return f"{namespace}/{name}"

    def get(self, name: str, namespace: str = "default") -> Strategy | None:
        return self._items.get(self._key(name, namespace))

    def all(self) -> list[Strategy]:
        return list(self._items.values())

    def upsert_from_crd(self, crd: dict[str, Any]) -> Strategy | None:
        try:
            s = Strategy.from_crd(crd)
        except Exception:
            logger.exception("failed to parse strategy crd")
            return None
        self._items[self._key(s.name, s.namespace)] = s
        return s

    def remove(self, name: str, namespace: str) -> None:
        self._items.pop(self._key(name, namespace), None)

    def load_from_items(self, items: list[dict[str, Any]]) -> None:
        for item in items:
            self.upsert_from_crd(item)
        logger.info("strategy registry loaded from kubernetes", count=len(self._items))

    def load_from_snapshot(self, rows: list[dict[str, Any]]) -> None:
        for row in rows:
            crd = {"metadata": {"name": row["name"], "namespace": row["namespace"]}, "spec": row["spec"]}
            self.upsert_from_crd(crd)
        logger.info("strategy registry loaded from snapshot fallback", count=len(self._items))
```

- [ ] **Step 3b: Profile registry (rewrite the file completely)**

```python
# src/resource_broker/common/services/profile_registry.py
from __future__ import annotations

from typing import Any

from structlog import get_logger

from resource_broker.common.models.profile import Profile

logger = get_logger(__name__)

PROFILE_GROUP = "resource-broker.io"
PROFILE_VERSION = "v1alpha1"
PROFILE_PLURAL = "profiles"


class ProfileRegistry:
    """Per-process in-memory cache of Profile CRDs. Not distributed."""

    def __init__(self) -> None:
        self._items: dict[str, Profile] = {}

    @staticmethod
    def _key(name: str, namespace: str) -> str:
        return f"{namespace}/{name}"

    def get(self, name: str, namespace: str = "default") -> Profile | None:
        return self._items.get(self._key(name, namespace))

    def all(self) -> list[Profile]:
        return list(self._items.values())

    def upsert_from_crd(self, crd: dict[str, Any]) -> Profile | None:
        try:
            p = Profile.from_crd(crd)
        except Exception:
            logger.exception("failed to parse profile crd")
            return None
        self._items[self._key(p.name, p.namespace)] = p
        return p

    def remove(self, name: str, namespace: str) -> None:
        self._items.pop(self._key(name, namespace), None)

    def load_from_items(self, items: list[dict[str, Any]]) -> None:
        for item in items:
            self.upsert_from_crd(item)
        logger.info("profile registry loaded from kubernetes", count=len(self._items))

    def load_from_snapshot(self, rows: list[dict[str, Any]]) -> None:
        for row in rows:
            crd = {"metadata": {"name": row["name"], "namespace": row["namespace"]}, "spec": row["spec"]}
            self.upsert_from_crd(crd)
        logger.info("profile registry loaded from snapshot fallback", count=len(self._items))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_registries.py -v`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
git add src/resource_broker/common/services/strategy_registry.py src/resource_broker/common/services/profile_registry.py tests/unit/test_registries.py
git commit -m "feat(cache): Strategy + Profile in-memory registries with snapshot load"
```

---

## Task 7: Strategy resolution (inline > ref > default)

**Files:**
- Create: `src/resource_broker/common/services/strategy_resolver.py`
- Test: `tests/unit/test_strategy_resolver.py`

**Interfaces:**
- Consumes: `Profile`/`FieldSpec` (T3), `Strategy` (T2), `StrategyRegistry` (T6).
- Produces: `resolve_field_strategy(profile: Profile, field_name: str, strategies: StrategyRegistry) -> Strategy | None`. Order: `fields.<f>.strategy_inline` → `fields.<f>.strategy_ref` → `profile.default_strategy_inline` → `profile.default_strategy_ref` → `None`. Refs resolve against the registry **in the profile's namespace**; an unresolved ref returns `None` and logs a warning.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_strategy_resolver.py
from __future__ import annotations

from resource_broker.common.models.profile import Profile
from resource_broker.common.services.strategy_registry import StrategyRegistry
from resource_broker.common.services.strategy_resolver import resolve_field_strategy


def _registry() -> StrategyRegistry:
    reg = StrategyRegistry()
    reg.upsert_from_crd(
        {"metadata": {"name": "p75", "namespace": "default"}, "spec": {"algo": "percentile", "args": {"percentile": 75}}}
    )
    reg.upsert_from_crd(
        {"metadata": {"name": "p90", "namespace": "default"}, "spec": {"algo": "percentile", "args": {"percentile": 90}}}
    )
    return reg


def test_field_inline_wins() -> None:
    p = Profile.from_crd(
        {
            "metadata": {"name": "x", "namespace": "default"},
            "spec": {"resource-type": "k8s-pod", "default-strategy": "p75",
                     "fields": {"cpu_request": {"strategy": {"algo": "static", "value": "1"}}}},
        }
    )
    s = resolve_field_strategy(p, "cpu_request", _registry())
    assert s is not None and s.algo == "static"


def test_field_ref_then_default_ref() -> None:
    p = Profile.from_crd(
        {
            "metadata": {"name": "x", "namespace": "default"},
            "spec": {"resource-type": "k8s-pod", "default-strategy": "p75",
                     "fields": {"cpu_request": {"strategy": "p90"}, "memory_request": {}}},
        }
    )
    reg = _registry()
    assert resolve_field_strategy(p, "cpu_request", reg).args["percentile"] == 90
    assert resolve_field_strategy(p, "memory_request", reg).args["percentile"] == 75


def test_unresolved_ref_returns_none() -> None:
    p = Profile.from_crd(
        {"metadata": {"name": "x", "namespace": "default"},
         "spec": {"resource-type": "k8s-pod", "fields": {"cpu_request": {"strategy": "missing"}}}}
    )
    assert resolve_field_strategy(p, "cpu_request", _registry()) is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_strategy_resolver.py -v`
Expected: FAIL with import error for `strategy_resolver`.

- [ ] **Step 3: Write minimal implementation**

```python
# src/resource_broker/common/services/strategy_resolver.py
from __future__ import annotations

from structlog import get_logger

from resource_broker.common.models.profile import Profile
from resource_broker.common.models.strategy import Strategy
from resource_broker.common.services.strategy_registry import StrategyRegistry

logger = get_logger(__name__)


def resolve_field_strategy(
    profile: Profile,
    field_name: str,
    strategies: StrategyRegistry,
) -> Strategy | None:
    """Resolve the effective Strategy for a field: inline > ref > profile default."""
    fs = profile.fields.get(field_name)

    if fs is not None and fs.strategy_inline is not None:
        return fs.strategy_inline
    if fs is not None and fs.strategy_ref is not None:
        return _resolve_ref(fs.strategy_ref, profile, strategies)

    if profile.default_strategy_inline is not None:
        return profile.default_strategy_inline
    if profile.default_strategy_ref is not None:
        return _resolve_ref(profile.default_strategy_ref, profile, strategies)

    return None


def _resolve_ref(ref: str, profile: Profile, strategies: StrategyRegistry) -> Strategy | None:
    s = strategies.get(ref, profile.namespace)
    if s is None:
        logger.warning("strategy ref not found", ref=ref, namespace=profile.namespace, profile=profile.name)
    return s
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_strategy_resolver.py -v`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
git add src/resource_broker/common/services/strategy_resolver.py tests/unit/test_strategy_resolver.py
git commit -m "feat(cache): field strategy resolution (inline > ref > default)"
```

---

## Task 8: CrdCache orchestrator + dual watch + serve wiring

**Files:**
- Create: `src/resource_broker/common/services/crd_cache.py`
- Rewrite: `src/resource_broker/watcher/controllers/crd_watcher.py`
- Modify: `src/resource_broker/config.py`
- Modify: `src/resource_broker/api/app.py`
- Test: `tests/unit/test_crd_cache.py`

**Interfaces:**
- Consumes: registries (T6), `SnapshotRepository` (T5), `content_hash` (T1).
- Produces:
  - `CrdCache(profiles: ProfileRegistry, strategies: StrategyRegistry)`
  - `async CrdCache.bootstrap(co_api) -> None` — list both kinds from k8s; on success write-through snapshots; on k8s failure load both registries from snapshots.
  - `async CrdCache.snapshot_profile(profile) -> None`, `async CrdCache.snapshot_strategy(strategy) -> None`
  - `async run_crd_watch_loops(co_api, cache: CrdCache, resync_seconds: int) -> None` — runs Profile watch, Strategy watch, and a resync timer concurrently.
  - `settings.cache_resync_seconds` (default 300).

- [ ] **Step 1: Write the failing test** (bootstrap fallback is the testable core; watch loops are exercised in the integration plan)

```python
# tests/unit/test_crd_cache.py
from __future__ import annotations

import pytest

from resource_broker.common.dao.repositories.snapshots import SnapshotRepository
from resource_broker.common.services.crd_cache import CrdCache
from resource_broker.common.services.profile_registry import ProfileRegistry
from resource_broker.common.services.strategy_registry import StrategyRegistry


class _FakeCoApi:
    """Stands in for kubernetes.CustomObjectsApi.list_cluster_custom_object."""

    def __init__(self, by_plural: dict[str, list[dict]], fail: bool = False) -> None:
        self._by_plural = by_plural
        self._fail = fail

    def list_cluster_custom_object(self, *, group: str, version: str, plural: str):  # noqa: ARG002
        if self._fail:
            raise RuntimeError("k8s api down")
        return {"items": self._by_plural.get(plural, [])}


@pytest.mark.asyncio
async def test_bootstrap_from_k8s_populates_and_snapshots(db_session, monkeypatch) -> None:
    # Route the cache's DB session to the in-memory test session.
    import resource_broker.common.services.crd_cache as mod

    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def _fake_session():
        yield db_session

    monkeypatch.setattr(mod, "get_session", _fake_session)

    api = _FakeCoApi(
        {
            "profiles": [{"metadata": {"name": "p", "namespace": "default"}, "spec": {"resource-type": "k8s-pod"}}],
            "strategies": [{"metadata": {"name": "s", "namespace": "default"}, "spec": {"algo": "percentile"}}],
        }
    )
    cache = CrdCache(ProfileRegistry(), StrategyRegistry())
    await cache.bootstrap(api)

    assert cache.profiles.get("p", "default") is not None
    assert cache.strategies.get("s", "default") is not None
    rows = await SnapshotRepository(db_session).all_profiles()
    assert rows[0]["name"] == "p"


@pytest.mark.asyncio
async def test_bootstrap_falls_back_to_snapshot_on_k8s_failure(db_session, monkeypatch) -> None:
    import resource_broker.common.services.crd_cache as mod
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def _fake_session():
        yield db_session

    monkeypatch.setattr(mod, "get_session", _fake_session)

    repo = SnapshotRepository(db_session)
    await repo.upsert_profile("default", "p", "h", {"resource-type": "k8s-pod"})
    await repo.upsert_strategy("default", "s", "h", {"algo": "static"})
    await db_session.commit()

    cache = CrdCache(ProfileRegistry(), StrategyRegistry())
    await cache.bootstrap(_FakeCoApi({}, fail=True))

    assert cache.profiles.get("p", "default") is not None
    assert cache.strategies.get("s", "default") is not None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_crd_cache.py -v`
Expected: FAIL with import error for `crd_cache`.

- [ ] **Step 3a: Add config var**

In `src/resource_broker/config.py`, under the `# ── Profiles ──` block add:

```python
    # ── CRD cache ────────────────────────────────────────────────────────
    cache_resync_seconds: int = 300
```

- [ ] **Step 3b: Write `crd_cache.py`**

```python
# src/resource_broker/common/services/crd_cache.py
from __future__ import annotations

from structlog import get_logger

from resource_broker.common.dao.database import get_session
from resource_broker.common.dao.repositories.snapshots import SnapshotRepository
from resource_broker.common.models.profile import Profile
from resource_broker.common.models.strategy import Strategy
from resource_broker.common.services.profile_registry import (
    PROFILE_GROUP,
    PROFILE_PLURAL,
    PROFILE_VERSION,
    ProfileRegistry,
)
from resource_broker.common.services.strategy_registry import STRATEGY_PLURAL, StrategyRegistry

logger = get_logger(__name__)


class CrdCache:
    def __init__(self, profiles: ProfileRegistry, strategies: StrategyRegistry) -> None:
        self.profiles = profiles
        self.strategies = strategies

    async def bootstrap(self, co_api) -> None:  # noqa: ANN001 (kubernetes CustomObjectsApi)
        try:
            prof_items = co_api.list_cluster_custom_object(
                group=PROFILE_GROUP, version=PROFILE_VERSION, plural=PROFILE_PLURAL
            ).get("items", [])
            strat_items = co_api.list_cluster_custom_object(
                group=PROFILE_GROUP, version=PROFILE_VERSION, plural=STRATEGY_PLURAL
            ).get("items", [])
        except Exception:
            logger.exception("crd bootstrap from kubernetes failed; loading from snapshot")
            await self._load_from_snapshots()
            return

        self.strategies.load_from_items(strat_items)
        self.profiles.load_from_items(prof_items)
        await self._write_through_all()

    async def _load_from_snapshots(self) -> None:
        try:
            async with get_session() as session:
                repo = SnapshotRepository(session)
                self.strategies.load_from_snapshot(await repo.all_strategies())
                self.profiles.load_from_snapshot(await repo.all_profiles())
        except Exception:
            logger.exception("snapshot fallback failed; caches start empty")

    async def _write_through_all(self) -> None:
        try:
            async with get_session() as session:
                repo = SnapshotRepository(session)
                for s in self.strategies.all():
                    await repo.upsert_strategy(s.namespace, s.name, s.strategy_hash, s.to_canonical())
                for p in self.profiles.all():
                    await repo.upsert_profile(p.namespace, p.name, p.profile_hash, _profile_spec(p))
        except Exception:
            logger.exception("write-through snapshot failed")

    async def snapshot_strategy(self, strategy: Strategy) -> None:
        try:
            async with get_session() as session:
                await SnapshotRepository(session).upsert_strategy(
                    strategy.namespace, strategy.name, strategy.strategy_hash, strategy.to_canonical()
                )
        except Exception:
            logger.exception("snapshot_strategy failed", name=strategy.name)

    async def snapshot_profile(self, profile: Profile) -> None:
        try:
            async with get_session() as session:
                await SnapshotRepository(session).upsert_profile(
                    profile.namespace, profile.name, profile.profile_hash, _profile_spec(profile)
                )
        except Exception:
            logger.exception("snapshot_profile failed", name=profile.name)


def _profile_spec(p: Profile) -> dict:
    """Round-trippable spec for the snapshot (rebuildable by Profile.from_crd)."""
    spec: dict = {"resource-type": p.resource_type, "mode": p.mode}
    if p.default_strategy_ref is not None:
        spec["default-strategy"] = p.default_strategy_ref
    elif p.default_strategy_inline is not None:
        s = p.default_strategy_inline
        spec["default-strategy"] = {"algo": s.algo, **s.args, **({"image": s.image} if s.image else {})}
    if p.restart_strategy is not None:
        spec["restart-strategy"] = p.restart_strategy
    fields: dict = {}
    for name, fs in p.fields.items():
        entry: dict = {}
        if fs.strategy_ref is not None:
            entry["strategy"] = fs.strategy_ref
        elif fs.strategy_inline is not None:
            s = fs.strategy_inline
            entry["strategy"] = {"algo": s.algo, **s.args, **({"image": s.image} if s.image else {})}
        if fs.locator:
            entry["locator"] = fs.locator
        if fs.min is not None:
            entry["min"] = fs.min
        if fs.max is not None:
            entry["max"] = fs.max
        fields[name] = entry
    spec["fields"] = fields
    return spec
```

- [ ] **Step 3c: Rewrite `crd_watcher.py` (dual watch + resync)**

```python
# src/resource_broker/watcher/controllers/crd_watcher.py
from __future__ import annotations

import asyncio
from typing import Any

from kubernetes import client as k8s_client, watch as k8s_watch
from structlog import get_logger

from resource_broker.common.services.crd_cache import CrdCache
from resource_broker.common.services.profile_registry import (
    PROFILE_GROUP,
    PROFILE_PLURAL,
    PROFILE_VERSION,
)
from resource_broker.common.services.strategy_registry import STRATEGY_PLURAL

logger = get_logger(__name__)


async def run_crd_watch_loops(
    co_api: k8s_client.CustomObjectsApi,
    cache: CrdCache,
    resync_seconds: int,
) -> None:
    """Run Profile watch, Strategy watch, and a periodic resync concurrently."""
    await asyncio.gather(
        _watch_kind_forever(co_api, cache, PROFILE_PLURAL),
        _watch_kind_forever(co_api, cache, STRATEGY_PLURAL),
        _resync_forever(co_api, cache, resync_seconds),
    )


async def _watch_kind_forever(co_api: k8s_client.CustomObjectsApi, cache: CrdCache, plural: str) -> None:
    loop = asyncio.get_running_loop()
    while True:
        try:
            await loop.run_in_executor(None, _watch_once, co_api, cache, plural, loop)
        except Exception:
            logger.exception("crd watch loop crashed, restarting in 5s", plural=plural)
            await asyncio.sleep(5)


def _watch_once(
    co_api: k8s_client.CustomObjectsApi, cache: CrdCache, plural: str, loop: asyncio.AbstractEventLoop
) -> None:
    w = k8s_watch.Watch()
    logger.info("crd watch started", plural=plural)
    for event in w.stream(
        co_api.list_cluster_custom_object, group=PROFILE_GROUP, version=PROFILE_VERSION, plural=plural
    ):
        _handle_event(event, cache, plural, loop)


def _handle_event(
    event: dict[str, Any], cache: CrdCache, plural: str, loop: asyncio.AbstractEventLoop
) -> None:
    etype = event.get("type", "")
    obj = event.get("raw_object", {}) or {}
    meta = obj.get("metadata", {}) or {}
    name = meta.get("name", "unknown")
    namespace = meta.get("namespace", "default")
    try:
        if plural == STRATEGY_PLURAL:
            if etype in ("ADDED", "MODIFIED"):
                s = cache.strategies.upsert_from_crd(obj)
                if s is not None:
                    asyncio.run_coroutine_threadsafe(cache.snapshot_strategy(s), loop)
            elif etype == "DELETED":
                cache.strategies.remove(name, namespace)
        else:  # profiles
            if etype in ("ADDED", "MODIFIED"):
                p = cache.profiles.upsert_from_crd(obj)
                if p is not None:
                    asyncio.run_coroutine_threadsafe(cache.snapshot_profile(p), loop)
            elif etype == "DELETED":
                cache.profiles.remove(name, namespace)
        logger.debug("crd event handled", plural=plural, type=etype, name=name, namespace=namespace)
    except Exception:
        logger.exception("error handling crd event", plural=plural, type=etype, name=name)


async def _resync_forever(co_api: k8s_client.CustomObjectsApi, cache: CrdCache, resync_seconds: int) -> None:
    while True:
        await asyncio.sleep(resync_seconds)
        try:
            await cache.bootstrap(co_api)
            logger.info("crd cache resynced")
        except Exception:
            logger.exception("crd resync failed")
```

- [ ] **Step 3d: Wire into `app.py`**

Replace the lifespan body that built `ProfileRegistry` + `RecommendationService` with the cache. New `lifespan`:

```python
# src/resource_broker/api/app.py — replace the imports + lifespan
from resource_broker.common.services.crd_cache import CrdCache
from resource_broker.common.services.profile_registry import ProfileRegistry
from resource_broker.common.services.strategy_registry import StrategyRegistry
from resource_broker.watcher.controllers.crd_watcher import run_crd_watch_loops


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    configure_logging()
    if not await check_connection():
        logger.warning("starting without database connectivity")
    else:
        logger.info("database connected")

    cache = CrdCache(ProfileRegistry(), StrategyRegistry())
    watch_task = None
    try:
        api = create_k8s_api(k8s_client.CustomObjectsApi)
        await cache.bootstrap(api)
        watch_task = asyncio.create_task(run_crd_watch_loops(api, cache, settings.cache_resync_seconds))
    except Exception:
        logger.exception("failed to connect to kubernetes; crd cache disabled")

    _app.state.crd_cache = cache

    yield

    if watch_task is not None:
        watch_task.cancel()
        try:
            await watch_task
        except asyncio.CancelledError:
            pass
```

Remove the now-unused `RecommendationService` import. The webhook controller still reads `app.state.recommendation_svc`; that path is replaced in **Service 3** (this plan leaves the legacy `/mutate` returning allow-only via `app.state.crd_cache` being present but unused — acceptable because Service 3 rewrites the webhook). To keep `serve` importable now, in `api/controllers/webhook.py` change the handler call to a temporary allow-all:

```python
# src/resource_broker/api/controllers/webhook.py — temporary until Service 3
@router.post("/mutate")
async def mutate(request: Request) -> AdmissionReviewResponse:
    body = await request.json()
    uid = body.get("request", {}).get("uid", "")
    return AdmissionReviewResponse(response={"uid": uid, "allowed": True})
```

Delete the dead handler import. (Service 3 reintroduces real lookup logic.)

- [ ] **Step 4: Run tests + import check**

Run: `uv run pytest tests/unit/test_crd_cache.py -v`
Expected: PASS (2 passed)

Run: `uv run python -c "import resource_broker.api.app"`
Expected: no ImportError.

Run: `uv run pytest tests/unit/ -q`
Expected: all green. (If `recommendation_service.py` / `handler.py` still import removed symbols and break collection, they are not imported by `tests/unit` after the webhook edit — leave them for the Service 3 cleanup task. If collection breaks, add `# isolated for Service 3` and stop importing them anywhere reachable from tests.)

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(cache): CrdCache orchestrator, dual watch + resync, serve wiring"
```

---

## Task 9: CRDs, samples, RBAC, env example

**Files:**
- Create: `deploy/crd/strategy-crd.yaml`, `deploy/crd/profile-crd.yaml`
- Remove: `deploy/crd/resourceprofile-crd.yaml`
- Create: `deploy/samples/strategy-percentile-p75.yaml`, `deploy/samples/profile-k8s-aggressive.yaml`
- Modify: `deploy/resource-broker/rbac.yaml`, `.env.example`
- Test: `tests/unit/test_crd_samples_parse.py`

**Interfaces:** none in code; this task ships cluster manifests + a parse test that the sample CRDs round-trip through the domain models.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_crd_samples_parse.py
from __future__ import annotations

from pathlib import Path

import yaml

from resource_broker.common.models.profile import Profile
from resource_broker.common.models.strategy import Strategy

SAMPLES = Path("deploy/samples")


def test_strategy_sample_parses() -> None:
    doc = yaml.safe_load((SAMPLES / "strategy-percentile-p75.yaml").read_text())
    s = Strategy.from_crd(doc)
    assert s.algo == "percentile"
    assert s.args.get("percentile") == 75


def test_profile_sample_parses_and_refs_strategy() -> None:
    doc = yaml.safe_load((SAMPLES / "profile-k8s-aggressive.yaml").read_text())
    p = Profile.from_crd(doc)
    assert p.resource_type == "k8s-pod"
    assert p.default_strategy_ref == "percentile-p75"
    assert "cpu_request" in p.fields
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_crd_samples_parse.py -v`
Expected: FAIL — sample files do not exist (`FileNotFoundError`).

(If `pyyaml` is not yet a dev dependency, add it: `uv add --dev pyyaml` then `uv sync`.)

- [ ] **Step 3a: Strategy CRD**

```yaml
# deploy/crd/strategy-crd.yaml
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: strategies.resource-broker.io
  labels: { app.kubernetes.io/name: resource-broker }
spec:
  group: resource-broker.io
  names: { kind: Strategy, listKind: StrategyList, plural: strategies, singular: strategy, shortNames: [strat] }
  scope: Namespaced
  versions:
    - name: v1alpha1
      served: true
      storage: true
      schema:
        openAPIV3Schema:
          type: object
          required: [spec]
          properties:
            spec:
              type: object
              required: [algo]
              x-kubernetes-preserve-unknown-fields: true
              properties:
                algo: { type: string, description: "percentile | static | derived | image" }
                args: { type: object, x-kubernetes-preserve-unknown-fields: true }
                image: { type: string, nullable: true, description: "docker image (only when algo: image; P2 runner)" }
```

- [ ] **Step 3b: Profile CRD**

```yaml
# deploy/crd/profile-crd.yaml
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: profiles.resource-broker.io
  labels: { app.kubernetes.io/name: resource-broker }
spec:
  group: resource-broker.io
  names: { kind: Profile, listKind: ProfileList, plural: profiles, singular: profile, shortNames: [prof] }
  scope: Namespaced
  versions:
    - name: v1alpha1
      served: true
      storage: true
      schema:
        openAPIV3Schema:
          type: object
          required: [spec]
          properties:
            spec:
              type: object
              required: [resource-type]
              x-kubernetes-preserve-unknown-fields: true
              properties:
                resource-type: { type: string }
                mode: { type: string, enum: [recommendation, enforce], default: recommendation }
                default-strategy:
                  description: "Strategy name (string) OR an inline strategy object"
                  x-kubernetes-preserve-unknown-fields: true
                restart-strategy: { type: object, x-kubernetes-preserve-unknown-fields: true }
                fields:
                  type: object
                  additionalProperties:
                    type: object
                    x-kubernetes-preserve-unknown-fields: true
                    properties:
                      strategy:
                        description: "Strategy name (string) OR inline strategy object"
                        x-kubernetes-preserve-unknown-fields: true
                      locator: { type: string }
                      min: { type: string }
                      max: { type: string }
```

- [ ] **Step 3c: Samples**

```yaml
# deploy/samples/strategy-percentile-p75.yaml
apiVersion: resource-broker.io/v1alpha1
kind: Strategy
metadata: { name: percentile-p75, namespace: default }
spec:
  algo: percentile
  args: { percentile: 75, lookback_hours: 168 }
```

```yaml
# deploy/samples/profile-k8s-aggressive.yaml
apiVersion: resource-broker.io/v1alpha1
kind: Profile
metadata: { name: k8s-aggressive, namespace: default }
spec:
  resource-type: k8s-pod
  mode: recommendation
  default-strategy: percentile-p75
  fields:
    cpu_request: { min: "100m", max: "4" }
    memory_request: { strategy: { algo: static, value: "512Mi" } }
```

- [ ] **Step 3d: RBAC + env**

In `deploy/resource-broker/rbac.yaml`, in the ClusterRole rule that grants the broker access to `resourceprofiles`, replace `resourceprofiles` with both new plurals:

```yaml
  - apiGroups: ["resource-broker.io"]
    resources: ["profiles", "strategies"]
    verbs: ["get", "list", "watch"]
```

In `.env.example`, add:

```bash
# CRD cache periodic re-list (seconds)
BROKER_CACHE_RESYNC_SECONDS=300
```

Remove `deploy/crd/resourceprofile-crd.yaml`:

```bash
git rm deploy/crd/resourceprofile-crd.yaml
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_crd_samples_parse.py -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(deploy): Strategy + Profile CRDs, samples, RBAC, env"
```

---

## Task 10: Service-1 cleanup + verification

**Files:**
- Modify/remove: `src/resource_broker/common/services/profile_loader.py` (remove), references in `watcher/services/collector.py`
- Modify: `README.md`

**Interfaces:** none. Removes dead per-pod CRD-fetch code that the cache replaces, leaving the tree importable.

- [ ] **Step 1: Find dead references**

Run: `uv run python - <<'PY'\nimport subprocess\nprint(subprocess.run(["grep","-rn","profile_loader\\|ProfileLoader","src"],capture_output=True,text=True).stdout)\nPY`
Expected: only `collector.py` (imports `CRD_GROUP/PLURAL/VERSION` from it) and `watcher.py` (uses `ProfileLoader`) reference it.

- [ ] **Step 2: Remove `profile_loader` import from collector**

In `src/resource_broker/watcher/services/collector.py`, replace the import:

```python
from resource_broker.common.services.profile_registry import (
    PROFILE_GROUP as CRD_GROUP,
    PROFILE_PLURAL as CRD_PLURAL,
    PROFILE_VERSION as CRD_VERSION,
)
```

(The collector itself is fully rewritten in Service 2; this keeps it importable now. `PROFILE_PLURAL` is `"profiles"`.)

- [ ] **Step 3: Delete `profile_loader.py`**

`watcher.py` (legacy per-pod enforcement) is rewritten/retired in Service 2/3. For this plan, delete the unused loader and leave `watcher.py` as-is (it is not imported by `serve`):

```bash
git rm src/resource_broker/common/services/profile_loader.py
```

If `watcher.py` import of `ProfileLoader` now breaks `controller`/`scrape` subcommands, that is acceptable — those subcommands are reworked in Service 2. Confirm the `serve` path is clean:

Run: `uv run python -c "import resource_broker.api.app"` → no error.

- [ ] **Step 4: Update README**

In `README.md`, replace the single-`ResourceProfile` CRD section with a short note: two CRDs now (`Strategy` + `Profile`), Profile references Strategy by name or inline; cache bootstraps from k8s with a Postgres snapshot fallback (`profile_snapshot`/`strategy_snapshot`). Update the "Profile persistence" section to describe snapshots instead of SCD tables. (Full README refresh lands in Service 3 Task 9 once webhook+engine exist.)

- [ ] **Step 5: Full lint + unit run + commit**

Run: `uv run ruff check src tests`
Expected: clean.

Run: `uv run pytest tests/unit/ -q`
Expected: all green.

```bash
git add -A
git commit -m "chore(cache): remove ProfileLoader, doc snapshot cache, lint clean"
```

---

## Self-Review

- **Spec coverage (1.a–1.d):** two CRDs + inline fallback (D1, T2/T3/T7/T9); cache built from CRDs (T6); fetch updates + new creations via dual watch + resync (T8); k8s-down cold-start fallback from Postgres snapshots `profile_snapshot`/`strategy_snapshot` (D2, T5/T8). ✓
- **Hashing identity:** `profile_hash`, `strategy_hash`, `resource_type_hash` all flow from the one `content_hash` helper (T1) — consistent and Service 2/3 consume the same primitive. ✓
- **Type consistency:** `Strategy.strategy_hash`, `Profile.profile_hash`, `ResourceType.resource_type_hash`, `resolve_field_strategy(profile, field_name, strategies)`, `CrdCache(profiles, strategies)`, `run_crd_watch_loops(co_api, cache, resync_seconds)` — names identical wherever referenced across tasks. ✓
- **No placeholders:** every code step shows complete code; every run step shows the command + expected result. ✓
