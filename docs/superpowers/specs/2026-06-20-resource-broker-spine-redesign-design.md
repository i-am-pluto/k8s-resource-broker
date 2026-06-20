# Resource-Broker Spine Redesign — Design

- **Date:** 2026-06-20
- **Status:** Approved (Phase 1 / "spine")
- **Authors:** team (i-am-pluto et al.), formalized with Claude

---

## 1. Context

The current broker (see `src/resource_broker/`) has:

- **One** CRD (`ResourceProfile`) with strategy + fields inline.
- Resource types hardcoded in a `ResourceType` class (`k8s-pod`).
- Algorithms in a code registry (`static`, `percentile`, `derived`).
- Recommendations computed **on demand** at webhook/watcher time, behind a TTL cache.
- Profiles persisted SCD-Type-2 in Postgres as a write-through cache.
- A per-pod mutating webhook + a per-pod watcher (in-place resize / delete-recreate).

We are moving to a **precompute** architecture: recommendations are computed in the
background and stored keyed by content hashes; the hot path (admission webhook) only
**looks up** a precomputed row. Compute is offloaded off the API server.

## 2. Goals (Phase 1 — spine)

1. Split Profile and Strategy into **two CRDs**; Profile references Strategy by name.
2. Keep resource types in **code** (registry; `k8s-pod` now, `spark` etc. later). CRD form is future scope but resource types are still **versioned + hashed**.
3. Per-broker **in-memory CRD cache** for Profile + Strategy: bootstrap on startup, watch-driven invalidation, periodic resync.
4. **Hash-keyed** storage: `profile_hash`, `strategy_hash`, `resource_type_hash` form the recommendation identity.
5. New **DB tables**: `active_services`, `pod_performance_metric`, `service_recommendations`.
6. A background **engine** (cron) that precomputes **service-level** recommendations for active services using **builtin** strategies.
7. **Service-level mutating webhook** on Deployment admission: build service id + 3 hashes → look up recommendation → patch pod template; **pass-through on miss** (keep the deployment's declared values).

## 3. Non-goals (deferred to later phases)

- **P2 — Offloading:** `algo: image` strategies that run a user-supplied docker image in an on-demand Job (selective: builtin stays in-broker, image is offloaded). Phase 1 ships the Strategy `algo: image` *schema* but no runner.
- **P3 — Pod-level mutation:** VPA-style resize/restart (per `restart-strategy` in Profile) **and** HPA-style replica tuning; `pod_recommendations` table; "apply-or-NOT" hysteresis controller.
- **ResourceType CRD** (stays code in P1).
- TLS/cert-manager automation, Helm chart.

## 4. Architecture — three processes, one image

| Process | Responsibility | Reads | Writes |
|---|---|---|---|
| **api** | FastAPI: mutating webhook lookup + REST | `service_recommendations`, in-mem registries | — (no hot-path compute) |
| **watcher** | watch Profile/Strategy CRDs + Deployments; scrape metrics | k8s API, Prometheus | CRD cache, `active_services`, `pod_performance_metric` |
| **engine** | cron: precompute service recs for active services | metric + active tables, CRD cache | `service_recommendations` |

All three run from the same image, selected by the `__main__` subcommand
(`serve`, `watcher`, `engine`). Each process maintains its **own** in-memory CRD cache.

**Invariant: the webhook never computes.** It only looks up a precomputed row.

## 5. CRD cache layer (per broker)

Each process holds its own in-memory registries (generalization of today's
`ProfileRegistry`):

```
ProfileRegistry       { ns/name -> Profile,  profile_hash }
StrategyRegistry      { ns/name -> Strategy, strategy_hash }
ResourceTypeRegistry  (code) { name -> ResourceType, version, resource_type_hash }
```

Lifecycle:

1. **Bootstrap** — on startup, `list` all Profiles + Strategies from the k8s API,
   populate the cache, compute hashes. If the k8s API is unreachable, fall back to the
   last-known DB snapshot (cold-start only).
2. **Watch invalidation** — one watch stream per kind. `ADDED`/`MODIFIED` → upsert +
   recompute that object's hash. `DELETED` → evict.
3. **Cascade on change** — when a Profile/Strategy hash changes, its dependent
   `active_services` rows carry the new hash, so existing `service_recommendations`
   no longer match (stale). The next webhook lookup misses → pass-through until the
   engine recomputes. No manual rec-table invalidation needed.
4. **Periodic resync** — re-`list` every `BROKER_CACHE_RESYNC_SECONDS` (default 300)
   to reconcile missed watch events / dropped connections. Watch-loop restart also
   re-lists.

**Hash is the cache-version key.** Lookup correctness falls out of hash matching.

## 6. CRDs

### 6.1 Strategy (`kind: Strategy`)

```yaml
apiVersion: resource-broker.io/v1alpha1
kind: Strategy
metadata: { name: percentile-p75, namespace: default }
spec:
  algo: percentile        # builtin name (percentile|static|derived) OR "image"
  args:                   # strategy-specific args
    percentile: 75
    lookback_hours: 168
  image: null             # only when algo: image  (P2 runner; schema only in P1)
```

- **Builtin** (`algo` in the code registry): computed in-broker by the engine.
- **Image** (`algo: image`): `image` field names a docker image; the engine spawns an
  on-demand Job to compute (P2). P1 validates the schema and rejects `algo: image` at
  compute time with a clear "not yet supported" status.

### 6.2 Profile (`kind: Profile`)

```yaml
apiVersion: resource-broker.io/v1alpha1
kind: Profile
metadata: { name: k8s-aggressive, namespace: default }
spec:
  resource-type: k8s-pod              # resolved in code ResourceTypeRegistry
  mode: recommendation | enforce
  default-strategy: percentile-p75    # Strategy ref by name (namespace-local)
  restart-strategy:                   # P3 only; parsed + stored in P1, unused
    on-resize: recreate
  fields:                             # only listed fields are tuned
    cpu_request:
      strategy: percentile-p75        # optional per-field Strategy ref override
      locator: null                   # optional; resource-type provides default
      min: 100m
      max: "4"
    memory_request:
      strategy: static-512mi
```

Field resolution order for a field's strategy: `fields.<f>.strategy` →
`spec.default-strategy` → resource-type field default. `min`/`max` are guardrails
applied after compute.

### 6.3 ResourceType (code, not CRD in P1)

`ResourceType` classes in `resource_types/` gain:

- `version: str` — bump when the field map changes.
- field locators are **pod-template-relative** (e.g. `/spec/containers/0/resources/requests/cpu`).
  For a Deployment the webhook prepends `/spec/template`.
- `resource_type_hash = sha256(canonical({version, fields}))`.

Registry keyed by name (`k8s-pod`, future `spark`, `vansh-service`).

## 7. Identity + hashing

- **service_id** = `{namespace}/{deployment-name}`; `service_uid` (Deployment uid)
  stored for uniqueness across delete/recreate.
  - Webhook (Deployment admission): name is taken directly from the object.
  - Watcher/engine (running pods): walk `ownerReferences` Pod → ReplicaSet →
    Deployment to resolve `service_id`.
- **profile_hash / strategy_hash / resource_type_hash** = SHA-256 of the canonical
  JSON of each spec (sorted keys). A recommendation row is valid only while **all
  three** equal the current hashes. Any change → lookup miss → recompute.

## 8. Database schema

New migration `0004`. Drops the SCD profile tables (CRDs are the source of truth now)
and folds `pod_metrics` into `pod_performance_metric`.

```
active_services
  namespace            text
  service_name         text
  service_uid          text
  profile_name         text
  profile_hash         text
  strategy_hash        text
  resource_type_hash   text
  mode                 text          -- recommendation | enforce
  status               text          -- active | paused | error
  updated_at           timestamptz
  PRIMARY KEY (namespace, service_name)

pod_performance_metric
  id                   uuid pk
  namespace            text
  service_name         text          -- resolved via ownerRef at scrape time
  pod_name             text
  container            text
  cpu_usage_cores      double
  mem_usage_bytes      bigint
  configured_resources jsonb         -- requests/limits the pod was created with
  resource_type_hash   text
  scraped_at           timestamptz
  INDEX (service_name, scraped_at)

service_recommendations
  namespace            text
  service_name         text
  profile_hash         text
  strategy_hash        text
  resource_type_hash   text
  recommendation       jsonb         -- field_name -> computed value (locator-independent)
  computed_at          timestamptz
  PRIMARY KEY (namespace, service_name, profile_hash, strategy_hash, resource_type_hash)
```

**Dropped:** `resource_profile_versions`, `resource_profile_field_strategies`,
`profile_recommendations` (migration 0003). **Folded:** `pod_metrics` (0001) →
`pod_performance_metric`.

Retention: a periodic sweep deletes `service_recommendations` rows whose hashes no
longer match any `active_services` row and are older than
`BROKER_REC_RETENTION_DAYS` (default 7).

## 9. Data flow (spine)

```
Deploy created
  └─ watcher (Deployment watch): if profile annotation present →
       resolve Profile+Strategy from cache, compute 3 hashes →
       upsert active_services(status=active)

scrape loop (watcher)
  └─ query Prometheus per active service → resolve service_name via ownerRef →
       insert pod_performance_metric

engine cron (every BROKER_RECOMMEND_INTERVAL_SECONDS)
  └─ for each active_services row:
       resolve Profile+Strategy from cache
       for each field: run builtin algo (percentile reads pod_performance_metric
         percentiles for that service_name), apply min/max →
       upsert service_recommendations keyed by (ns, service, 3 hashes)

Deploy admission (api webhook)
  └─ build service_id + current 3 hashes →
       SELECT service_recommendations by (ns, service, 3 hashes)
       hit  → patch /spec/template pod resources, admit
       miss → admit unchanged (pass-through; keep declared defaults)
```

## 10. Config additions (`BROKER_*`)

| Var | Default | Purpose |
|---|---|---|
| `CACHE_RESYNC_SECONDS` | 300 | CRD cache periodic re-list |
| `RECOMMEND_INTERVAL_SECONDS` | 3600 | engine cron period |
| `REC_RETENTION_DAYS` | 7 | stale recommendation sweep |
| `WEBHOOK_TARGET_KINDS` | `Deployment` | admission targets (StatefulSet later) |

## 11. Changes to existing code

| Component | Change |
|---|---|
| `common/models/profile.py` | Split: Profile references Strategy by name; new Strategy model |
| `common/services/profile_registry.py` | Generalize → add `StrategyRegistry`, hash computation, resync timer |
| `common/services/profile_loader.py` | **Remove** (per-pod CRD fetch replaced by registry) |
| `common/services/recommendation_service.py` | **Remove** on-demand compute; replace with lookup-only service |
| `watcher/services/patcher.py` (`compute_patches`) | Reused by the **engine**, not the webhook |
| `watcher/controllers/watcher.py` | Per-pod enforce path → deferred to P3; add Deployment watch → active_services |
| `watcher/services/collector.py` | Write `pod_performance_metric` with resolved `service_name` |
| `api/controllers/webhook.py` + `handler.py` | Deployment-targeted lookup + pass-through |
| `common/dao/repositories/profiles.py` + SCD tables | **Remove**; new repos for the 3 new tables |
| `resource_types/base.py` | Add `version`, `resource_type_hash`, template-relative locators |
| migrations | `0004`: drop SCD tables, fold `pod_metrics`, create 3 new tables |

## 12. Testing

- Unit: hashing canonicalization (stable across key order), strategy resolution
  order, builtin algos (percentile/static/derived) against fixture metrics,
  webhook hit/miss patch shaping, ownerRef → service_id resolution.
- Integration (minikube script extension): apply Strategy + Profile CRDs, deploy an
  annotated Deployment, seed metrics, run engine once, assert the next rollout's pod
  template is patched; assert cold-start deploy passes through unchanged.

## 13. Deferred / open

- Webhook also covering StatefulSet/DaemonSet (P1 = Deployment only).
- Multi-container resource types (locators index container `0` today).
- DB snapshot fallback fidelity for full Profile+Strategy specs (P1 stores enough to
  rebuild the cache cold).
