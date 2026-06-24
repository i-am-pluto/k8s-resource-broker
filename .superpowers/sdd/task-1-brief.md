# Task 1 — Squash migration 0001 + ORM models + remove legacy metrics path

NOTE: `alembic/versions/0001_initial_schema.py` has already been edited (by
the controller, before this task was dispatched) to replace the `pod_metrics`
table with the three new tables below. Verify that edit is correct and
complete the rest of this task; fold everything into your own commit(s).

## Database — migration 0001 (verify, already applied)

`alembic/versions/0001_initial_schema.py` — revision stays `"0001"`,
`down_revision=None`. Three tables, in FK order:

- **`active_services`**: `id` uuid PK (server_default `gen_random_uuid()`),
  `namespace` String(253) not null, `service_name` String(253) not null,
  `resource_type` String(253) not null; unique `(namespace, service_name)`.
- **`active_pods`**: `id` uuid PK, `active_service_id` uuid FK →
  `active_services.id` not null, `pod_name` String(253) not null,
  `configured_resource` JSONB not null, `pod_status` String(64) not null,
  `restart_count` int not null default 0, `last_terminated_reason`
  String(64) nullable; unique `(active_service_id, pod_name)`.
- **`pod_performance_metric`**: `id` uuid PK, `active_pod_id` uuid FK →
  `active_pods.id` not null, `cpu_usage_cores` double precision not null,
  `mem_usage_bytes` bigint not null, `scraped_at` timestamptz not null
  server_default now(); index `(active_pod_id, scraped_at)`.

`downgrade()` drops in reverse FK order. If the existing edit doesn't match
this exactly, fix it.

## ORM models

In `src/resource_broker/common/dao/orm_models.py`: delete `PodMetricModel`.
Add `ActiveServiceModel`, `ActivePodModel`, `PodPerformanceMetricModel`
matching the migration above, in the existing file's style (`Mapped[...] =
mapped_column(...)`, `UniqueConstraint`/`Index`/`ForeignKey` in
`__table_args__`/column args, matching patterns already in the file).
Leave `ProfileVersionModel`, `ProfileFieldStrategyModel`,
`ProfileRecommendationModel` completely untouched.

## Delete the legacy metrics repository

Delete `src/resource_broker/common/dao/repositories/metrics.py`
(`MetricsRepository` class) entirely.

## Stub now-broken callers (don't fix them — this task is schema-only)

- `src/resource_broker/performance_monitor/services/collector.py` — its
  only caller is `scrape_runner.py`, which you are also editing in this
  task to stop calling it (see below). Once that wiring is removed, delete
  `collector.py` outright (no dead stub needed — nothing imports it anymore).

- `src/resource_broker/recommender/algorithms/percentile.py` — replace
  `PercentileAlgorithm.compute()`'s body so it raises `NotImplementedError`
  with a short docstring/comment explaining percentile grouping is moving
  from `profile_name` to `active_service` in a later, separate rebuild (out
  of scope here). Keep the class and method signature intact (same
  `RecommendationAlgorithm` base, same `compute(self, field, config,
  context=None) -> StrategyResult` signature) so anything importing the
  class still imports cleanly. Remove the now-dead `_parse_resource_value`
  helper and the `MetricsRepository` import from this file.
  Check `tests/` (e.g. `tests/unit/test_strategies.py` or similar — grep
  for "Percentile" or "percentile") for any test exercising
  `PercentileAlgorithm.compute()`; if one exists, mark it `xfail` (with
  `pytest.mark.xfail(reason="...", strict=False)`) or skip it with a
  one-line comment pointing at this stub — don't delete the test.

## Fix `scrape_runner.py`'s dead `MetricsCollector` wiring (nothing else)

In `src/resource_broker/performance_monitor/controllers/scrape_runner.py`:
- Remove `from resource_broker.performance_monitor.services.collector import MetricsCollector`.
- Remove `self._collector = MetricsCollector(adapter)` from `PodWatcher.__init__`.
- Remove the `collector_task = asyncio.create_task(self._collector.run_forever())`
  line and its corresponding `collector_task.cancel()` /
  `await self._collector._adapter.close()` lines in `run()`'s `finally` block.
- If `adapter` is now unused anywhere else in `__init__`/the class, drop the
  `adapter: MetricsAdapter` constructor parameter and the now-unused
  `MetricsAdapter` import too.
- **Do not touch anything else in this file** — `ProfileLoader`,
  `compute_patches`, `_enforce_patches`, `_recreate_with_patches`, the
  `_watch_pods`/`_handle_event` watch logic, all stay exactly as they are.
  This file's profile/strategy coupling is a pre-existing, intentionally
  untouched transitional wart (flagged by its own `TODO(#24)` comment at
  the top of the file) — out of scope for this task.

Update `src/resource_broker/performance_monitor/__main__.py`'s `scrape`
command to match `PodWatcher`'s new constructor signature (if you dropped
the `adapter` param, stop passing `adapter=adapter` and drop the now-unused
`create_metrics_adapter`/`settings` imports if nothing else in that file
uses them — check before removing).

## Constraints

- Don't add a new alembic revision file — edit `0001` in place (already
  done, verify only).
- Don't touch `ProfileVersionModel`/`ProfileFieldStrategyModel`/
  `ProfileRecommendationModel` or anything under `recommender/` besides the
  one `percentile.py` stub described above.
- No profile/strategy/recommendation imports introduced anywhere in
  `performance_monitor/` by this task.

## Verification (run and report output)

- `python -c "from resource_broker.common.dao import orm_models"` — must
  import cleanly.
- `grep -rn "PodMetricModel" src/` — must return zero hits.
- `grep -rn "MetricsRepository" src/` — must return zero hits.
- `python -c "from resource_broker.performance_monitor.controllers import scrape_runner"` — must import cleanly.
- `python -c "from resource_broker.recommender.algorithms import percentile"` — must import cleanly.
- Run the existing test suite (`pytest`) and report pass/fail counts —
  some pre-existing tests may need the `xfail`/skip treatment described
  above; no other test should newly fail because of this task's changes.

## Report

Write your full report (what you changed, command output, any deviations
or judgment calls) to `.superpowers/sdd/task-1-report.md`. Return to the
controller only: status (DONE / DONE_WITH_CONCERNS / NEEDS_CONTEXT /
BLOCKED), the commit hash(es), a one-line test summary, and any concerns.
