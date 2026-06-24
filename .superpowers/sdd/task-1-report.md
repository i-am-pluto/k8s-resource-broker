# Task 1 Report — Squash migration 0001 + ORM models + remove legacy metrics path

## What I changed

### Migration (`alembic/versions/0001_initial_schema.py`)
Verified only — the pre-existing edit (made before this task was dispatched) already
matched the brief exactly: `active_services`, `active_pods`, `pod_performance_metric`
in FK order, correct columns/types/nullability/defaults, correct unique constraints,
correct FK chain, correct index, `downgrade()` dropping in reverse FK order, revision
`"0001"` / `down_revision=None` unchanged. No fix needed.

### ORM models (`src/resource_broker/common/dao/orm_models.py`)
- Deleted `PodMetricModel`.
- Added `ActiveServiceModel`, `ActivePodModel`, `PodPerformanceMetricModel` matching the
  migration: same table names, columns, types, `server_default=text("gen_random_uuid()")`
  PKs, `UniqueConstraint`/`ForeignKey`/`Index` via `__table_args__`/column args, in the
  file's existing `Mapped[...] = mapped_column(...)` style.
- Added `UniqueConstraint` to the sqlalchemy import line (multi-line now, ruff-formatted).
- `ProfileVersionModel`, `ProfileFieldStrategyModel`, `ProfileRecommendationModel` left
  completely untouched.

### Deleted legacy repository
- Removed `src/resource_broker/common/dao/repositories/metrics.py` (`MetricsRepository`).

### Deleted now-orphaned collector
- Removed `src/resource_broker/performance_monitor/services/collector.py`
  (`MetricsCollector`) outright, per brief (its only caller, `scrape_runner.py`, no
  longer references it after this task's wiring removal).

### Stubbed `PercentileAlgorithm.compute()` (`src/resource_broker/recommender/algorithms/percentile.py`)
- Removed the `MetricsRepository`/`get_session` imports and the `_parse_resource_value`
  helper.
- `compute()` now raises `NotImplementedError` immediately, with a comment explaining
  percentile grouping is moving from `profile_name` to `active_service` in a later,
  separate rebuild. Class/method signature (`RecommendationAlgorithm` base,
  `compute(self, field, config, context=None) -> StrategyResult`) is unchanged, so
  imports of the class still work and the algorithm registry (`"percentile":
  PercentileAlgorithm`) still resolves.

### Test adjustments (`tests/unit/test_strategies.py`)
- Removed the `_parse_resource_value` import (symbol no longer exists).
- `test_parse_cpu_millicores` and `test_parse_memory` (the only two tests exercising
  the now-removed helper) are kept (not deleted) but marked
  `@pytest.mark.skip(reason="_parse_resource_value removed in Task 1; pending
  active_service-based percentile rewrite")`, bodies replaced with `pass`, with a
  comment above pointing at this task and the brief. No test called
  `PercentileAlgorithm.compute()` directly (only `test_registry_list` checks
  `"percentile" in available`, which still passes since the class/registry entry is
  unchanged) — so no `xfail` was needed for `.compute()` itself.

### `scrape_runner.py` (only the dead `MetricsCollector` wiring touched)
- Removed `from resource_broker.performance_monitor.services.collector import
  MetricsCollector` and the `MetricsAdapter` import (now unused).
- Removed `self._collector = MetricsCollector(adapter)` from `PodWatcher.__init__`.
- `adapter: MetricsAdapter` constructor parameter dropped — nothing else in `PodWatcher`
  used `adapter` once the collector was gone.
- Removed `collector_task = asyncio.create_task(self._collector.run_forever())` and the
  `finally` block's `collector_task.cancel()` / `await self._collector._adapter.close()`
  lines from `run()`.
- Everything else in the file — `ProfileLoader`, `compute_patches`, `_enforce_patches`,
  `_recreate_with_patches`, `_watch_pods`/`_handle_event`, the file-header `TODO(#24)`
  comment — left exactly as-is, per the brief's explicit instruction.
- `ruff format` reflowed a couple of now-shorter lines (e.g.
  `read_namespaced_pod` call site) — cosmetic only, no logic change.

### `__main__.py`
- `scrape()` now calls `PodWatcher()` (no `adapter=` kwarg).
- Dropped `create_metrics_adapter` import (now unused) and `settings` import (was only
  used to build the adapter; `scrape()` no longer needs it and nothing else in the file
  used it).
- `metrics_adapter.py` / `metrics_factory.py` themselves were left in place — the brief
  only asked to delete `collector.py`, not these adapter/factory modules, and nothing
  else currently imports them, so leaving them is a no-op (not new dead-code creation
  beyond what the brief specified, and out of scope to remove).

## Verification command output

```
$ python -c "from resource_broker.common.dao import orm_models"
OK1   (no output/exception — import succeeded)

$ grep -rn "PodMetricModel" src/
(no output — exit 1 / zero hits)

$ grep -rn "MetricsRepository" src/
(no output — exit 1 / zero hits)

$ python -c "from resource_broker.performance_monitor.controllers import scrape_runner"
OK2   (no output/exception — import succeeded)

$ python -c "from resource_broker.recommender.algorithms import percentile"
OK3   (no output/exception — import succeeded)

$ python -m pytest -q
..............ss...
17 passed, 2 skipped in 0.16s
```

The 2 skips are exactly `test_parse_cpu_millicores` and `test_parse_memory` (the two
tests exercising the removed `_parse_resource_value` helper), skipped per the brief's
instruction rather than deleted. No other test newly fails or skips because of this
task's changes.

## Files changed

- `alembic/versions/0001_initial_schema.py` — verified only, no edits (pre-existing
  controller edit was already correct)
- `src/resource_broker/common/dao/orm_models.py` — modified
- `src/resource_broker/common/dao/repositories/metrics.py` — deleted
- `src/resource_broker/performance_monitor/services/collector.py` — deleted
- `src/resource_broker/recommender/algorithms/percentile.py` — modified
- `src/resource_broker/performance_monitor/controllers/scrape_runner.py` — modified
- `src/resource_broker/performance_monitor/__main__.py` — modified
- `tests/unit/test_strategies.py` — modified

## Self-review findings

- **Completeness**: every item in the brief is done — migration verified, ORM models
  swapped, legacy repo deleted, collector deleted, percentile stubbed with helper
  removed, scrape_runner/​__main__ wiring cleaned, tests skipped per instruction.
- **Quality**: new ORM models follow the file's existing `Mapped[...] =
  mapped_column(...)` / `__table_args__` style; ran `ruff format` on the two files it
  flagged (orm_models.py, scrape_runner.py) — both were whitespace/line-wrap only, no
  semantic change, confirmed via diff and a post-format pytest run.
- **Discipline**: did not touch `ProfileVersionModel`/`ProfileFieldStrategyModel`/
  `ProfileRecommendationModel`; did not touch any `scrape_runner.py` logic beyond the
  `MetricsCollector` wiring; did not build any of the later-task service logic
  (`UsageCollector`, `StatusWatcher`, `K8sAdapter`, `AlertSink`, `ConfiguredResource`);
  did not add a new alembic revision; did not introduce any profile/strategy/
  recommendation import into `performance_monitor/`.
- **Testing**: ran the full suite once before committing (17 passed, 2 skipped,
  pristine — no errors, no unexpected failures). Confirmed via grep that no source file
  under `performance_monitor/` imports anything from `recommender/` or
  `broker_api.services.profile_loader` beyond the pre-existing, explicitly-flagged
  `scrape_runner.py` wart (untouched, out of scope).

## Concerns

- `src/resource_broker/performance_monitor/services/metrics_adapter.py` and
  `metrics_factory.py` are now unused by anything in the codebase (their only caller,
  `__main__.py`, no longer imports `create_metrics_adapter`). The brief only named
  `collector.py` for deletion, so I left these two files in place rather than deleting
  unlisted files — flagging this as a minor follow-up for a later task/cleanup pass if
  the controller wants them gone too.
- The two skipped tests (`test_parse_cpu_millicores`, `test_parse_memory`) are now
  effectively empty no-op tests (`pass` body) under `@pytest.mark.skip` — kept per the
  brief's "don't delete the test" instruction, but they'll need real bodies once
  `PercentileAlgorithm`/percentile parsing is rebuilt on `active_service` in a later task.
