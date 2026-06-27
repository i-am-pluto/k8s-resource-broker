# Profiles and Strategies

This document explains how `ResourceProfile` and `Strategy` CRDs work together to drive
automatic resource recommendations and enforcement in k8s-resource-broker.

---

## Overview

| CRD | Scope | Purpose |
|-----|-------|---------|
| `Strategy` | Cluster | Declares an algorithm and its argument schema; optionally sets a re-evaluation schedule |
| `ResourceProfile` | Cluster | Targets a set of pod fields, references a Strategy by name, supplies argument values |

The two-CRD design separates *algorithm definition* from *algorithm parameterisation*:
one `percentile` Strategy can serve hundreds of Profiles, each with a different lookback
window or target percentile, without duplicating the algorithm declaration.

---

## Strategy CRD

A Strategy CR declares:
- **algo** — the algorithm identifier (`percentile`, `static`, `derived`, or a custom name)
- **args** — the argument schema (names, types, whether required, defaults, descriptions)
- **schedule.run-every** *(optional)* — how often the background worker should re-evaluate
  pods that reference this strategy (minute granularity, e.g. `30m`, `6h`, `1d`)

```yaml
apiVersion: resource-broker.io/v1alpha1
kind: Strategy
metadata:
  name: percentile          # referenced by Profiles as { ref: percentile }
spec:
  algo: percentile
  schedule:
    run-every: 360m         # re-evaluate every 6 hours
  args:
    percentile-type:
      type: string
      enum: [p50, p75, p90, p95, p99]
      required: true
      default: p90
      description: "Which percentile of historical usage to target."
    coolback-period:
      type: string
      required: false
      default: "24h"
      description: "Lookback window to sample (e.g. 24h, 7d, 168h)."
```

### Built-in strategies

#### `percentile`

Queries historical CPU and memory usage from the PostgreSQL metrics store and recommends
the Nth percentile over a configurable lookback window.

| percentile-type | Meaning |
|----------------|---------|
| `p50` | Median — half of observed usage was at or below this value |
| `p75` | 75th percentile — a moderately conservative target |
| `p90` | 90th percentile — avoids throttling on ~90% of historical samples (default) |
| `p95` | 95th percentile — more headroom; useful for bursty workloads |
| `p99` | 99th percentile — near-maximum; good for latency-sensitive services |

The `coolback-period` (lookback window) controls how far back the query reaches.
A longer window smooths out short traffic spikes; a shorter window reacts faster to
sustained usage changes.

Common values: `24h` (1 day), `168h` (7 days), `30d` (30 days).

#### `static`

Emits a fixed Kubernetes quantity string. Useful when a field's value is known in advance
or is not driven by historical metrics.

```yaml
spec:
  algo: static
  args:
    value:
      required: true
      description: "Fixed quantity (e.g. 500m, 256Mi, 1.5)."
```

#### `derived`

Derives a value from another field via a configurable transform expression
(e.g. cpu_limit = 2× cpu_request). The transform can be a string shorthand
(`to_string`, `to_int`, `to_float`) or an object `{op: mul, operand: 1.5}`.

```yaml
spec:
  algo: derived
  args:
    source-field:
      type: string
      required: true
      description: "JSONPath into the pod spec to read from."
    transform:
      required: false
      description: "Shorthand string or {op, operand} object."
```

---

## ResourceProfile CRD

A Profile CR declares:
- **resource-type** — what kind of resource this profile manages (currently `k8s-pod`)
- **mode** — `recommendation` (log only) or `enforce` (apply patches)
- **default-strategy** — the strategy used for all fields unless overridden
- **fields** — per-field configuration (strategy override, min/max clamps)

```yaml
apiVersion: resource-broker.io/v1alpha1
kind: Profile
metadata:
  name: my-app-profile
spec:
  resource-type: k8s-pod
  mode: enforce
  default-strategy:           # supplies arg values to the percentile Strategy
    ref: percentile
    args:
      percentile-type: p90
      coolback-period: 168h   # 7-day lookback window
  fields:
    cpu_request: {}           # uses default-strategy (p90, 168h lookback)
    memory_request:
      strategy:               # per-field override — more conservative for memory
        ref: percentile
        args:
          percentile-type: p75
          coolback-period: 72h
    cpu_limit:
      strategy:
        ref: derived
        args:
          source-field: /spec/containers/0/resources/requests/cpu
          transform:
            op: mul
            operand: 2.0
    memory_limit:
      strategy:
        ref: static
        args:
          value: "4Gi"
      min: "256Mi"
      max: "8Gi"
```

Annotate pods to opt in:

```yaml
metadata:
  annotations:
    resource-broker/profile: my-app-profile
```

---

## The ref + args binding model

When a Profile specifies `{ ref: percentile, args: { percentile-type: p75, coolback-period: 168h } }`:

1. The broker looks up the `percentile` Strategy CR from the `StrategyRegistry`.
2. It validates each arg value against the declared schema (type, enum, required).
3. It constructs a `FieldStrategy` with `algo="percentile"`, `percentile=75`,
   `lookback_hours=168.0` and passes it to `PercentileAlgorithm.compute()`.
4. The algorithm queries PostgreSQL for the p75 value across the last 168 hours.

The Strategy CR owns the schema; the Profile owns the values. This means:
- One `percentile` Strategy → many Profiles, each with a different `percentile-type`
- Changing an arg's `default` in the Strategy affects all Profiles that omit that arg

---

## Per-field inheritance

Fields without an explicit strategy inherit `default-strategy`. Fields with an explicit
strategy use that instead. The inheritance is shallow — there is no partial merge between
the default and the per-field strategy.

```
Profile.default-strategy  →  used by any field with an empty {}
Profile.fields[x].strategy  →  fully overrides default-strategy for field x
```

---

## Schedule: run-every and the periodic worker

When a Strategy carries `schedule.run-every`, the background `PeriodicCheckWorker` will
re-evaluate all pods whose profiles reference that strategy at that cadence.

**Duration units:** `s` (seconds), `m` (minutes), `h` (hours), `d` (days), `w` (weeks).
All values are normalised to minutes internally (e.g. `6h` → 360 minutes).

**Multiple strategies, different schedules:**
A profile can reference multiple strategies (default + per-field overrides). The worker
uses the **shortest** `run-every` across all referenced strategies. This ensures no field
falls behind its desired cadence.

```
Profile A references:
  - default-strategy: percentile  → run-every: 360m
  - fields.cpu_limit: derived     → no schedule

Effective run-every for Profile A: 360m
```

**Strategies without a schedule** (e.g. `static`, `derived`) do not trigger periodic checks.
The worker only runs for profiles where at least one referenced strategy has a schedule.

---

## Cold-start fallback

At startup, each replica:
1. Lists all Profile and Strategy CRs from the Kubernetes API server.
2. Writes them to flat snapshot tables (`profile_snapshots`, `strategy_snapshots`) in PostgreSQL
   using a SHA-256 hash-gate (skips the write if the hash is unchanged — safe for multi-replica).
3. Starts watch streams for both CRD types.

If the Kubernetes API is unavailable at startup, the replica falls back to reading from the
snapshot tables instead. Watch streams are retried until the API recovers.

---

## Full example: applying two Strategy CRs and a Profile

```bash
# 1. Apply Strategy CRs (cluster-scoped, no namespace)
kubectl apply -f deploy/crd/strategy-crd.yaml
kubectl apply -f deploy/samples/strategy-percentile-p75.yaml

# 2. Apply Profile CRD
kubectl apply -f deploy/crd/profile-crd.yaml

# 3. Apply a Profile CR
kubectl apply -f deploy/samples/profile-k8s-aggressive.yaml

# 4. Annotate a pod
kubectl annotate pod my-pod resource-broker/profile=k8s-aggressive
```

The broker picks up the annotated pod, resolves the profile, evaluates the strategy
for each field, and either logs the recommendations or applies patches depending on `mode`.

---

## Deploying the CRDs

```bash
kubectl apply -f deploy/crd/profile-crd.yaml
kubectl apply -f deploy/crd/strategy-crd.yaml
```

See `deploy/samples/` for ready-to-use Profile and Strategy CRs.
