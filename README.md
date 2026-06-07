# k8s-resource-broker

> **⚠️ Work in progress — not production-ready. APIs and CRD schema may change.**

A Kubernetes controller that automatically patches pod resource requests and limits
based on configurable `ResourceProfile` CRDs. Annotate a pod with a profile name;
the broker rewrites its CPU/memory fields before (or after) it schedules.

---

## What it does

Most teams either over-provision pods to avoid OOMs or under-provision and get throttled.
Resource-broker decouples "what resources a pod declares" from "what resources it actually needs"
by intercepting pod creation and rewriting the resource spec according to a profile.

**Two enforcement mechanisms** (configurable, can run both simultaneously):

| Mode | How it works |
|------|-------------|
| **Mutating webhook** | Intercepts `Pod CREATE` admission requests and patches resources in-flight before the pod is scheduled |
| **Watcher / post-create** | Watches for annotated pods after creation and applies patches via the Kubernetes API |

**Two profile modes:**

| Mode | Behaviour |
|------|-----------|
| `recommendation` | Compute the recommended values, log them — don't apply (safe to try first) |
| `enforce` | Compute and apply JSON patches to the pod spec |

**Pluggable recommendation algorithms:**

| Algorithm | Description |
|-----------|-------------|
| `static` | Fixed values — set it and forget it |
| `percentile` | Reads historical usage from PostgreSQL (scraped from Prometheus/Thanos/VictoriaMetrics/Mimir) and sets resources to the Nth percentile of actual usage |
| `derived` | Formula-based — derive one field from another (e.g. limit = 2× request) |

---

## Architecture

```
┌────────────────────────────────────────────────────────────┐
│  Kubernetes Cluster                                        │
│                                                            │
│  ┌─────────────┐  annotate   ┌──────────────────────────┐ │
│  │   Your Pod  │────────────▶│   MutatingWebhook        │ │
│  │  (pending)  │             │   /api/v1/webhook/mutate  │ │
│  └─────────────┘             └────────────┬─────────────┘ │
│                                           │ patch          │
│  ┌─────────────┐  watch      ┌────────────▼─────────────┐ │
│  │ResourceProf │────────────▶│   resource-broker        │ │
│  │ile CRD      │             │   (FastAPI + watcher)     │ │
│  └─────────────┘             └────────────┬─────────────┘ │
│                                           │                │
│                               ┌───────────▼──────────┐    │
│                               │   PostgreSQL          │    │
│                               │   (metrics history)   │    │
│                               └───────────────────────┘    │
└────────────────────────────────────────────────────────────┘
                                      ▲
                              scrapes │
                         ┌────────────┴────────────┐
                         │  Prometheus / Thanos /   │
                         │  VictoriaMetrics / Mimir │
                         └─────────────────────────┘
```

**Key components:**

```
src/resource_broker/
├── algorithms/          # Recommendation strategies (static, percentile, derived)
├── api/
│   ├── controllers/     # FastAPI routes — webhook, profiles, recommendations, health
│   └── services/        # Profile service
├── common/
│   ├── dao/             # SQLAlchemy async ORM + repositories
│   ├── models/          # Pydantic domain models (profile, patch, strategy)
│   └── services/        # Profile registry, metrics adapter, recommendation service
├── resource_types/      # Pluggable resource type definitions (k8s-pod)
├── watcher/
│   ├── controllers/     # CRD watcher, pod event handler
│   └── services/        # Patcher (applies JSON patches), metrics collector
└── config.py            # Pydantic-settings config (env-prefixed BROKER_*)
```

---

## ResourceProfile CRD

```yaml
apiVersion: resource-broker.io/v1alpha1
kind: ResourceProfile
metadata:
  name: my-app-profile
  namespace: default
spec:
  resource-type: k8s-pod
  mode: enforce           # recommendation | enforce
  strategy:               # default strategy for all fields
    algo: percentile
    percentile: 90
    lookback_hours: 168   # 7 days
  fields:
    cpu_request: {}       # use default strategy
    memory_request:
      strategy:
        algo: static
        value: "512Mi"
    cpu_limit:
      strategy:
        algo: derived
        source: cpu_request
        multiplier: 2.0
    memory_limit:
      min: "256Mi"
      max: "4Gi"
```

Annotate pods to opt in:

```yaml
metadata:
  annotations:
    resource-broker/profile: my-app-profile
```

---

## Local development

### Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/)
- Docker + Docker Compose
- `kubectl` + `minikube` (for integration testing)

### Setup

```bash
# Clone and install dependencies
git clone https://github.com/i-am-pluto/k8s-resource-broker
cd k8s-resource-broker
uv sync

# Copy and edit env
cp .env.example .env

# Start PostgreSQL
docker compose up -d postgres

# Run migrations
uv run alembic upgrade head

# Start the broker (API + watcher)
uv run python -m resource_broker serve
```

The API will be at `http://localhost:8080`. Swagger docs at `/docs`.

### Configuration

All config is via environment variables prefixed `BROKER_`:

| Variable | Default | Description |
|----------|---------|-------------|
| `BROKER_DATABASE_URL` | `postgresql+asyncpg://broker:broker@localhost:5432/broker` | PostgreSQL DSN |
| `BROKER_WEBHOOK_MODE` | `both` | `admission` / `post-create` / `both` |
| `BROKER_WEBHOOK_FAIL_OPEN` | `true` | On webhook error: allow pod (`true`) or deny (`false`) |
| `BROKER_METRICS_ADAPTER_TYPE` | `prometheus` | `prometheus` / `thanos` / `victoria_metrics` / `mimir` / `kubecost` |
| `BROKER_METRICS_URL` | `http://prometheus:9090` | Metrics backend URL |
| `BROKER_K8S_IN_CLUSTER` | `true` | Use in-cluster kubeconfig (`false` = use `~/.kube/config`) |
| `BROKER_LOG_LEVEL` | `INFO` | `DEBUG` / `INFO` / `WARNING` / `ERROR` |

See [`.env.example`](.env.example) for full list.

### Run tests

```bash
uv run pytest tests/unit/
```

### Integration test (minikube)

```bash
./scripts/setup-minikube.sh
```

Starts a minikube cluster, builds the image, deploys PostgreSQL, runs migrations,
deploys the broker, creates an annotated pod, and asserts it was patched.

---

## Deploying to a real cluster

```bash
# Install CRD
kubectl apply -f deploy/crd/resourceprofile-crd.yaml

# Create namespace, RBAC, ConfigMap, Deployment, Service
kubectl apply -f deploy/namespace.yaml
kubectl apply -f deploy/resource-broker/

# Apply sample profiles
kubectl apply -f deploy/samples/profile-efficient.yaml

# Annotate your pods
kubectl annotate pod my-pod resource-broker/profile=k8s-efficient
```

The mutating webhook requires TLS. Set `BROKER_TLS_CERT_FILE` and `BROKER_TLS_KEY_FILE`
(or use [cert-manager](https://cert-manager.io/) and populate `caBundle` in
`deploy/resource-broker/mutating-webhook.yaml`).

---

## Current status / known gaps

This is a working skeleton — the core machinery is in place but several pieces are incomplete:

- [ ] `profiles` repository (`common/dao/repositories/profiles.py`) not yet implemented — profile persistence via DB is missing, currently profiles load from CRD watch only
- [ ] TLS bootstrap automation (cert-manager integration not wired)
- [ ] `derived` algorithm formula evaluation is stubbed
- [ ] Metrics scraper loop needs wiring to the collector service
- [ ] No Helm chart yet — raw manifests only
- [ ] API services layer partially stubbed (`api/services/`)
- [ ] Integration tests are setup-script only — no pytest integration suite

---

## Tech stack

- **Python 3.12** + [uv](https://docs.astral.sh/uv/)
- **FastAPI** — REST API and webhook handler
- **SQLAlchemy** (async) + **asyncpg** — metrics persistence
- **Alembic** — schema migrations
- **Kubernetes Python client** — CRD watch + pod patching
- **Pydantic v2** + pydantic-settings — config and models
- **structlog** — structured JSON logging
- **Prometheus / Thanos / VictoriaMetrics / Mimir / Kubecost** — pluggable metrics backends
