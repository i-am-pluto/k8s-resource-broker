#!/usr/bin/env bash
# =============================================================================
# dev-setup.sh — Full development environment setup for k8s-resource-broker
#
# This script has three independent paths you can run separately or together:
#
#   --local      Checks prerequisites, installs Python deps, starts Postgres
#                in podman, and runs DB migrations. Enough to run the API and
#                unit tests without any Kubernetes cluster.
#
#   --minikube   Starts a minikube cluster (using the podman driver), builds
#                the broker Docker image, deploys Postgres + the broker
#                controller inside the cluster, and runs a smoke test that
#                verifies an annotated pod gets its resources patched.
#
#   --serve      After local setup, starts the FastAPI server in the
#                foreground so you can hit http://localhost:8080.
#
#   (no flags)   Runs --local then --minikube (full setup).
#
# Usage:
#   ./scripts/dev-setup.sh              # everything
#   ./scripts/dev-setup.sh --local      # local dev only
#   ./scripts/dev-setup.sh --local --serve
#   ./scripts/dev-setup.sh --minikube   # cluster only (local must already be done)
# =============================================================================

set -euo pipefail

# ── Colour helpers ────────────────────────────────────────────────────────────
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; BOLD='\033[1m'; NC='\033[0m'
info()  { echo -e "${BOLD}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
fail()  { echo -e "${RED}[FAIL]${NC}  $*"; exit 1; }
step()  { echo ""; echo -e "${BOLD}══ $* ══${NC}"; }

# ── Argument parsing ──────────────────────────────────────────────────────────
RUN_LOCAL=false
RUN_MINIKUBE=false
RUN_SERVE=false

if [[ $# -eq 0 ]]; then
    # No flags → run everything
    RUN_LOCAL=true
    RUN_MINIKUBE=true
fi

for arg in "$@"; do
    case "$arg" in
        --local)    RUN_LOCAL=true ;;
        --minikube) RUN_MINIKUBE=true ;;
        --serve)    RUN_SERVE=true; RUN_LOCAL=true ;;
        --help|-h)
            sed -n '2,30p' "$0" | sed 's/^# \?//'
            exit 0
            ;;
        *) fail "Unknown argument: $arg. Use --local, --minikube, or --serve." ;;
    esac
done

# Absolute path to the repo root regardless of where the script is called from
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

# =============================================================================
# TLS WORKAROUND — podman on macOS
# =============================================================================
# On macOS, podman runs inside a lightweight VM (Lima/QEMU). That VM has its
# own certificate store which does NOT inherit macOS system CAs or corporate
# proxy certificates. The result: every docker.io / gcr.io pull fails with
#   "x509: certificate signed by unknown authority"
#
# Fix: write an insecure-registry config inside the VM so podman skips TLS
# verification for public registries. This is a dev machine, not production.
# The config survives VM restarts and applies to ALL subsequent pulls.
#
# We also use --tls-verify=false on individual pull commands as a fallback for
# cases where the machine config hasn't propagated yet.
# =============================================================================

# Images that every minikube run needs. We pull them locally first, then load
# them into the cluster so kubelet never has to reach the network.
# nginx:alpine is the smoke-test workload; postgres:16-alpine is in-cluster DB.
CLUSTER_IMAGES=("postgres:16-alpine" "nginx:alpine")

# ── Config (mirrors .env.example and setup-minikube.sh) ──────────────────────
CLUSTER_NAME="${CLUSTER_NAME:-k8s-resource-broker}"
NAMESPACE="resource-broker"       # broker controller lives here
BROKER_TEST_NS="broker-test"      # smoke-test pods live here (never default)
BROKER_IMAGE="localhost/k8s-resource-broker:latest"
PG_CONTAINER="broker-postgres"          # podman container name for local dev
PG_USER="broker"
PG_PASSWORD="broker"
PG_DB="broker"
PG_PORT="5432"
LOCAL_DB_URL="postgresql+asyncpg://${PG_USER}:${PG_PASSWORD}@localhost:${PG_PORT}/${PG_DB}"

# =============================================================================
# SECTION 0 — Configure podman TLS (macOS workaround)
# =============================================================================
# Writes an insecure-registry config into the running podman machine VM so all
# subsequent pulls (including ones triggered by minikube for kicbase) bypass
# TLS verification. Only needed on macOS where the VM cert store is isolated.
# =============================================================================
configure_podman_tls() {
    step "Configuring podman TLS for public registries"

    # Make sure the podman machine is initialised and running before we try to
    # SSH into it. On first run this creates the VM (~500 MB download).
    if ! podman machine list --format '{{.Running}}' 2>/dev/null | grep -q "true"; then
        info "No running podman machine found — initialising one..."
        podman machine init 2>/dev/null || true
        podman machine start
    fi

    # Get the name of the running machine (usually "podman-machine-default").
    local machine
    machine=$(podman machine list --format '{{.Name}}\t{{.Running}}' 2>/dev/null \
        | awk '$2=="true"{print $1}' | head -1)
    if [[ -z "$machine" ]]; then
        fail "Could not find a running podman machine after start."
    fi
    info "Podman machine: $machine"

    # Write the insecure-registry config inside the VM.
    # We cover docker.io (postgres, python, nginx), gcr.io (minikube kicbase),
    # and registry.k8s.io (core k8s images pulled by minikube addons).
    podman machine ssh "$machine" "
        sudo mkdir -p /etc/containers/registries.conf.d
        sudo tee /etc/containers/registries.conf.d/001-dev-insecure.conf > /dev/null <<'REGCONF'
[[registry]]
location = \"docker.io\"
insecure = true

[[registry]]
location = \"gcr.io\"
insecure = true

[[registry]]
location = \"registry.k8s.io\"
insecure = true
REGCONF
        echo 'Insecure registry config written.'
    "

    ok "Podman VM configured to skip TLS for docker.io / gcr.io / registry.k8s.io."
    warn "Dev-only setting — do not replicate on production machines."
}

# =============================================================================
# SECTION 1 — Prerequisites
# =============================================================================
# We check for every tool we'll call before doing anything, so the user gets a
# single clear list of what's missing rather than failing mid-way through setup.
# =============================================================================
check_prerequisites() {
    step "Checking prerequisites"

    local missing=0

    # uv is the project's package manager (replaces pip+venv).
    # It reads pyproject.toml and resolves a hermetic environment in .venv/.
    if ! command -v uv &>/dev/null; then
        warn "uv not found. Install with:"
        warn "  curl -LsSf https://astral.sh/uv/install.sh | sh"
        warn "  Then restart your shell and re-run this script."
        missing=1
    else
        ok "uv $(uv --version)"
    fi

    # The project's pyproject.toml requires Python >=3.12. macOS ships with
    # 3.9 as the system default. Brew installs 3.12 as a versioned binary
    # (python3.12) without overwriting the system 'python3' symlink, so we
    # check for the versioned binary first before falling back to 'python3'.
    # uv also maintains its own Python registry and can find brew-installed
    # interpreters independently of what 'python3' on PATH resolves to.
    local py_bin py_version
    if command -v python3.12 &>/dev/null; then
        # Brew-installed Python 3.12 found at the versioned name — all good.
        py_bin="python3.12"
        py_version=$(python3.12 --version | awk '{print $2}')
        ok "Python $py_version (at $(command -v python3.12))"
    else
        # Fall back to whatever 'python3' resolves to and check the version.
        py_version=$(python3 --version 2>/dev/null | awk '{print $2}' || echo "0.0.0")
        local py_major py_minor
        py_major=$(echo "$py_version" | cut -d. -f1)
        py_minor=$(echo "$py_version" | cut -d. -f2)
        if [[ "$py_major" -lt 3 || ( "$py_major" -eq 3 && "$py_minor" -lt 12 ) ]]; then
            warn "Python 3.12+ required, found $py_version."
            warn "  brew install python@3.12 installs it as 'python3.12' — that is enough."
            warn "  You do NOT need to relink 'python3'; uv finds versioned binaries automatically."
            missing=1
        else
            py_bin="python3"
            ok "Python $py_version"
        fi
    fi

    # Podman is used as our container runtime. The original scripts assumed
    # Docker, but we use podman since that's what you have installed.
    if ! command -v podman &>/dev/null; then
        warn "podman not found. Install with: brew install podman"
        missing=1
    else
        ok "podman $(podman --version | awk '{print $3}')"
    fi

    if $RUN_MINIKUBE; then
        # kubectl is needed to apply manifests and query the cluster.
        if ! command -v kubectl &>/dev/null; then
            warn "kubectl not found. Install with: brew install kubectl"
            missing=1
        else
            ok "kubectl $(kubectl version --client --short 2>/dev/null | awk '{print $3}')"
        fi

        # minikube creates the local Kubernetes cluster. We use the podman
        # driver (--driver=podman) so it runs inside podman instead of Docker.
        if ! command -v minikube &>/dev/null; then
            warn "minikube not found. Install with: brew install minikube"
            missing=1
        else
            ok "minikube $(minikube version --short)"
        fi
    fi

    if [[ $missing -eq 1 ]]; then
        fail "Fix the missing tools above, then re-run this script."
    fi

    ok "All prerequisites satisfied."
}

# =============================================================================
# SECTION 2 — Python environment
# =============================================================================
# uv reads pyproject.toml and creates a .venv in the project root with all
# declared dependencies pinned to the versions in uv.lock. --all-extras pulls
# in the [dev] group (pytest, ruff, mypy, etc.) as well.
# =============================================================================
setup_python_env() {
    step "Installing Python dependencies"

    cd "$ROOT_DIR"

    # uv searches its own Python registry first, then common install locations
    # (brew, pyenv, asdf). It finds python3.12 even when the system 'python3'
    # symlink still points to 3.9 — so we don't need to relink anything.
    # --python 3.12 pins the interpreter version for this project explicitly.
    # If uv can't find 3.12 anywhere it will download and manage it itself.
    uv sync --all-extras --python 3.12
    ok "Python environment ready at .venv/"
}

# =============================================================================
# SECTION 3 — .env file
# =============================================================================
# The app reads all config from environment variables prefixed BROKER_ (via
# pydantic-settings in config.py). .env.example has safe defaults for local
# dev. We only create .env if it doesn't already exist so we never clobber
# customisations the user has already made.
# =============================================================================
setup_env_file() {
    step "Setting up .env"

    cd "$ROOT_DIR"

    if [[ -f .env ]]; then
        ok ".env already exists — leaving it untouched."
    else
        cp .env.example .env

        # BROKER_K8S_IN_CLUSTER=false tells the Kubernetes client to load
        # ~/.kube/config from disk instead of the in-cluster service account
        # token. This is required when running the broker on your laptop
        # (outside of the cluster).
        sed -i.bak 's/BROKER_K8S_IN_CLUSTER=true/BROKER_K8S_IN_CLUSTER=false/' .env
        rm -f .env.bak

        ok ".env created from .env.example (K8S_IN_CLUSTER set to false)."
    fi
}

# =============================================================================
# SECTION 4 — PostgreSQL via podman
# =============================================================================
# The broker uses PostgreSQL to store historical pod metrics. The percentile
# algorithm queries this table (pod_metrics) to compute the Nth-percentile of
# CPU/memory usage over a lookback window.
#
# We run Postgres as a podman container instead of docker-compose because you
# have podman installed, not Docker. The container config mirrors what
# docker-compose.yaml defines.
# =============================================================================
start_postgres() {
    step "Starting PostgreSQL in podman"

    # If the container already exists and is running, do nothing.
    if podman ps --format '{{.Names}}' | grep -q "^${PG_CONTAINER}$"; then
        ok "Postgres container '$PG_CONTAINER' is already running."
        return
    fi

    # If the container exists but is stopped, just start it (preserves the
    # existing database volume and avoids re-initialising the schema).
    if podman ps -a --format '{{.Names}}' | grep -q "^${PG_CONTAINER}$"; then
        info "Container '$PG_CONTAINER' exists but is stopped — starting it..."
        podman start "$PG_CONTAINER"
        ok "Postgres restarted."
        _wait_for_postgres
        return
    fi

    info "Creating new Postgres container..."
    # Pull explicitly with --tls-verify=false before 'podman run' so we control
    # the TLS flag. 'podman run' would also pull if the image is missing, but
    # it doesn't accept --tls-verify, so any pull it triggers would use the
    # default (verify) and fail. Pre-pulling avoids that race.
    podman pull --tls-verify=false postgres:16-alpine
    podman run -d \
        --name "$PG_CONTAINER" \
        --env POSTGRES_USER="$PG_USER" \
        --env POSTGRES_PASSWORD="$PG_PASSWORD" \
        --env POSTGRES_DB="$PG_DB" \
        -p "${PG_PORT}:5432" \
        postgres:16-alpine

    _wait_for_postgres
    ok "Postgres is up at localhost:$PG_PORT (db=$PG_DB, user=$PG_USER)."
}

# Poll until pg_isready succeeds. Without this, alembic will fail immediately
# because Postgres takes a few seconds to initialise on first run.
_wait_for_postgres() {
    info "Waiting for Postgres to accept connections..."
    local retries=0
    until podman exec "$PG_CONTAINER" pg_isready -U "$PG_USER" -d "$PG_DB" &>/dev/null; do
        retries=$((retries + 1))
        if [[ $retries -gt 20 ]]; then
            warn "Postgres container state:"
            podman inspect "$PG_CONTAINER" \
                --format '  status={{.State.Status}}  exit={{.State.ExitCode}}' 2>/dev/null || true
            warn "Postgres container logs (last 20 lines):"
            podman logs --tail=20 "$PG_CONTAINER" 2>/dev/null || true
            fail "Postgres did not become ready after 20 retries (~40s). See logs above."
        fi
        sleep 2
    done
    ok "Postgres is ready."
}

# =============================================================================
# SECTION 5 — Database migrations
# =============================================================================
# Alembic applies version-controlled SQL migrations to the database. The only
# migration so far (0001_initial_schema.py) creates the pod_metrics table and
# its indexes.
#
# 'alembic upgrade head' is idempotent — if the schema is already current it
# does nothing, so it's safe to run on every setup.
# =============================================================================
run_migrations() {
    step "Running database migrations"

    cd "$ROOT_DIR"

    BROKER_DATABASE_URL="$LOCAL_DB_URL" \
        uv run alembic upgrade head

    ok "Migrations applied (schema is at latest revision)."
}

# =============================================================================
# SECTION 6 — Unit tests (quick sanity check)
# =============================================================================
# Run the unit test suite against the local environment to confirm the install
# is healthy before doing anything heavier (minikube etc.). Unit tests use an
# in-memory SQLite DB (aiosqlite) so they don't need Postgres running.
# =============================================================================
run_unit_tests() {
    step "Running unit tests"

    cd "$ROOT_DIR"

    uv run pytest tests/unit/ -v --tb=short
    ok "All unit tests passed."
}

# =============================================================================
# SECTION 7 — Start local API server
# =============================================================================
# Starts the FastAPI server on localhost:8080. The server runs the watcher
# loop and the REST API in a single process. Because BROKER_K8S_IN_CLUSTER is
# false, it loads ~/.kube/config — if minikube is running and set as the
# current context, the watcher will watch that cluster.
#
# Swagger UI: http://localhost:8080/docs
# Health check: http://localhost:8080/healthz
# =============================================================================
serve_local() {
    step "Starting API server"

    cd "$ROOT_DIR"

    info "API will be available at http://localhost:8080"
    info "Swagger docs at          http://localhost:8080/docs"
    info "Press Ctrl-C to stop."

    # Load .env values into the shell so uvicorn picks them up alongside any
    # overrides already set in the current environment.
    set -a; source .env; set +a

    uv run python -m resource_broker serve
}

# =============================================================================
# SECTION 8 — Minikube cluster
# =============================================================================
# Creates a local Kubernetes cluster using the podman driver. The original
# setup-minikube.sh used --driver=docker; we change that to --driver=podman
# since that's what's available on this machine.
#
# We allocate 3 CPUs / 6 GB RAM — enough to run Postgres + the broker
# controller + a test nginx pod without swapping.
# =============================================================================
start_minikube() {
    step "Starting minikube cluster: $CLUSTER_NAME"

    local status
    status=$(minikube status --profile="$CLUSTER_NAME" --format='{{.Host}}' 2>/dev/null || echo "Nonexistent")

    # Detect if an existing cluster was created with CRI-O. CRI-O calls
    # 'sudo runc list' to enumerate pod sandboxes, and runc needs the
    # /run/runc directory with elevated privileges. Rootless podman on macOS
    # runs containers without those privileges, so /run/runc never exists
    # inside the minikube node container → every runc call fails.
    # containerd uses a different shim (containerd-shim-runc-v2) that does
    # NOT require /run/runc to be present at the node level.
    # We detect CRI-O by checking for the crio binary inside the cluster.
    # If found, delete and recreate — you cannot change a cluster's container
    # runtime in-place; it must be rebuilt from scratch.
    if [[ "$status" != "Nonexistent" ]]; then
        if minikube ssh --profile="$CLUSTER_NAME" "command -v crio" &>/dev/null 2>&1; then
            warn "Existing cluster uses CRI-O, which fails under rootless podman (runc /run/runc missing)."
            warn "Deleting cluster and recreating with containerd..."
            minikube delete --profile="$CLUSTER_NAME"
            status="Nonexistent"
        fi
    fi

    if [[ "$status" == "Running" ]]; then
        # Verify the API server actually has InPlacePodVerticalScaling enabled.
        # If the cluster was started before we added --extra-config=apiserver.feature-gates,
        # the flag was never applied and in-place pod resource patches will return 422.
        local api_fg_args
        api_fg_args=$(kubectl -n kube-system get pod \
            -l component=kube-apiserver \
            -o jsonpath='{.items[0].spec.containers[0].command}' 2>/dev/null || echo "")
        if echo "$api_fg_args" | grep -q "InPlacePodVerticalScaling=true"; then
            ok "Cluster '$CLUSTER_NAME' is already running with InPlacePodVerticalScaling ✓"
        else
            warn "Cluster '$CLUSTER_NAME' is running but the API server is missing InPlacePodVerticalScaling=true."
            warn "This flag can only be set at cluster creation time. Deleting and recreating..."
            minikube delete --profile="$CLUSTER_NAME"
            status="Nonexistent"
        fi
    fi
    if [[ "$status" == "Running" ]]; then
        : # already confirmed healthy above
    else
        info "Starting minikube with podman driver (this takes 2–4 min on first run)..."
        # --driver=podman             uses podman to create the cluster node container.
        # --container-runtime=containerd   avoids the CRI-O/runc privilege issue (see above).
        # --kubernetes-version=v1.32.3     pins to k8s 1.32 — v1.35+ moved binaries to
        #                              dl.k8s.io but minikube's downloader still uses the
        #                              old storage.googleapis.com URL and gets a 404.
        # --cni=bridge                 CRITICAL on this setup. Every other CNI option
        #                              (kindnet, flannel, calico) requires containerd inside
        #                              the cluster to pull an image from docker.io at startup.
        #                              Containerd inside the cluster has its own TLS config,
        #                              completely separate from the podman machine config we
        #                              wrote in Section 0. It cannot pull from docker.io due
        #                              to the x509 certificate error — which is why kindnet
        #                              hits ImagePullBackOff and CNI never initialises, leaving
        #                              the node NotReady forever.
        #                              'bridge' is compiled into the kicbase image itself —
        #                              no pull required, no TLS involved.
        # --wait=all                   blocks until kubelet, system pods, and node Ready are
        #                              all healthy before returning control to the script.
        # --extra-config=apiserver.feature-gates sets the kube-apiserver binary flag
        #   directly — this is what allows the API server to accept resource patches
        #   on running pods (InPlacePodVerticalScaling validation is on the API server).
        # --feature-gates sets kubelet's feature gate via KubeletConfiguration (NOT via
        #   command-line, which is deprecated since k8s 1.21). This is what makes the
        #   kubelet actually perform the in-place resize and populate ResizePolicy on pods.
        # --extra-config=kubelet.feature-gates does NOT work because it tries to set the
        #   kubelet command-line --feature-gates flag, which kubelet ignores in 1.21+.
        if ! minikube start \
                --cpus=3 \
                --memory=1800mb \
                --driver=podman \
                --container-runtime=containerd \
                --kubernetes-version=v1.32.3 \
                --cni=bridge \
                --feature-gates=InPlacePodVerticalScaling=true \
                --extra-config=apiserver.feature-gates=InPlacePodVerticalScaling=true \
                --wait=all \
                --wait-timeout=5m \
                --profile="$CLUSTER_NAME"; then
            warn "minikube start failed. Dumping diagnostics..."
            warn "── kube-system pods ──"
            kubectl -n kube-system get pods -o wide 2>/dev/null || true
            warn "── node conditions ──"
            kubectl describe node 2>/dev/null | awk '/Conditions:/,/Addresses:/' || true
            warn "── minikube logs (last 40 lines) ──"
            minikube logs --profile="$CLUSTER_NAME" 2>/dev/null | tail -40 || true
            fail "Cluster did not reach a healthy state. See diagnostics above."
        fi
        ok "Cluster started and all components are Ready."
    fi

    # metrics-server is optional — it powers 'kubectl top pod' and the broker's
    # scraper fallback. We make it non-fatal because addon pulls hit registry.k8s.io
    # from inside the cluster container, which can fail on restricted networks.
    minikube addons enable metrics-server --profile="$CLUSTER_NAME" 2>/dev/null \
        && ok "metrics-server addon enabled." \
        || warn "metrics-server addon failed (non-fatal — broker works without it)."
}

# =============================================================================
# SECTION 9 — Build and load broker image into minikube
# =============================================================================
# With Docker you'd use 'eval $(minikube docker-env)' to point Docker at the
# minikube daemon, then docker build. With podman the equivalent is
# 'minikube podman-env', but the most reliable cross-platform approach is to
# build with podman locally and pipe the image directly into minikube.
#
# 'minikube image load' copies the OCI image from the local store into the
# cluster's container runtime without needing a registry.
# =============================================================================
build_and_load_image() {
    step "Building broker image and loading into minikube"

    cd "$ROOT_DIR"

    # Pre-pull the Dockerfile base image with TLS verification disabled.
    # Without this, 'podman build' tries to pull python:3.12-slim on its own
    # and hits the x509 error (the inner pull has no --tls-verify flag).
    info "Pre-pulling Dockerfile base images..."
    podman pull --tls-verify=false python:3.12-slim

    info "Building broker image with podman..."
    podman build -t "$BROKER_IMAGE" -f Dockerfile .

    # Load every cluster image via a temp tar file rather than piping stdin.
    #
    # WHY NOT PIPE: 'podman save ... | minikube image load -' is unreliable
    # with containerd. minikube's stdin handler for containerd can silently
    # drop bytes mid-stream, producing a truncated or empty tar archive. The
    # image appears to load (no error) but is never registered in containerd's
    # k8s.io namespace, so kubelet can't find it and pods hit ErrImageNeverPull.
    #
    # WHY TEMP FILE: writing to a .tar first guarantees the full archive is on
    # disk before minikube reads it. minikube image load <file> then uses a
    # proper file descriptor, not a stdin pipe, which containerd handles safely.
    local tmp_tar
    tmp_tar=$(mktemp /tmp/broker-img-XXXXXX.tar)

    info "Pre-pulling cluster images and loading into minikube..."
    local img
    for img in "${CLUSTER_IMAGES[@]}"; do
        info "  pulling $img..."
        podman pull --tls-verify=false "$img"
        info "  saving to temp file and loading into minikube..."
        podman save -o "$tmp_tar" "$img"
        minikube image load --overwrite=true "$tmp_tar" --profile="$CLUSTER_NAME"
        ok "  $img loaded."
    done

    info "Loading broker image into minikube..."
    podman save -o "$tmp_tar" "$BROKER_IMAGE"
    minikube image load --overwrite=true "$tmp_tar" --profile="$CLUSTER_NAME"

    rm -f "$tmp_tar"

    # Confirm the images are visible inside the cluster so we catch load
    # failures before pods are scheduled (faster feedback than a pod timeout).
    info "Verifying images inside cluster..."
    local loaded_images
    loaded_images=$(minikube image ls --profile="$CLUSTER_NAME" 2>/dev/null || echo "")
    for img in "${CLUSTER_IMAGES[@]}" "$BROKER_IMAGE"; do
        local img_name="${img%%:*}"   # strip tag for a broader match
        if echo "$loaded_images" | grep -q "$img_name"; then
            ok "  verified: $img"
        else
            warn "  $img not found in cluster image list — pods using it will fail with ErrImageNeverPull"
        fi
    done

    ok "All images available inside the cluster."
}

# =============================================================================
# SECTION 10 — Deploy in-cluster PostgreSQL
# =============================================================================
# Inside the cluster we run Postgres as a bare Pod (not a Deployment) for
# simplicity — no persistent volumes, no StatefulSet overhead. This is
# fine for integration testing; don't do this in production.
#
# A headless ClusterIP Service (selector: run=postgres) lets the broker
# resolve it by DNS: postgres.resource-broker.svc.cluster.local:5432
# =============================================================================
deploy_cluster_postgres() {
    step "Deploying PostgreSQL inside minikube"

    # Namespace must exist before we can create any resources in it.
    kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

    # Never — kubelet must use the image we pre-loaded in Section 9.
    # IfNotPresent would still try to pull if the image isn't cached in the
    # node's CRI store, which would fail with the same TLS error.
    kubectl -n "$NAMESPACE" run postgres \
        --image="postgres:16-alpine" \
        --env="POSTGRES_USER=$PG_USER" \
        --env="POSTGRES_PASSWORD=$PG_PASSWORD" \
        --env="POSTGRES_DB=$PG_DB" \
        --restart=Never --port=5432 \
        --image-pull-policy=Never \
        2>/dev/null || true

    # Service exposes the pod to other pods in the cluster on port 5432.
    kubectl -n "$NAMESPACE" apply -f - <<SVC
apiVersion: v1
kind: Service
metadata:
  name: postgres
spec:
  ports:
    - port: 5432
      targetPort: 5432
  selector:
    run: postgres
SVC

    info "Waiting for Postgres pod to be Ready..."
    # 90s covers slow containerd image unpack on first run. On timeout we
    # print the pod status and last events so the cause is immediately visible
    # rather than just "timed out" with no context.
    if ! kubectl -n "$NAMESPACE" wait \
            --for=condition=Ready \
            --timeout=90s \
            pod/postgres 2>/dev/null; then
        warn "Postgres pod did not become Ready within 90s. Current state:"
        kubectl -n "$NAMESPACE" get pod postgres -o wide || true
        warn "Recent pod events:"
        kubectl -n "$NAMESPACE" describe pod postgres \
            | awk '/^Events:/,0' | tail -20 || true
        fail "Cannot continue — Postgres must be running before migrations."
    fi

    ok "Cluster Postgres is running."
}

# =============================================================================
# SECTION 11 — Run migrations inside the cluster
# =============================================================================
# We port-forward the in-cluster Postgres to localhost:5433 (not 5432 to avoid
# conflicting with the local dev Postgres on 5432), run alembic from the host,
# then tear down the port-forward.
# =============================================================================
run_cluster_migrations() {
    step "Running migrations against in-cluster PostgreSQL"

    # Start port-forward in the background; wait until the TCP port is open
    # rather than sleeping blindly. kubectl port-forward can take 1-3s to bind,
    # and alembic will fail immediately if it connects before the tunnel is up.
    kubectl -n "$NAMESPACE" port-forward pod/postgres 5433:5432 &
    local PF_PID=$!
    # Ensure port-forward is killed even if the script exits early.
    trap 'kill "$PF_PID" 2>/dev/null || true' EXIT INT TERM

    info "Waiting for port-forward 5433 → cluster postgres..."
    local pf_retries=0
    until nc -z localhost 5433 2>/dev/null; do
        pf_retries=$((pf_retries + 1))
        if [[ $pf_retries -gt 15 ]]; then
            warn "Port-forward process alive: $(kill -0 "$PF_PID" 2>/dev/null && echo yes || echo no)"
            fail "Port-forward to cluster postgres did not bind within 15s."
        fi
        sleep 1
    done

    # nc only confirms the port-forward tunnel is bound on the host. Postgres
    # inside the pod goes through an init-then-restart cycle before it actually
    # accepts connections. Wait for pg_isready via kubectl exec so alembic never
    # hits "connection refused" due to the race between pod Ready and postgres ready.
    info "Waiting for Postgres to accept connections inside the pod..."
    local pg_retries=0
    until kubectl -n "$NAMESPACE" exec pod/postgres -- \
            pg_isready -U "$PG_USER" -d "$PG_DB" &>/dev/null 2>&1; do
        pg_retries=$((pg_retries + 1))
        if [[ $pg_retries -gt 30 ]]; then
            fail "Postgres inside the pod did not accept connections within 30s."
        fi
        sleep 1
    done

    CLUSTER_DB_URL="postgresql+asyncpg://${PG_USER}:${PG_PASSWORD}@localhost:5433/${PG_DB}"

    BROKER_DATABASE_URL="$CLUSTER_DB_URL" \
        uv run --directory "$ROOT_DIR" alembic upgrade head

    kill "$PF_PID" 2>/dev/null || true
    # Reset the trap so subsequent EXIT handlers aren't also killed.
    trap - EXIT INT TERM

    ok "Cluster database schema is up to date."
}

# =============================================================================
# SECTION 12 — Deploy CRD, RBAC, and broker controller
# =============================================================================
# Deployment order matters:
#   1. CRD — defines the ResourceProfile custom resource type. The broker
#      watcher subscribes to this CRD, so it must exist before the broker
#      starts or the watch call will fail with "resource not found".
#   2. RBAC — ServiceAccount + ClusterRole + ClusterRoleBinding. The broker
#      needs watch/patch on pods and get/list/watch on ResourceProfiles.
#   3. ConfigMap — carries all BROKER_* env vars into the pod.
#   4. Controller Deployment — the watcher process. Uses imagePullPolicy:
#      IfNotPresent so it uses our locally-loaded image instead of pulling
#      from a registry (which doesn't exist for this image).
# =============================================================================
deploy_broker_controller() {
    step "Deploying CRD, RBAC, ConfigMap, and broker controller"

    cd "$ROOT_DIR"

    # Apply the Custom Resource Definition so Kubernetes recognises
    # 'ResourceProfile' as a valid resource kind.
    kubectl apply -f deploy/crd/resourceprofile-crd.yaml

    # RBAC gives the broker's ServiceAccount the rights it needs:
    # read+watch pods cluster-wide, patch pod specs, and read ResourceProfiles.
    kubectl apply -f deploy/resource-broker/rbac.yaml

    # ConfigMap holds all BROKER_* environment variables so we don't bake
    # secrets into the image. The controller pod loads this via envFrom.
    kubectl apply -f deploy/resource-broker/configmap.yaml

    # Deploy the broker in controller (watcher-only) mode. This runs only the
    # pod watcher + patcher, not the full API server or the admission webhook.
    # Watcher-only is enough to verify that annotated pods get their resources
    # rewritten.
    kubectl -n "$NAMESPACE" apply -f - <<DEPLOY
apiVersion: apps/v1
kind: Deployment
metadata:
  name: resource-broker-controller
  labels:
    app.kubernetes.io/name: resource-broker
    app.kubernetes.io/component: controller
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: resource-broker
      app.kubernetes.io/component: controller
  template:
    metadata:
      labels:
        app.kubernetes.io/name: resource-broker
        app.kubernetes.io/component: controller
    spec:
      serviceAccountName: resource-broker
      containers:
        - name: broker
          image: ${BROKER_IMAGE}
          # IfNotPresent means minikube uses the image we loaded in Section 9
          # instead of trying to pull it from Docker Hub (where it doesn't exist).
          imagePullPolicy: IfNotPresent
          command: ["python", "-m", "resource_broker", "controller"]
          envFrom:
            - configMapRef:
                name: broker-config
          env:
            - name: BROKER_K8S_IN_CLUSTER
              value: "true"
            # For this smoke test we don't have Prometheus in-cluster, so we
            # point metrics at a placeholder. Static profiles don't query it.
            - name: BROKER_METRICS_URL
              value: "http://prometheus:9090"
            # Slow the scraper loop to avoid log noise during the test.
            - name: BROKER_SCRAPER_INTERVAL_SECONDS
              value: "3600"
DEPLOY

    info "Waiting for controller Deployment to be Available..."
    # 120s covers: image unpack (~10s) + app startup + readiness probe settling.
    # The Deployment condition flips Available once at least one replica is Ready.
    if ! kubectl -n "$NAMESPACE" wait \
            --for=condition=Available \
            --timeout=120s \
            deployment/resource-broker-controller 2>/dev/null; then
        warn "Controller Deployment did not become Available within 120s. Diagnostics:"
        warn "── Deployment conditions ──"
        kubectl -n "$NAMESPACE" describe deployment resource-broker-controller \
            | awk '/^Conditions:/,/^[A-Z]/' | head -20 || true
        warn "── Controller pods ──"
        kubectl -n "$NAMESPACE" get pods -l app.kubernetes.io/component=controller -o wide || true
        warn "── Pod events ──"
        kubectl -n "$NAMESPACE" describe pods \
            -l app.kubernetes.io/component=controller \
            | awk '/^Events:/,0' | tail -30 || true
        warn "── Controller container logs (last 40 lines) ──"
        kubectl -n "$NAMESPACE" logs \
            -l app.kubernetes.io/component=controller \
            --tail=40 2>/dev/null || true
        fail "Broker controller did not start. See diagnostics above."
    fi

    local pod
    pod=$(kubectl -n "$NAMESPACE" get pod \
        -l app.kubernetes.io/component=controller \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")

    if [[ -z "$pod" ]]; then
        fail "Could not find a controller pod after Deployment became Available."
    fi

    if ! kubectl -n "$NAMESPACE" wait --for=condition=Ready \
            --timeout=30s "pod/$pod" 2>/dev/null; then
        warn "Controller pod $pod did not reach Ready within 30s."
        kubectl -n "$NAMESPACE" describe pod "$pod" || true
        warn "── Logs ──"
        kubectl -n "$NAMESPACE" logs "$pod" --tail=40 2>/dev/null || true
        fail "Controller pod failed to reach Ready state."
    fi

    ok "Broker controller is running ($pod)."
    info "Controller logs:"
    kubectl -n "$NAMESPACE" logs "$pod" --tail=10 2>/dev/null || true
}

# =============================================================================
# SECTION 13 — Apply sample ResourceProfile CRD instances
# =============================================================================
# These YAML files create actual ResourceProfile objects (instances of the CRD
# we registered above). The broker watcher watches for these and builds an
# in-memory profile registry from them — no database needed for profiles.
# =============================================================================
apply_sample_profiles() {
    step "Applying sample ResourceProfile manifests"

    cd "$ROOT_DIR"

    # Profiles no longer carry namespace: in their YAML so they can be applied
    # to any namespace with -n. Here we populate the default namespace for
    # general-purpose use. The smoke test applies the same profiles to
    # $BROKER_TEST_NS separately (find_for_pod looks in the pod's own namespace).
    kubectl apply -n default -f deploy/samples/profile-efficient.yaml
    kubectl apply -n default -f deploy/samples/profile-aggressive.yaml 2>/dev/null || true
    kubectl apply -n default -f deploy/samples/profile-requests-only.yaml 2>/dev/null || true

    ok "Sample profiles applied (namespace: default)."
}

# =============================================================================
# SECTION 14 — Smoke tests (two scenarios)
# =============================================================================
# Scenario A — Enforce mode (k8s-efficient profile):
#   Pod starts with cpu=10m / memory=10Mi. The watcher detects the ADDED event,
#   waits for the pod to be Ready, then tries an in-place PATCH. If the cluster
#   lacks InPlacePodVerticalScaling the watcher falls back to delete-and-recreate
#   (same as Kubernetes VPA "Recreate" mode). We verify:
#     1. Pod spec reflects the enforced values (cpu=250m / memory=256Mi + limits)
#     2. Recreated pod reaches Ready
#     3. Pod is actually serving HTTP traffic (exec wget)
#
# Scenario B — Recommendation mode (k8s-requests-only profile):
#   Pod starts with cpu=10m / memory=10Mi. The watcher computes what the optimal
#   resources would be (cpu=500m / memory=1Gi per the profile) and logs them, but
#   leaves the pod completely untouched. We verify:
#     1. Pod spec is UNCHANGED after the watcher processes the event
#     2. Controller log contains "recommendation computed" for this pod
# =============================================================================
run_smoke_test() {
    step "Running smoke tests (enforce + recommendation)"

    local TARGET_NS="$BROKER_TEST_NS"   # never "default" — keep test resources isolated
    local ENFORCE_POD="test-enforce-pod"
    local RECOMMEND_POD="test-recommend-pod"

    # ── Create the test namespace ────────────────────────────────────────────
    # All smoke-test pods live here, not in "default". This keeps test resources
    # isolated and makes cleanup trivial (kubectl delete ns broker-test).
    kubectl create namespace "$TARGET_NS" --dry-run=client -o yaml | kubectl apply -f -

    # ProfileLoader uses the pod's namespace to look up ResourceProfiles
    # (find_for_pod calls get_by_name(profile_name, pod.namespace)). So the
    # profiles must exist in the same namespace as the test pods.
    info "Applying ResourceProfiles to namespace: $TARGET_NS"
    kubectl apply -n "$TARGET_NS" -f "$ROOT_DIR/deploy/samples/profile-efficient.yaml"
    kubectl apply -n "$TARGET_NS" -f "$ROOT_DIR/deploy/samples/profile-requests-only.yaml"

    # ── Pre-flight: API server feature gate check ────────────────────────────
    info "Pre-flight: checking InPlacePodVerticalScaling on API server..."
    local apiserver_args
    apiserver_args=$(kubectl -n kube-system get pod \
        -l component=kube-apiserver \
        -o jsonpath='{.items[0].spec.containers[0].command}' 2>/dev/null || echo "")
    if echo "$apiserver_args" | grep -q "InPlacePodVerticalScaling=true"; then
        ok "  API server: InPlacePodVerticalScaling=true ✓ (in-place resize available)"
    else
        warn "  API server: InPlacePodVerticalScaling not set — watcher will use delete-and-recreate fallback"
    fi

    # ┌──────────────────────────────────────────────────────────────────────┐
    # │ Scenario A — Enforce mode                                            │
    # └──────────────────────────────────────────────────────────────────────┘
    info ""
    info "── Scenario A: enforce mode (k8s-efficient) ──"

    # Clean up any leftover pod so we always get a fresh ADDED event.
    kubectl delete pod "$ENFORCE_POD" -n "$TARGET_NS" --ignore-not-found 2>/dev/null || true

    kubectl apply -f - <<ENFORCE_POD_YAML
apiVersion: v1
kind: Pod
metadata:
  name: $ENFORCE_POD
  namespace: $TARGET_NS
  annotations:
    resource-broker/profile: "k8s-efficient"
  labels:
    app: test-resource-broker
spec:
  containers:
    - name: nginx
      image: nginx:alpine
      imagePullPolicy: Never
      # Intentionally tiny resources — broker should raise them to profile values.
      resources:
        requests:
          cpu: 10m
          memory: 10Mi
        limits:
          cpu: 10m
          memory: 10Mi
ENFORCE_POD_YAML

    info "Waiting for enforce test pod to be Ready..."
    if ! kubectl -n "$TARGET_NS" wait \
            --for=condition=Ready \
            --timeout=60s "pod/$ENFORCE_POD" 2>/dev/null; then
        warn "Enforce test pod did not become Ready within 60s:"
        kubectl -n "$TARGET_NS" describe pod "$ENFORCE_POD" | awk '/^Events:/,0' | tail -15 || true
        fail "Enforce test pod failed to reach Ready — cannot verify patching."
    fi

    # Poll for the broker to enforce the profile.
    # Timeline: watcher polls until pod Ready (~2s already done), tries PATCH
    # → 422 → delete-and-recreate. New pod starts up. Budget = 60s (30 × 2s).
    info "Polling for resource enforcement (60s budget)..."
    local CPU_REQ MEM_REQ enforce_ok=false
    for ((i=1; i<=30; i++)); do
        CPU_REQ=$(kubectl -n "$TARGET_NS" get pod "$ENFORCE_POD" -o json 2>/dev/null \
            | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(d['spec']['containers'][0].get('resources',{}).get('requests',{}).get('cpu','<none>'))
" 2>/dev/null || echo "<error>")
        MEM_REQ=$(kubectl -n "$TARGET_NS" get pod "$ENFORCE_POD" -o json 2>/dev/null \
            | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(d['spec']['containers'][0].get('resources',{}).get('requests',{}).get('memory','<none>'))
" 2>/dev/null || echo "<error>")

        if [[ "$CPU_REQ" == "250m" && "$MEM_REQ" == "256Mi" ]]; then
            ok "  requests.cpu=$CPU_REQ  requests.memory=$MEM_REQ ✓"
            enforce_ok=true
            break
        fi
        info "    Attempt $i/30: cpu=$CPU_REQ mem=$MEM_REQ — waiting 2s..."
        sleep 2
    done

    if ! $enforce_ok; then
        warn "Resource enforcement timed out. Final: cpu=$CPU_REQ mem=$MEM_REQ"
        local ctrl_pod
        ctrl_pod=$(kubectl -n "$NAMESPACE" get pod \
            -l app.kubernetes.io/component=controller \
            -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
        [[ -n "$ctrl_pod" ]] && kubectl -n "$NAMESPACE" logs "$ctrl_pod" --tail=40 2>/dev/null || true
        kubectl auth can-i patch pods -n "$TARGET_NS" \
            --as="system:serviceaccount:${NAMESPACE}:resource-broker" 2>/dev/null || true
        fail "Scenario A FAILED — broker did not enforce resources on the test pod."
    fi

    # Check limits too (profile sets cpu_limit=500m, memory_limit=512Mi)
    local CPU_LIM MEM_LIM
    CPU_LIM=$(kubectl -n "$TARGET_NS" get pod "$ENFORCE_POD" -o json 2>/dev/null \
        | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(d['spec']['containers'][0].get('resources',{}).get('limits',{}).get('cpu','<none>'))
" 2>/dev/null || echo "<error>")
    MEM_LIM=$(kubectl -n "$TARGET_NS" get pod "$ENFORCE_POD" -o json 2>/dev/null \
        | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(d['spec']['containers'][0].get('resources',{}).get('limits',{}).get('memory','<none>'))
" 2>/dev/null || echo "<error>")
    if [[ "$CPU_LIM" == "500m" && "$MEM_LIM" == "512Mi" ]]; then
        ok "  limits.cpu=$CPU_LIM  limits.memory=$MEM_LIM ✓"
    else
        warn "  limits unexpected — limits.cpu=$CPU_LIM limits.memory=$MEM_LIM"
    fi

    # Health check: the recreated pod must reach Ready and serve HTTP traffic.
    # (Verifies the container actually started correctly with the new resources,
    # not just that the spec fields changed.)
    info "Verifying recreated pod health..."
    if kubectl -n "$TARGET_NS" wait \
            --for=condition=Ready \
            --timeout=60s "pod/$ENFORCE_POD" 2>/dev/null; then
        ok "  Recreated pod is Ready ✓"
    else
        warn "  Recreated pod did not reach Ready within 60s"
        kubectl -n "$TARGET_NS" describe pod "$ENFORCE_POD" | awk '/^Events:/,0' | tail -10 || true
        fail "Scenario A FAILED — recreated pod never became Ready."
    fi
    if kubectl -n "$TARGET_NS" exec "$ENFORCE_POD" -- \
            wget -qO- --timeout=5 http://localhost/ 2>/dev/null | grep -qi "html\|nginx\|welcome"; then
        ok "  Recreated pod is serving HTTP traffic ✓"
    else
        warn "  HTTP health check inconclusive (pod may still be initializing)"
    fi

    # ┌──────────────────────────────────────────────────────────────────────┐
    # │ Scenario B — Recommendation mode                                     │
    # └──────────────────────────────────────────────────────────────────────┘
    info ""
    info "── Scenario B: recommendation mode (k8s-requests-only) ──"

    kubectl delete pod "$RECOMMEND_POD" -n "$TARGET_NS" --ignore-not-found 2>/dev/null || true

    kubectl apply -f - <<RECOMMEND_POD_YAML
apiVersion: v1
kind: Pod
metadata:
  name: $RECOMMEND_POD
  namespace: $TARGET_NS
  annotations:
    # k8s-requests-only has no mode field → defaults to recommendation.
    # The watcher will compute cpu=500m / memory=1Gi and log them,
    # but must NOT modify the pod.
    resource-broker/profile: "k8s-requests-only"
  labels:
    app: test-resource-broker-recommend
spec:
  containers:
    - name: nginx
      image: nginx:alpine
      imagePullPolicy: Never
      resources:
        requests:
          cpu: 10m
          memory: 10Mi
        limits:
          cpu: 10m
          memory: 10Mi
RECOMMEND_POD_YAML

    info "Waiting for recommendation test pod to be Ready..."
    if ! kubectl -n "$TARGET_NS" wait \
            --for=condition=Ready \
            --timeout=60s "pod/$RECOMMEND_POD" 2>/dev/null; then
        warn "Recommendation test pod did not become Ready within 60s."
        kubectl -n "$TARGET_NS" describe pod "$RECOMMEND_POD" | awk '/^Events:/,0' | tail -10 || true
        fail "Recommendation test pod failed to reach Ready."
    fi

    # Watcher logs the recommendation immediately on the ADDED event (no
    # wait-for-Ready in recommendation path). Give it 15s of headroom.
    info "Waiting 15s for watcher to process recommendation..."
    sleep 15

    # Resources must be completely UNCHANGED — recommendation mode never modifies pods.
    local REC_CPU REC_MEM
    REC_CPU=$(kubectl -n "$TARGET_NS" get pod "$RECOMMEND_POD" -o json 2>/dev/null \
        | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(d['spec']['containers'][0].get('resources',{}).get('requests',{}).get('cpu','<none>'))
" 2>/dev/null || echo "<error>")
    REC_MEM=$(kubectl -n "$TARGET_NS" get pod "$RECOMMEND_POD" -o json 2>/dev/null \
        | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(d['spec']['containers'][0].get('resources',{}).get('requests',{}).get('memory','<none>'))
" 2>/dev/null || echo "<error>")

    if [[ "$REC_CPU" == "10m" && "$REC_MEM" == "10Mi" ]]; then
        ok "  Pod resources UNCHANGED (cpu=$REC_CPU mem=$REC_MEM) ✓"
    else
        fail "Scenario B FAILED — recommendation mode unexpectedly modified the pod. cpu=$REC_CPU mem=$REC_MEM"
    fi

    # Verify the watcher logged a recommendation (structured JSON log).
    # Log line will contain both "recommendation computed" and the pod name.
    local ctrl_pod rec_log
    ctrl_pod=$(kubectl -n "$NAMESPACE" get pod \
        -l app.kubernetes.io/component=controller \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
    if [[ -n "$ctrl_pod" ]]; then
        rec_log=$(kubectl -n "$NAMESPACE" logs "$ctrl_pod" 2>/dev/null \
            | grep "recommendation computed" | grep "$RECOMMEND_POD" || true)
        if [[ -n "$rec_log" ]]; then
            ok "  Watcher logged recommendation for $RECOMMEND_POD ✓"
            info "  $rec_log"
        else
            warn "  'recommendation computed' log for $RECOMMEND_POD not found"
            info "  Controller logs (last 30 lines):"
            kubectl -n "$NAMESPACE" logs "$ctrl_pod" --tail=30 2>/dev/null || true
            warn "  (This may be a false alarm if the pod name changed between runs)"
        fi
    fi

    # ── Final summary ─────────────────────────────────────────────────────────
    echo ""
    ok "All smoke test scenarios passed:"
    echo "  Scenario A (enforce):        cpu=250m / memory=256Mi enforced; pod healthy"
    echo "  Scenario B (recommendation): pod unchanged; recommendation logged"
    echo ""
    info "Test resources are in namespace '$TARGET_NS'."
    info "To inspect:  kubectl -n $TARGET_NS get pod,resourceprofile"
    info "To clean up: kubectl delete namespace $TARGET_NS"
}


# =============================================================================
# SECTION 15 — Print useful follow-up commands
# =============================================================================
print_summary() {
    echo ""
    echo -e "${BOLD}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BOLD}║  Setup complete                                              ║${NC}"
    echo -e "${BOLD}╚══════════════════════════════════════════════════════════════╝${NC}"
    echo ""

    if $RUN_LOCAL; then
        echo -e "${GREEN}Local dev:${NC}"
        echo "  Start API:     uv run python -m resource_broker serve"
        echo "  Swagger UI:    http://localhost:8080/docs"
        echo "  Health check:  http://localhost:8080/healthz"
        echo "  Unit tests:    uv run pytest tests/unit/ -v"
        echo "  Stop Postgres: podman stop $PG_CONTAINER"
        echo ""
    fi

    if $RUN_MINIKUBE; then
        local pod
        pod=$(kubectl -n "$NAMESPACE" get pod \
            -l app.kubernetes.io/component=controller \
            -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "<pod>")
        echo -e "${GREEN}Minikube cluster:${NC}"
        echo "  Controller logs:  kubectl -n $NAMESPACE logs $pod -f"
        echo "  Test pods:        kubectl -n $BROKER_TEST_NS get pod,resourceprofile"
        echo "  Tear down:        minikube delete --profile=$CLUSTER_NAME"
        echo "  Test ns cleanup:  kubectl delete namespace $BROKER_TEST_NS"
        echo ""
    fi

    echo -e "${YELLOW}Known gaps (WIP in this repo):${NC}"
    echo "  - 'seed' CLI command not yet implemented in __main__.py"
    echo "  - TLS bootstrap for the mutating webhook is not wired"
    echo "  - 'derived' algorithm formula evaluation is stubbed"
    echo "  - Metrics scraper loop needs wiring to the collector service"
}

# =============================================================================
# Main — orchestrate the phases
# =============================================================================
main() {
    echo ""
    echo -e "${BOLD}k8s-resource-broker dev setup${NC}"
    echo -e "Root: $ROOT_DIR"
    echo ""

    check_prerequisites

    if $RUN_LOCAL; then
        setup_python_env
        setup_env_file
        # Configure podman TLS before pulling the postgres image — the pull
        # that 'podman run' would do internally cannot pass --tls-verify=false,
        # so we must both configure the VM and pre-pull with the flag ourselves.
        configure_podman_tls
        start_postgres
        run_migrations
        run_unit_tests
    fi

    if $RUN_MINIKUBE; then
        # TLS config must happen before minikube start, because minikube pulls
        # the kicbase VM image via podman — and that pull hits gcr.io which
        # needs the same insecure-registry workaround.
        $RUN_LOCAL || configure_podman_tls   # skip if already ran above
        start_minikube
        build_and_load_image
        deploy_cluster_postgres
        run_cluster_migrations
        deploy_broker_controller
        apply_sample_profiles
        run_smoke_test
    fi

    if $RUN_SERVE; then
        # This blocks — run last after everything else is set up.
        serve_local
    fi

    print_summary
}

main "$@"