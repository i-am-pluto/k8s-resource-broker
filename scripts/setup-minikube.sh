#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# Minikube setup + integration test for the resource-broker watcher.
#
# Phases:
#   1. Start minikube (if not running) and build the broker Docker image.
#   2. Deploy PostgreSQL, run DB migrations, seed profile data.
#   3. Deploy the broker in "controller" (watcher) mode — no API, no webhook.
#   4. Assert the controller pod is up and healthy.
#   5. Create annotated test pods and verify the watcher patches them.
#
# Usage:
#   ./scripts/setup-minikube.sh
#   CLUSTER_NAME=custom-name ./scripts/setup-minikube.sh
#
# Prerequisites:
#   minikube, kubectl, docker, uv
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail
shopt -s inherit_errexit nullglob

# ── Config ───────────────────────────────────────────────────────────────────
CLUSTER_NAME="${CLUSTER_NAME:-k8s-resource-broker}"
NAMESPACE="resource-broker"
BROKER_IMAGE="k8s-resource-broker:latest"
TEST_POD_NAME="test-annotated-pod"
POSTGRES_POD="postgres"
POSTGRES_SVC="postgres"
POSTGRES_IMAGE="postgres:16-alpine"
POSTGRES_USER="broker"
POSTGRES_PASSWORD="broker"
POSTGRES_DB="broker"
TARGET_NS="default"
TIMEOUT_SEC=60

GREEN='\033[0;32m'; RED='\033[0;31m'; NC='\033[0m'
pass() { echo -e "${GREEN}[PASS]${NC} $*"; }
fail() { echo -e "${RED}[FAIL]${NC} $*"; exit 1; }
info() { echo "[INFO] $*"; }
step() { local label="$1"; shift; echo ""; echo "═══ $label ═══"; "$@" || fail "Step failed: $label"; pass "$label"; }

# ── Prerequisites ────────────────────────────────────────────────────────────
for cmd in minikube kubectl docker uv; do
    command -v "$cmd" >/dev/null 2>&1 || fail "Missing prerequisite: $cmd"
done
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

# ── Phase 1 — minikube ──────────────────────────────────────────────────────
step "Starting minikube cluster: $CLUSTER_NAME" \
    minikube start --cpus=3 --memory=6g --driver=docker --profile="$CLUSTER_NAME"

step "Enabling metrics-server addon" \
    minikube addons enable metrics-server --profile="$CLUSTER_NAME"

eval "$(minikube docker-env --profile="$CLUSTER_NAME")"

step "Building broker Docker image" \
    docker build -t "$BROKER_IMAGE" -f "$ROOT_DIR/Dockerfile" "$ROOT_DIR"

# ── Phase 2 — PostgreSQL ────────────────────────────────────────────────────
step "Creating namespace: $NAMESPACE" \
    kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

info "Deploying PostgreSQL"
kubectl -n "$NAMESPACE" run "$POSTGRES_POD" \
    --image="$POSTGRES_IMAGE" \
    --env="POSTGRES_USER=$POSTGRES_USER" \
    --env="POSTGRES_PASSWORD=$POSTGRES_PASSWORD" \
    --env="POSTGRES_DB=$POSTGRES_DB" \
    --restart=Never --port=5432 --image-pull-policy=IfNotPresent 2>/dev/null || true

cat <<SVC | kubectl -n "$NAMESPACE" apply -f -
apiVersion: v1
kind: Service
metadata:
  name: $POSTGRES_SVC
spec:
  ports:
    - port: 5432
      targetPort: 5432
  selector:
    run: $POSTGRES_POD
SVC

step "Waiting for PostgreSQL to be ready" \
    kubectl -n "$NAMESPACE" wait --for=condition=Ready --timeout="${TIMEOUT_SEC}s" "pod/$POSTGRES_POD"

# ── Phase 3 — Migrations + Seed ─────────────────────────────────────────────
info "Port-forwarding PostgreSQL to localhost:5433"
kubectl -n "$NAMESPACE" port-forward "pod/$POSTGRES_POD" 5433:5432 &
PF_PID=$!
trap 'kill "$PF_PID" 2>/dev/null || true; exit' EXIT INT TERM
sleep 3

DATABASE_URL="postgresql+asyncpg://broker:broker@localhost:5433/broker"

step "Running DB migrations" \
    env BROKER_DATABASE_URL="$DATABASE_URL" \
    uv run --directory "$ROOT_DIR" alembic upgrade head

step "Seeding profile data" \
    env BROKER_DATABASE_URL="$DATABASE_URL" \
    uv run --directory "$ROOT_DIR" python -m resource_broker seed --config "$ROOT_DIR/scripts/seed-data.json"

kill "$PF_PID" 2>/dev/null; wait "$PF_PID" 2>/dev/null || true
trap - EXIT INT TERM

# ── Phase 4 — Deploy broker RBAC + ConfigMap + Controller ───────────────────
step "Deploying RBAC" \
    kubectl apply -f "$ROOT_DIR/deploy/resource-broker/rbac.yaml"

step "Deploying ConfigMap" \
    kubectl apply -f "$ROOT_DIR/deploy/resource-broker/configmap.yaml"

info "Deploying broker controller (watcher-only mode)"
cat <<DEPLOY | kubectl -n "$NAMESPACE" apply -f -
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
          image: $BROKER_IMAGE
          imagePullPolicy: IfNotPresent
          command: ["python", "-m", "resource_broker", "controller"]
          envFrom:
            - configMapRef:
                name: broker-config
          env:
            - name: BROKER_K8S_IN_CLUSTER
              value: "true"
            - name: BROKER_METRICS_URL
              value: "http://prometheus:9090"
            - name: BROKER_SCRAPER_INTERVAL_SECONDS
              value: "3600"
DEPLOY

step "Waiting for controller deployment to be Available" \
    kubectl -n "$NAMESPACE" wait --for=condition=Available --timeout="${TIMEOUT_SEC}s" \
        deployment/resource-broker-controller

CONTROLLER_POD=$(kubectl -n "$NAMESPACE" get pod -l app.kubernetes.io/component=controller \
    -o jsonpath='{.items[0].metadata.name}')
step "Controller pod $CONTROLLER_POD is Running" \
    kubectl -n "$NAMESPACE" wait --for=condition=Ready --timeout="${TIMEOUT_SEC}s" "pod/$CONTROLLER_POD"

echo ""
info "=== resource-broker is running! ==="
kubectl -n "$NAMESPACE" logs "$CONTROLLER_POD" --tail=5 2>/dev/null || true

# ── Phase 5 — Create annotated test pod and assert patching ────────────────
echo ""
info "═══ Deploying annotated test pod ═══"
cat <<POD | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: $TEST_POD_NAME
  namespace: $TARGET_NS
  annotations:
    resource-broker/profile: "k8s-efficient"
  labels:
    app: test-resource-broker
spec:
  containers:
    - name: nginx
      image: nginx:alpine
      ports:
        - containerPort: 80
      # Dummy resources so the JSON Patch replace op has a path to target
      resources:
        requests:
          cpu: 10m
          memory: 10Mi
        limits:
          cpu: 10m
          memory: 10Mi
POD

step "Waiting for test pod to be Ready" \
    kubectl -n "$TARGET_NS" wait --for=condition=Ready --timeout="${TIMEOUT_SEC}s" "pod/$TEST_POD_NAME"

# Poll for patches (should happen within seconds of ADDED event)
echo ""
info "═══ Asserting pod was patched by the watcher ═══"
for ((i=1; i<=15; i++)); do
    CPU_REQ=$(kubectl -n "$TARGET_NS" get pod "$TEST_POD_NAME" -o json 2>/dev/null \
        | python3 -c "import sys,json; d=json.load(sys.stdin); r=d.get('spec',{}).get('containers',[{}])[0].get('resources',{}).get('requests',{}); print(r.get('cpu','<none>'))" 2>/dev/null || echo "<error>")
    MEM_REQ=$(kubectl -n "$TARGET_NS" get pod "$TEST_POD_NAME" -o json 2>/dev/null \
        | python3 -c "import sys,json; d=json.load(sys.stdin); r=d.get('spec',{}).get('containers',[{}])[0].get('resources',{}).get('requests',{}); print(r.get('memory','<none>'))" 2>/dev/null || echo "<error>")

    if [[ "$CPU_REQ" == "250m" && "$MEM_REQ" == "256Mi" ]]; then
        pass "Pod patched: cpu=$CPU_REQ mem=$MEM_REQ"
        break
    fi
    if (( i < 15 )); then
        info "  Attempt $i/15: cpu=$CPU_REQ mem=$MEM_REQ (waiting...)"
        sleep 2
    else
        fail "Pod NOT patched within timeout. cpu=$CPU_REQ mem=$MEM_REQ"
    fi
done

# Also show limits for completeness
CPU_LIM=$(kubectl -n "$TARGET_NS" get pod "$TEST_POD_NAME" -o json 2>/dev/null \
    | python3 -c "import sys,json; d=json.load(sys.stdin); r=d.get('spec',{}).get('containers',[{}])[0].get('resources',{}).get('limits',{}); print(r.get('cpu','<none>'))" 2>/dev/null || echo "<error>")
MEM_LIM=$(kubectl -n "$TARGET_NS" get pod "$TEST_POD_NAME" -o json 2>/dev/null \
    | python3 -c "import sys,json; d=json.load(sys.stdin); r=d.get('spec',{}).get('containers',[{}])[0].get('resources',{}).get('limits',{}); print(r.get('memory','<none>'))" 2>/dev/null || echo "<error>")
info "Limits: cpu=$CPU_LIM memory=$MEM_LIM"

echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  All tests passed!                                        ║"
echo "║  Watcher is running and patching annotated pods.           ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""
echo "Controller logs:    kubectl -n $NAMESPACE logs $CONTROLLER_POD -f"
echo "Test pod details:   kubectl -n $TARGET_NS describe pod $TEST_POD_NAME"
echo "Clean up:           minikube delete --profile=$CLUSTER_NAME"
