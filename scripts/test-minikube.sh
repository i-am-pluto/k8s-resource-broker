#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# Minikube integration test for the resource-broker watcher.
#
# What it does:
#   1. Starts minikube (if not running) and builds the broker image
#   2. Deploys PostgreSQL as a pod + service in the cluster
#   3. Runs DB migrations and seeds profile data
#   4. Deploys the broker in "controller" mode (watcher only, no API/webhook)
#   5. Asserts the controller pod is Running
#   6. Creates test pods annotated with "resource-broker/profile"
#   7. Waits up to 30s and asserts the watcher patched them with resource requests
#
# Usage:
#   ./scripts/test-minikube.sh
#
# Prerequisites:
#   - minikube, kubectl, docker, uv (or python/pip)
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
TEST_NAMESPACE="default"
TIMEOUT_SEC=60

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

pass() { echo -e "${GREEN}[PASS]${NC} $*"; }
fail() { echo -e "${RED}[FAIL]${NC} $*"; exit 1; }
info() { echo "[INFO] $*"; }

# ── Helper: run a step and exit on failure ──────────────────────────────────
step() {
    local label="$1"; shift
    echo ""
    info "═══ $label ═══"
    "$@" || fail "Step failed: $label"
    pass "$label"
}

# ── Prerequisites check ─────────────────────────────────────────────────────
check_prereqs() {
    for cmd in minikube kubectl docker uv; do
        command -v "$cmd" >/dev/null 2>&1 || fail "Missing prerequisite: $cmd"
    done
}
check_prereqs
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

# ── Step 1: Start minikube ─────────────────────────────────────────────────
step "Starting minikube cluster" \
    minikube start \
        --cpus=3 --memory=6g --driver=docker \
        --profile="$CLUSTER_NAME"

step "Enabling metrics-server addon" \
    minikube addons enable metrics-server --profile="$CLUSTER_NAME"

# Use minikube's docker daemon
eval "$(minikube docker-env --profile="$CLUSTER_NAME")"

# ── Step 2: Build broker Docker image ──────────────────────────────────────
step "Building broker image into minikube" \
    docker build -t "$BROKER_IMAGE" -f "$ROOT_DIR/Dockerfile" "$ROOT_DIR"

# ── Step 3: Create namespace and deploy PostgreSQL ─────────────────────────
step "Creating namespace" \
    kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

info "Deploying PostgreSQL"
# Deploy postgres as a standalone pod (simpler than a full deployment for testing)
kubectl -n "$NAMESPACE" run "$POSTGRES_POD" \
    --image="$POSTGRES_IMAGE" \
    --env="POSTGRES_USER=$POSTGRES_USER" \
    --env="POSTGRES_PASSWORD=$POSTGRES_PASSWORD" \
    --env="POSTGRES_DB=$POSTGRES_DB" \
    --restart=Never \
    --port=5432 \
    --image-pull-policy=IfNotPresent 2>/dev/null || true

# Create corresponding service
cat <<EOF | kubectl -n "$NAMESPACE" apply -f -
apiVersion: v1
kind: Service
metadata:
  name: $POSTGRES_SVC
  namespace: $NAMESPACE
spec:
  ports:
    - port: 5432
      targetPort: 5432
  selector:
    run: $POSTGRES_POD
EOF

step "Waiting for PostgreSQL to be ready" \
    kubectl -n "$NAMESPACE" wait --for=condition=Ready --timeout="${TIMEOUT_SEC}s" "pod/$POSTGRES_POD"

# ── Step 4: Run DB migrations + seed ───────────────────────────────────────
info "Port-forwarding PostgreSQL to localhost:5433"
kubectl -n "$NAMESPACE" port-forward "pod/$POSTGRES_POD" 5433:5432 &
PF_PID=$!
trap 'kill "$PF_PID" 2>/dev/null || true' EXIT

# Wait for port-forward to be ready
sleep 3

DATABASE_URL="postgresql+asyncpg://broker:broker@localhost:5433/broker"

step "Running DB migrations" \
    env BROKER_DATABASE_URL="$DATABASE_URL" \
    uv run --directory "$ROOT_DIR" alembic upgrade head

step "Seeding profile data" \
    env BROKER_DATABASE_URL="$DATABASE_URL" \
    uv run --directory "$ROOT_DIR" python -m resource_broker seed --config "$ROOT_DIR/scripts/seed-data.json"

# Stop port-forward (we don't need it anymore; the broker in-cluster will use the service)
kill "$PF_PID" 2>/dev/null; wait "$PF_PID" 2>/dev/null || true
trap - EXIT
info "PostgreSQL port-forward stopped"

# ── Step 5: Deploy broker RBAC + ConfigMap ─────────────────────────────────
step "Deploying RBAC" \
    kubectl apply -f "$ROOT_DIR/deploy/resource-broker/rbac.yaml"

# Deploy configmap (but override the database URL to use postgres service)
step "Deploying ConfigMap" \
    kubectl apply -f "$ROOT_DIR/deploy/resource-broker/configmap.yaml"

# ── Step 6: Deploy broker in controller (watcher) mode ─────────────────────
info "Deploying broker controller (watcher only)"

cat <<EOF | kubectl -n "$NAMESPACE" apply -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: resource-broker-controller
  namespace: $NAMESPACE
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
              value: "http://nonexistent:9090"
            - name: BROKER_SCRAPER_INTERVAL_SECONDS
              value: "3600"
          resources:
            requests:
              cpu: 100m
              memory: 128Mi
            limits:
              cpu: 250m
              memory: 256Mi
EOF

step "Waiting for broker controller to be ready" \
    kubectl -n "$NAMESPACE" wait --for=condition=Available --timeout="${TIMEOUT_SEC}s" \
        deployment/resource-broker-controller

CONTROLLER_POD=$(kubectl -n "$NAMESPACE" get pod -l app.kubernetes.io/component=controller \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
if [[ -z "$CONTROLLER_POD" ]]; then
    fail "Controller pod not found"
fi
pass "Controller pod: $CONTROLLER_POD is running"

# Tail logs briefly to confirm it started
info "Controller logs (first 10 lines):"
kubectl -n "$NAMESPACE" logs "$CONTROLLER_POD" --tail=10 2>/dev/null || true

# ── Step 7: Deploy test pod with annotation ────────────────────────────────
info "Deploying test pod with profile annotation"
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: $TEST_POD_NAME
  namespace: $TEST_NAMESPACE
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
      resources:
        requests:
          cpu: 10m
          memory: 10Mi
        limits:
          cpu: 10m
          memory: 10Mi
EOF

# Wait for the pod to be running
step "Waiting for test pod to be Running" \
    kubectl -n "$TEST_NAMESPACE" wait --for=condition=Ready --timeout="${TIMEOUT_SEC}s" "pod/$TEST_POD_NAME"

# ── Step 8: Assert the pod was patched ─────────────────────────────────────
info "Checking if watcher patched the test pod..."

PATCH_CHECK_ATTEMPTS=15
PATCH_CHECK_INTERVAL=2

for ((i=1; i<=PATCH_CHECK_ATTEMPTS; i++)); do
    POD_JSON=$(kubectl -n "$TEST_NAMESPACE" get pod "$TEST_POD_NAME" -o json 2>/dev/null || true)
    CPU_REQ=$(echo "$POD_JSON" | python3 -c "
import sys, json
doc = json.load(sys.stdin)
ctrs = doc.get('spec', {}).get('containers', [])
for c in ctrs:
    r = c.get('resources', {}).get('requests', {})
    print(r.get('cpu', '<none>'))
    break
" 2>/dev/null || echo "<error>")
    MEM_REQ=$(echo "$POD_JSON" | python3 -c "
import sys, json
doc = json.load(sys.stdin)
ctrs = doc.get('spec', {}).get('containers', [])
for c in ctrs:
    r = c.get('resources', {}).get('requests', {})
    print(r.get('memory', '<none>'))
    break
" 2>/dev/null || echo "<error>")

    if [[ "$CPU_REQ" == "250m" && "$MEM_REQ" == "256Mi" ]]; then
        pass "Pod was patched correctly: requests.cpu=$CPU_REQ requests.memory=$MEM_REQ"
        break
    fi

    if (( i < PATCH_CHECK_ATTEMPTS )); then
        info "  Attempt $i/$PATCH_CHECK_ATTEMPTS: cpu=$CPU_REQ mem=$MEM_REQ (waiting for patching...)"
        sleep "$PATCH_CHECK_INTERVAL"
    else
        fail "Pod was NOT patched within timeout. Final state: cpu=$CPU_REQ mem=$MEM_REQ"
    fi
done

# Also check limits
CPU_LIM=$(echo "$POD_JSON" | python3 -c "
import sys, json
doc = json.load(sys.stdin)
ctrs = doc.get('spec', {}).get('containers', [])
for c in ctrs:
    r = c.get('resources', {}).get('limits', {})
    print(r.get('cpu', '<none>'))
    break
" 2>/dev/null || echo "<error>")
MEM_LIM=$(echo "$POD_JSON" | python3 -c "
import sys, json
doc = json.load(sys.stdin)
ctrs = doc.get('spec', {}).get('containers', [])
for c in ctrs:
    r = c.get('resources', {}).get('limits', {})
    print(r.get('memory', '<none>'))
    break
" 2>/dev/null || echo "<error>")

info "Limits: cpu=$CPU_LIM memory=$MEM_LIM"

# ── Log the watcher output for debugging ─────────────────────────────────
echo ""
info "=== Controller logs (last 20 lines) ==="
kubectl -n "$NAMESPACE" logs "$CONTROLLER_POD" --tail=20 2>/dev/null || true

# ── Success ────────────────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║            All tests passed!                                ║"
echo "║                                                            ║"
echo "║  Watcher is running and successfully patched the test pod.  ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""
echo "To view watcher logs:  kubectl -n $NAMESPACE logs $CONTROLLER_POD -f"
echo "To clean up:          minikube delete --profile=$CLUSTER_NAME"
