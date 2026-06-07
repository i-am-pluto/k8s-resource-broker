#!/usr/bin/env bash
set -euo pipefail

echo "==> Installing minikube..."
if ! command -v minikube &>/dev/null; then
    brew install minikube
fi

echo "==> Installing kubectl..."
if ! command -v kubectl &>/dev/null; then
    brew install kubectl
fi

echo "==> Installing Helm..."
if ! command -v helm &>/dev/null; then
    brew install helm
fi

echo "==> Installing uv (if not present)..."
if ! command -v uv &>/dev/null; then
    curl -LsSf https://astral.sh/uv/install.sh | sh
fi

echo "==> Installing PostgreSQL client..."
if ! command -v psql &>/dev/null; then
    brew install libpq
    brew link --force libpq 2>/dev/null || true
fi

echo "==> All tools installed."
echo "    minikube : $(minikube version --short 2>/dev/null || echo 'not found')"
echo "    kubectl  : $(kubectl version --client -o json 2>/dev/null | python3 -c 'import sys,json; print(json.load(sys.stdin).get("clientVersion",{}).get("gitVersion",""))')"
echo "    helm     : $(helm version --short 2>/dev/null || echo 'not found')"
echo "    uv       : $(uv --version 2>/dev/null || echo 'not found')"
