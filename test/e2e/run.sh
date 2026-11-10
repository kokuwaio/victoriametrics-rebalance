#!/usr/bin/env bash
# Runs the end-to-end test on a local kind cluster: two small VMClusters, a VMAgent writing to both, and
# vmrebalance running locally (preflight, verify, status) and as a Job from the Helm chart (migrate).
#
# Needs docker, kind, helm, kubectl and go. The cluster gets its own kubeconfig file, so your current context
# stays untouched. Set KEEP_CLUSTER=1 to keep the cluster afterwards (the next run reuses it).
set -eu

cd "$(dirname "$0")/../.."
root=$(pwd)
name=vmrebalance-e2e
work="${root}/test/e2e/.work"
mkdir -p "$work"
export KUBECONFIG="${work}/kubeconfig"

if ! kind get clusters | grep -qx "$name"; then
  kind create cluster --name "$name" --kubeconfig "$KUBECONFIG" --wait 2m
fi
if [ "${KEEP_CLUSTER:-}" != "1" ]; then
  trap 'kind delete cluster --name "$name"' EXIT
fi

helm repo add vm https://victoriametrics.github.io/helm-charts/ >/dev/null 2>&1 || true
helm repo update vm >/dev/null
helm upgrade --install vm-operator vm/victoria-metrics-operator --version 0.68.2 \
  --namespace vm-operator --create-namespace --wait \
  --set admissionWebhooks.enabled=false

docker build -t vmrebalance:e2e .
kind load docker-image vmrebalance:e2e --name "$name"
go build -o "${work}/vmrebalance" ./cmd/vmrebalance

kubectl create namespace monitoring --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f test/e2e/manifests.yaml
for c in blue green; do
  kubectl -n monitoring wait "vmcluster/${c}" --for=jsonpath='{.status.updateStatus}'=operational --timeout=5m
done
kubectl -n monitoring wait vmagent/agent --for=jsonpath='{.status.updateStatus}'=operational --timeout=5m

VMREBALANCE_BIN="${work}/vmrebalance" go test -tags e2e -count=1 -v -timeout 30m ./test/e2e/
