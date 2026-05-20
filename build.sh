#!/bin/bash
# Build and deploy kasten-metrics-model

set -euo pipefail

HARBOR="harbor.apps.openshift2.lab.home"
PROJECT="kasten-metrics-model"
IMAGE="${HARBOR}/${PROJECT}/${PROJECT}:latest"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "==> Building image: ${IMAGE}"
docker build -t "${IMAGE}" "${SCRIPT_DIR}"

echo "==> Pushing to Harbor"
docker push "${IMAGE}"

echo "==> Applying manifests"
oc apply -f "${SCRIPT_DIR}/manifests/deploy.yaml"

echo "==> Waiting for rollout"
oc rollout status deployment/kasten-metrics-model -n kasten-metrics-model --timeout=120s

echo "==> Route:"
oc get route kasten-metrics-model -n kasten-metrics-model \
  -o jsonpath='https://{.spec.host}{"\n"}'
