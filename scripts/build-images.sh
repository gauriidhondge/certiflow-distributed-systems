#!/usr/bin/env bash
# Builds all CertiFlow images with the tags used in k8s/*.yaml
set -e
cd "$(dirname "$0")/.."
for s in gateway event-service verify-service worker; do
  echo ">> building certiflow/$s:1.0"
  docker build -t certiflow/$s:1.0 ./$s
done
echo "Done. If you use minikube, run:  for s in gateway event-service verify-service worker; do minikube image load certiflow/\$s:1.0; done"
