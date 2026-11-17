#!/usr/bin/env bash
# CI: build, vet, unit tests, then integration tests against the
# docker-compose Community Edition fixture.
set -euo pipefail
cd "$(dirname "$0")/.."

make build vet test-unit test-unit-race

trap 'docker compose down -v' EXIT
docker compose up -d --wait
make test-integration test-integration-race
