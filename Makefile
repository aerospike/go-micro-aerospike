.PHONY: build vet test test-unit test-unit-race test-integration \
	test-integration-race fixture-up fixture-down secured-up secured-down \
	test-secured coverage

AEROSPIKE_ADDR ?= 127.0.0.1:3100
export AEROSPIKE_ADDR

build:
	go build ./...

vet:
	go vet ./...

test: test-unit

test-unit:
	go test -short ./...

test-unit-race:
	go test -short -race ./...

test-integration:
	go test -tags integration -count=1 ./...

test-integration-race:
	go test -tags integration -race -count=1 ./...

fixture-up:
	docker compose up -d --wait

fixture-down:
	docker compose down -v

# Secured lane (Enterprise eval image, TLS + RBAC). Explicitly opt-in: this
# target fails actionably if the fixture cannot be brought up rather than
# silently passing through skipped tests.
secured-up:
	docker compose -f docker-compose.secured.yml up -d --wait

secured-down:
	docker compose -f docker-compose.secured.yml down -v

test-secured:
	@test -f docker-certs/ca.crt || { \
		echo "secured fixture certificates missing: run \`make secured-up\` first" >&2; exit 1; }
	SECURED_REQUIRED=1 go test -tags secured -count=1 ./...

# Labeled coverage reports per lane.
coverage:
	@mkdir -p .coverage
	go test -short -coverprofile=.coverage/unit.out ./...
	AEROSPIKE_ADDR=$${AEROSPIKE_ADDR:-127.0.0.1:3100} go test -tags integration -count=1 -coverprofile=.coverage/integration.out .
	@echo "== unit ==" && go tool cover -func=.coverage/unit.out | tail -1
	@echo "== integration ==" && go tool cover -func=.coverage/integration.out | tail -1
