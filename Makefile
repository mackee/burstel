GO ?= go
BUN ?= bun
IMAGE ?= burstel:deploy
IMAGE_COMPACTOR ?= burstel-compactor:deploy

.PHONY: build test vet fmt tidy run burstel-cli docker docker-compactor clean cf-typecheck cf-test cf-preflight cf-preflight-config cf-smoke cf-multishot-smoke cf-secret-put container-smoke compactor-smoke trial trial-smoke

build:
	$(GO) build -o bin/burstel ./cmd/burstel
	$(GO) build -o bin/burstel-trial ./cmd/burstel-trial
	$(GO) build -o bin/burstel-inspect ./cmd/burstel-inspect
	$(GO) build -o bin/burstel-compact ./cmd/burstel-compact
	$(GO) build -o bin/otlpsend ./cmd/otlpsend
	$(GO) build -o bin/burstel-cli ./cmd/burstel-cli

test:
	$(GO) test ./...

vet:
	$(GO) vet ./...

fmt:
	gofmt -w cmd internal

tidy:
	$(GO) mod tidy

run:
	$(GO) run ./cmd/burstel

# Admin CLI for a deployed Worker (shot/token/analysis); see cmd/burstel-cli.
burstel-cli:
	$(GO) build -o bin/burstel-cli ./cmd/burstel-cli

docker:
	docker build -t burstel:dev .

# Spec 018: the short-lived DO-mediated compactor service image.
docker-compactor:
	docker build -f Dockerfile.compactor -t burstel-compactor:dev .

# Phase 2 Cloudflare Worker (see cloudflare/README.md)
cf-typecheck:
	cd cloudflare && $(BUN) run typecheck && $(BUN) run typecheck:test

cf-test:
	cd cloudflare && $(BUN) test

# Phase 9: read-only deploy preflight and live smoke (see docs/cloudflare-deploy.md)
cf-preflight:
	scripts/cloudflare-preflight.sh

cf-preflight-config:
	scripts/cloudflare-preflight.sh --config-only

cf-smoke:
	scripts/cloudflare-smoke.sh

# Spec 014: named-instance continuous ingest plus a shared multi-user section.
# (Requires a live deploy host; not part of the offline gates.)
cf-multishot-smoke:
	scripts/cloudflare-multishot-smoke.sh

# Generate a strong Worker secret with openssl and put it in one step.
#   make cf-secret-put NAME=BURSTEL_API_TOKEN
cf-secret-put:
	scripts/secret-put.sh $(NAME)

# Real linux/amd64 container smoke against an already-built image
container-smoke:
	scripts/container-smoke.sh $(IMAGE)

# Spec 018: real linux/amd64 smoke for the compactor image
compactor-smoke:
	scripts/compactor-smoke.sh $(IMAGE_COMPACTOR)

# Standalone field trial (Phase 4); see docs/standalone-field-trial.md
trial:
	scripts/local-trial.sh

trial-smoke:
	scripts/local-trial.sh --smoke

clean:
	rm -rf bin
