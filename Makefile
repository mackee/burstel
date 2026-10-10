GO ?= go
BUN ?= bun
IMAGE ?= burstel:deploy

.PHONY: build test vet fmt tidy run docker clean cf-typecheck cf-test cf-preflight cf-preflight-config cf-smoke cf-secret-put container-smoke trial trial-smoke

build:
	$(GO) build -o bin/burstel ./cmd/burstel
	$(GO) build -o bin/burstel-trial ./cmd/burstel-trial
	$(GO) build -o bin/burstel-inspect ./cmd/burstel-inspect
	$(GO) build -o bin/otlpsend ./cmd/otlpsend

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

docker:
	docker build -t burstel:dev .

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

# Generate a strong Worker secret with openssl and put it in one step.
#   make cf-secret-put NAME=BURSTEL_API_TOKEN
cf-secret-put:
	scripts/secret-put.sh $(NAME)

# Real linux/amd64 container smoke against an already-built image
container-smoke:
	scripts/container-smoke.sh $(IMAGE)

# Standalone field trial (Phase 4); see docs/standalone-field-trial.md
trial:
	scripts/local-trial.sh

trial-smoke:
	scripts/local-trial.sh --smoke

clean:
	rm -rf bin
