GO ?= go
BUN ?= bun

.PHONY: build test vet fmt tidy run docker clean cf-typecheck cf-test trial trial-smoke

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

# Standalone field trial (Phase 4); see docs/standalone-field-trial.md
trial:
	scripts/local-trial.sh

trial-smoke:
	scripts/local-trial.sh --smoke

clean:
	rm -rf bin
