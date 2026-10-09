GO ?= go
BUN ?= bun

.PHONY: build test vet fmt tidy run docker clean cf-typecheck cf-test

build:
	$(GO) build -o bin/burstel ./cmd/burstel

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

clean:
	rm -rf bin
