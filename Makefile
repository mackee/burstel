GO ?= go

.PHONY: build test vet fmt tidy run docker clean

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

clean:
	rm -rf bin
