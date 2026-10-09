# syntax=docker/dockerfile:1

# burstel links the prebuilt static libduckdb via cgo, so the build stage needs
# a C/C++ toolchain and a glibc-based runtime. This image is intended for
# linux/amd64 (the DuckDB bindings ship a linux-amd64 static library).
FROM golang:1.26-bookworm AS build

WORKDIR /src

# cgo + C++ linker for the DuckDB static library.
RUN apt-get update \
 && apt-get install -y --no-install-recommends g++ \
 && rm -rf /var/lib/apt/lists/*

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ENV CGO_ENABLED=1
RUN go build -trimpath -ldflags="-s -w" -o /out/burstel ./cmd/burstel

FROM debian:bookworm-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates libstdc++6 \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 10001 --create-home --shell /usr/sbin/nologin burstel

COPY --from=build /out/burstel /usr/local/bin/burstel

WORKDIR /data
RUN chown burstel:burstel /data
USER burstel

EXPOSE 4318
ENV BURSTEL_ADDR=:4318 \
    BURSTEL_DATA_DIR=/data

ENTRYPOINT ["/usr/local/bin/burstel"]
