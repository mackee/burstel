# AGENTS.md

Guidance for coding agents working on **burstel** through itako.

## Workflow

- Specs live in the itako project `burstel`.
- Read the spec with `spec_get` before implementing anything.
- Implement against the current revision and its acceptance criteria. Do not assume an older draft.
- Commit your work inside the sandbox only.
- Push only through the itako `git_push` capability `git:push:burstel`.
- Never run a raw `git push` (no `git push`, `gh`, API calls, or credential setup).
- Do not modify specs unless explicitly asked.

## Toolchain on build-host-1 (verified for Phase 1)

`go`, `gcc`, `docker` are **not** on the default `PATH`, but a working Go +
cgo toolchain is available in the Nix store. There is no root and no Docker.

| Tool | Location / version |
| --- | --- |
| go | `/nix/store/<hash>-go-1.25.5/bin/go` (module auto-downloads go1.26.x) |
| gcc/g++ | `/nix/store/<hash>-gcc-wrapper-15.2.0/bin/` |
| docker | absent (cannot run the Docker build here) |

Setup used for every command (adjust the store hashes if the sandbox changes;
`ls /nix/store/*go-1*/bin/go` and `ls /nix/store/*gcc-wrapper*/bin/gcc`):

```bash
export PATH=/nix/store/<hash>-go-1.25.5/bin:/nix/store/<hash>-gcc-wrapper-15.2.0/bin:$PATH
export CC=/nix/store/<hash>-gcc-wrapper-15.2.0/bin/gcc
export CXX=/nix/store/<hash>-gcc-wrapper-15.2.0/bin/g++
export CGO_ENABLED=1
```

- The project uses `github.com/duckdb/duckdb-go/v2`, which is **cgo** and links a
  prebuilt static `libduckdb`. `CGO_ENABLED=1` and a C compiler are mandatory.
- Do not `cd` outside the workspace: the sandbox denies `cd`/`workdir` into
  `/tmp` (use absolute paths for scratch files, or `go -C <dir>`).
- DuckDB is a single writer. Keep `database/sql` pinned to one connection and
  serialize writes.

### Common commands

```bash
go build ./...
go vet ./...
go test ./...
gofmt -w cmd internal
```

### Environment limits

- No Docker/Podman: the Dockerfile can be linted by inspection only. Report the
  missing toolchain as the blocker rather than claiming the image built.
- No third-party network calls are required for tests (module downloads and
  Docker base image pulls excepted).
