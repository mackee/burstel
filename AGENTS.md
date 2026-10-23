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

## Toolchain on build-host-1 (verified again for Phase 24)

`go`, `gcc`, and `docker` are **not** on the default `PATH`. A cgo-capable Go
toolchain is available in the Nix store; there is no root and no Docker.

| Tool | Location / version |
| --- | --- |
| go | `/nix/store/<hash>-go-1.25.5/bin/go` (the module may fetch its own patch toolchain) |
| gcc/g++ | `/nix/store/<hash>-gcc-wrapper-16.2.0/bin/` |
| docker | absent (cannot run the Docker build here) |

The store hashes change when the sandbox is rebuilt. Use the repo script
instead of copying a stale hash:

```bash
# Writes .scratch/toolchain.env (gitignored) and prints the exports.
scripts/dev-toolchain.sh --write
. .scratch/toolchain.env

# Or source the exports directly:
source <(scripts/dev-toolchain.sh)
```

On a cold store the script realizes `nixpkgs#{go,gcc}` from
`cache.nixos.org`; on a warm store it reuses the paths. It never commits a
binary: everything lives under the gitignored `.scratch/`. The Phase 24 manual
equivalent (adjust hashes with `ls /nix/store/*go-1*/bin/go` and
`ls /nix/store/*gcc-wrapper*/bin/gcc`) was:

```bash
export PATH=/nix/store/<hash>-go-1.25.5/bin:/nix/store/<hash>-gcc-wrapper-16.2.0/bin:$PATH
export CC=/nix/store/<hash>-gcc-wrapper-16.2.0/bin/gcc
export CXX=/nix/store/<hash>-gcc-wrapper-16.2.0/bin/g++
export CGO_ENABLED=1
```

- The project uses `github.com/duckdb/duckdb-go/v2`, which is **cgo** and links a
  prebuilt static `libduckdb`. `CGO_ENABLED=1` and a C compiler are mandatory.
- Do not `cd` outside the workspace: the sandbox denies `cd`/`workdir` into
  `/tmp` (use absolute paths for scratch files, or `go -C <dir>`).
- DuckDB is a single writer. Keep `database/sql` pinned to one connection and
  serialize writes. Standalone shots bound the WAL at 256 MiB by default and
  checkpoint from a background guard (`BURSTEL_WAL_CHECKPOINT_BYTES`); the
  ingest path never runs CHECKPOINT itself.
- Bun is on `PATH` (`bun --version` = 1.4.2). Install Worker dependencies with
  `bun install --frozen-lockfile` inside `cloudflare/` before typecheck/tests or
  `wrangler deploy --dry-run`.

### Common commands

```bash
# Go (after sourcing the toolchain env)
go build ./...
go vet ./...
go test ./...
gofmt -w cmd internal

# Worker
cd cloudflare
bun install --frozen-lockfile
bun run typecheck
bun run typecheck:test
bun test
bun run deploy:dry-run       # wrangler deploy --dry-run --containers-rollout=none
```


### Environment limits

- No Docker/Podman: the Dockerfile can be linted by inspection only. Report the
  missing toolchain as the blocker rather than claiming the image built.
- No third-party network calls are required for tests (module downloads and
  Docker base image pulls excepted).
