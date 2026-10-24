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
- Do not deploy or mutate Cloudflare resources from an implementation job.

## Toolchain

`go`, `gcc`, and `docker` are not guaranteed to be on the default `PATH` in a
coding sandbox. The project uses `github.com/duckdb/duckdb-go/v2`, which is
**cgo** and links a prebuilt static `libduckdb`, so `CGO_ENABLED=1` and a C
compiler are mandatory. Use the repository script instead of hard-coding paths:

```bash
# Writes .scratch/toolchain.env (gitignored) and prints the exports.
scripts/dev-toolchain.sh --write
. .scratch/toolchain.env

# Or source the exports directly:
source <(scripts/dev-toolchain.sh)
```

`scripts/dev-toolchain.sh` discovers `go` and `gcc` at runtime from `PATH`,
from a Nix store glob, or by realizing `nixpkgs#{go,gcc}` with `nix build` when
Nix is available. It never commits a binary: everything lives under the
gitignored `.scratch/`.

- Do not `cd` outside the workspace: the sandbox may deny `cd`/`workdir` into
  `/tmp` (use absolute paths for scratch files, or `go -C <dir>`).
- DuckDB is a single writer. Keep `database/sql` pinned to one connection and
  serialize writes. Standalone shots bound the WAL at 256 MiB by default and
  checkpoint from a background guard (`BURSTEL_WAL_CHECKPOINT_BYTES`); the
  ingest path never runs CHECKPOINT itself.
- Bun is on `PATH` in the standard sandbox. Install Worker dependencies with
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

- Docker/Podman may be unavailable: the Dockerfile can be linted by inspection
  only in that case. Report the missing toolchain as the blocker rather than
  claiming the image built.
- No third-party network calls are required for tests (module downloads and
  Docker base image pulls excepted).
