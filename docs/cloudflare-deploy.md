# Cloudflare deployment runbook (spec 009)

This runbook deploys the burstel Worker + per-shot `ShotDO` Containers + SQLite
`ControlDO` (formerly `IngestDO`) + R2 freeze layer. Since spec 023 the ingest
hot path is authenticated from KV `INSTANCE_TOKENS` and performs one R2 PUT;
the deployment must create that namespace and the `INGEST_ANALYTICS` dataset
(see the bindings in `wrangler.jsonc`) and, for an existing installation, apply
a Durable Object `renamed_classes` migration from `IngestDO` to `ControlDO`. **Nothing in this repository performs a Cloudflare
mutation on its own**: the preflight script is strictly read-only, and the
deploy sequence is an explicit operator action.

> **Atomicity warning.** On Cloudflare, a Worker deploy and a Container rollout
> are *not* atomic. `wrangler.jsonc` builds the container image from
> `../Dockerfile` during a real `wrangler deploy`, so **never deploy the Worker
> before the image build has been proven**. If the image cannot be built on the
> deploy host, stop here; do not deploy and then discover the Container cannot
> start.

## 1. Prerequisites

| Requirement | Why | How to confirm |
| --- | --- | --- |
| Cloudflare account on **Workers Paid** | Containers require a paid plan | `scripts/cloudflare-preflight.sh` (plan lookup) |
| R2 bucket `burstel-shots` | freeze target | preflight R2 lookup; **never auto-created** |
| `docker` + `docker buildx` on the deploy host | `wrangler deploy` builds the `../Dockerfile` image locally | preflight Docker checks |
| `wrangler` + `bun` (or node) | deploy + preflight validator | `wrangler --version`, `bun --version` |
| account id + read-only API token | preflight lookups | `wrangler whoami --json` |

The image is `linux/amd64` only (the DuckDB Go bindings ship a linux-amd64
static library). The Dockerfile is multistage Debian + cgo; it requires g++ and
runs as the nonroot user `burstel` (uid 10001).

## 2. Secrets (distinct, never committed)

| Secret | Scope | Where it lives |
| --- | --- | --- |
| `BURSTEL_API_TOKEN` | admin: `/api/*`, `/mcp`, legacy per-shot path | Worker secret |
| `BURSTEL_INTERNAL_TOKEN` | recorder `/internal/*` defense-in-depth | Worker secret, injected into the container by the `ShotDO` |
| instance push token | `POST /ingest/v1/{traces,metrics,logs}` only, exactly 24h, routes to its named instance | minted at runtime by `POST /api/instances/{name}/tokens` (admin\|member), shown **once**; only its digest is stored in `ControlDO` (and positively cached in KV) |
| per-person token | `/api/*` + `/mcp` as `admin` or `member` (member = full operational, no registry) | minted at runtime by `POST /api/tokens` (admin only), shown **once**; only its digest is stored in `AuthDO` |

Rules (all fail closed): the internal token must be non-empty and **different**
from the admin token; the admin token is never forwarded to the container; the
push token is never used as either. `.dev.vars` is gitignored and must never be
committed. `wrangler secret put` prompts without echoing the value:
```bash
wrangler secret put BURSTEL_API_TOKEN
wrangler secret put BURSTEL_INTERNAL_TOKEN
```

Or generate and put a strong secret in one step — the value is generated with
`openssl rand -hex 32`, sent to `wrangler secret put` over stdin (never argv),
and saved to a mode-600 file under `.secrets/` because a Worker secret cannot be
read back after it is set:

```bash
scripts/secret-put.sh BURSTEL_API_TOKEN       # or: make cf-secret-put NAME=BURSTEL_API_TOKEN
scripts/secret-put.sh BURSTEL_INTERNAL_TOKEN  # pass --show to print it once
```

## 3. Build and prove the image first

The exact image the Container runs must build and pass a smoke test **before**
any Worker deploy.

Locally, on a Docker host:

```bash
docker buildx build --platform linux/amd64 --load -t burstel:deploy .
scripts/container-smoke.sh burstel:deploy
```

In CI (used by this repo, because the authoring sandbox has no Docker daemon),
`ci/github-actions/container-image.yml` does the same on an Ubuntu Docker runner
(install it to `.github/workflows/` with `scripts/install-github-workflow.sh`;
the managed push App lacks the GitHub App `workflows` permission, so this
pipeline cannot push the workflow file itself):
real `docker buildx build --platform linux/amd64`, then
`scripts/container-smoke.sh`, which exercises `/healthz`, `/readyz`, real OTLP
traces/metrics/logs, `stop` → freeze size/SHA-256, on-disk DuckDB persistence
across a restart, the nonroot uid 10001, and the libstdc++ (cgo) link. It needs
**no Cloudflare credentials** and deploys nothing.

> `wrangler deploy --dry-run --containers-rollout=none` (and the package script
> `bun run deploy:dry-run`) only validates the Wrangler config and bundles the
> Worker. It does **not** build or validate the container image. A passing
> dry-run is not evidence the image builds.

**Prebuilt-image alternative.** If the deploy host cannot build locally, pin an
immutable image by digest from an officially supported registry (for example a
`registry.cloudflare.com/...@sha256:...` reference built by CI) and point the
`containers[].image` field at that digest. Do not invent image paths or
credentials, and do not use a mutable tag.

## 4. Read-only preflight

```bash
# Config invariants only (no credentials, safe anywhere, used by tests):
scripts/cloudflare-preflight.sh --config-only

# Full read-only preflight (requires wrangler auth / CLOUDFLARE_API_TOKEN):
CLOUDFLARE_API_TOKEN=... scripts/cloudflare-preflight.sh
```

The full preflight checks, without mutating anything:

* account identity + id (`wrangler whoami --json`, never `wrangler auth token`);
* Workers Paid / Containers eligibility (warns if unknown, fails if definitely
  not eligible; it never enrolls or upgrades a plan);
* existing R2 bucket lookup (reused, never created);
* Worker/DO collision lookup (a deploy updates in place; resources are never
  recreated);
* effective secret **presence** via `wrangler secret list` (names only — values
  are never read or printed);
* `docker`/`buildx`/`linux/amd64` compatibility;
* the `wrangler.jsonc` invariants: `ShotDO` container on `standard-1`, bounded
  `max_instances`, SQLite `ShotDO`/`ControlDO`/`AuthDO` exports,
  `BUCKET -> burstel-shots`, no `BURSTEL_DEV_INSECURE_AUTH`, no plaintext tokens
  in `vars`.

It exits non-zero and prints `BLOCKED` if anything required fails — including
"no image-build mechanism". It never runs `wrangler deploy`.

## 5. Deploy sequence

Only after step 3 (image proven) and step 4 (preflight green):

```bash
# From the repository root. This builds the container image from ../Dockerfile
# and rolls out the Worker + Container together.
(cd cloudflare && wrangler deploy)

curl -fsS https://<host>/healthz
```

If the deployment partially fails, the Worker and Container may be at different
revisions; re-run the deploy after fixing the image/config rather than patching
live.

## 6. Post-deploy smoke

```bash
BURSTEL_BASE_URL=https://<host> \
BURSTEL_API_TOKEN=... \
BURSTEL_INTERNAL_TOKEN=... \
scripts/cloudflare-smoke.sh
```

`scripts/cloudflare-smoke.sh` is a **small, bounded** E2E (two short shots —
never a 60/180s benchmark). It checks health, auth boundaries, creates a named
instance and issues its 24h token, sends real OTLP traces/metrics/logs through
the stable `/ingest` endpoint, freezes a shot, then runs REST
query/analyze/schema/compare and MCP `tools/list`/`tools/call`, verifies
admin/instance scope separation, and asserts the retired `/api/ingest-token` and
`/api/shots/{id}/ingest-token` endpoints return `410`. `scripts/cloudflare-multishot-smoke.sh`
additionally runs two **concurrent** shots, proves instance data isolation, and
(spec 013, Part B) mints two `member` tokens, cross-operates on each other's
shots, denies the person-token registry, revokes one immediately and lists MCP
tools.
Tokens are read from the environment, passed to curl through `--config` process
substitution (never argv) and to `cmd/otlpsend` through `BURSTEL_PUSH_TOKEN`;
they are never echoed, written to the repo, logged, put in URLs or CI artifacts.
It does not touch an ISUCON host. Both scripts need a live deploy host and are
not executed by the offline test suites.

### Installing the collector token on the ISUCON host (manual)

1. Create the named instance (`POST /api/instances`, admin|member auth) and issue
   its token (`POST /api/instances/{name}/tokens`); the response discloses the
   token **once**.
2. On the benchmark host, set it in the shell/unit environment only:
   `export OTEL_EXPORTER_OTLP_HEADERS="X-Burstel-Token=<token>"` with
   `OTEL_EXPORTER_OTLP_ENDPOINT=https://<host>/ingest`.
3. Never paste the token into a command line, commit it, or upload it
   automatically; it expires exactly 24h after issuance. Ingestion is continuous
   and independent of shots, so reconfigure only once per day.

## 7. Rollback

```bash
(cd cloudflare && wrangler rollback)   # previous Worker version
```

R2 objects (`shots/<id>/telemetry.duckdb`) are authoritative and are **never**
deleted by a rollback or by analysis; delete them explicitly only if intended.
Containers are ephemeral and are destroyed on freeze/idle.

## 8. Cost bounds

* `containers[].max_instances` is capped at 10 and validated by preflight; an
  account-level limit overrun surfaces to clients as a retryable `503`.
* `instance_type` is `standard-1`.
* Stop shots promptly; a recording Container holds an ephemeral disk.
* The durable raw journal writes one R2 object per accepted export under
  `journal/*`; add an R2 lifecycle rule for old `shots/*` **and** `journal/*`
  objects if long-term retention is not required. Compaction discovers these
  objects by listing (spec 023), so never delete `journal/*` before the
  compactor has published (or explicitly excluded) the object.
* Containers require Workers Paid; do not leave test Containers running.

## 9. Non-goals

This runbook and its scripts do not create buckets, upgrade plans, set secrets,
upload tokens to third-party hosts, or perform any Cloudflare mutation. The
deployment is always an explicit operator step.
