# Spec 017 — production rollout runbook: Phase 12 → specs 014–016

This runbook is the executable, **production-safe** migration plan for jumping a
deployed **Phase 12** Worker straight to the current tree (specs **014**
continuous named-instance intake, **015** segmented R2 compaction, **016**
logical window shots). **Spec 013 was never deployed**, so there is no
intermediate per-person-token deployment to migrate from: the jump is
Phase 12 → 014/015/016 in one Worker deploy.

> This document is part of the spec-017 **read-only audit**. Nothing here has
> been executed against production. It performs no Cloudflare mutation, sets no
> secrets, creates no buckets and deploys nothing. The actual deploy is a
> separate, explicitly authorized job that must own Docker and the account
> credentials.

## 0. What changes for an operator (read this first)

| Area | Phase 12 (deployed) | After the jump | Compatibility |
| --- | --- | --- | --- |
| Continuous OTLP endpoint | installation-wide push token at `/ingest/v1/*` | named-**instance** push token at the same `/ingest/v1/*` | **breaking for the credential, not the URL** |
| Per-shot token endpoints | `POST /api/ingest-token`, `POST /api/shots/{id}/ingest-token` | `410 Gone` with migration guidance | collectors that *fetch* a token from these must be re-pointed |
| Per-shot OTLP path | `/shots/{id}/v1/{traces,metrics,logs}` (admin) | unchanged, still works | **dual-route retained** |
| Person tokens | none (env admin only) | optional `admin`/`member` registry in `AuthDO` | additive; env admin still works |
| Frozen shots | `shots/{id}/telemetry.duckdb` in R2 | unchanged; legacy shots remain analyzable | **no data migration** |
| Raw intake storage | n/a | `journal/{instance_id}/…` in the same bucket | additive prefix |
| Durable Objects | `ShotDO` | `ShotDO` + `IngestDO` + `AuthDO` | additive classes; `ShotDO` storage preserved |
| Shot creation | shot = recording container | shot = logical **window** by default (spec 022) | `mode=recording` keeps the legacy shot, flagged `deprecated: true` |

> Since spec 022, `POST /api/shots`, MCP `shot_start` and
> `burstel-cli shot start` open a logical window when `mode` is omitted, and
> window `instances` must already exist (`400 unknown_instance` otherwise).
> Operators should stop minting per-shot tokens and use one named-instance
> token per host.

The two real cutover risks are therefore:

1. **A collector pinned to the Phase 12 installation-wide `/ingest` token will
   get `401` after the jump.** The URL does not change; the accepted credential
   does. This must be handled by *staged credential enrollment* (§4), not by
   silently flipping the Worker.
2. **A collector that mints a per-shot token on every shot via
   `/api/shots/{id}/ingest-token` will get `410 Gone`.** This must be replaced
   with one named-instance token configured once per day.

Both are addressed by enrolling named instances and cutting exporters over
**before** the Worker deploy, while the legacy per-shot path stays live as the
dual route.

## 1. Preconditions

| Requirement | Why | How to confirm |
| --- | --- | --- |
| Workers Paid | Containers | `scripts/cloudflare-preflight.sh` |
| R2 bucket `burstel-shots` exists (never auto-created) | journal + freeze | preflight R2 lookup |
| `docker` + `buildx` on the deploy host | `wrangler deploy` builds `../Dockerfile` | preflight Docker checks |
| A recorded inventory of every live collector and its credential/URL | staged cutover | operator checklist (§3) |
| Current Worker version id | rollback target | `wrangler deployments list` (read-only) |

The image is `linux/amd64` only. The container build must be proven (§2) before
the Worker deploy; a Worker/Container rollout is **not atomic**.

## 2. Prove the image first (unchanged from spec 009)

```bash
docker buildx build --platform linux/amd64 --load -t burstel:deploy .
scripts/container-smoke.sh burstel:deploy        # OTLP + freeze + nonroot + restart
```

`wrangler deploy --dry-run --containers-rollout=none` validates the Wrangler
config and bundles the Worker but does **not** build the image; it is not
evidence the image builds.

## 3. Collector inventory and staged credential enrollment

**Do this before the deploy.** The goal is that every exporter already speaks
the new credential when the new Worker goes live.

1. Inventory every collector: its `OTEL_EXPORTER_OTLP_ENDPOINT`, the credential
   it sends, and whether it mints a token per shot.
2. For each collector create a named instance and issue its exactly-24h token
   **on the current Phase 12 Worker** is not possible (the instance endpoints do
   not exist yet). Instead, issue them immediately **after** the new Worker is
   live and **before** cutting the collector over, or use a temporary
   dual-accept window (§4, option B).
3. On the collector host set, once only per day:
   ```bash
   export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
   export OTEL_EXPORTER_OTLP_ENDPOINT=https://<host>/ingest
   export OTEL_EXPORTER_OTLP_HEADERS="X-Burstel-Token=<instance token>"
   ```
   Never paste the token into argv, a commit, or an upload; it expires exactly
   24h after issue. For benchmark-host tuning (gzip, BSP batch/queue, shutdown
   and colo routing), see [`isucon-runbook.md`](./isucon-runbook.md).

> **Zero-downtime requirement?** A collector cannot present a new credential it
> has not been given. If a hard cutover is unacceptable, ship the **scoped
> follow-up** in §4 option B (a temporary dual-accept shim) before this deploy.

## 4. Migration options

### Option A — staged cutover (default, no code change)

1. Deploy the new Worker (§5).
2. Immediately create instances + tokens and cut collectors over (§3.3),
   verifying each with a real OTLP export.
3. Retire the old collectors. The legacy `/shots/{id}/v1/*` path remains for any
   shot-scoped collector that cannot move yet.

Downtime window = time between deploy and each collector cutover. Keep it short;
do the cutover in the same maintenance window as the deploy.

### Option B — reversible dual-accept flag (recommended if zero downtime is required)

This needs a small, separately-scoped implementation job (it is **not** in the
current tree and must not be improvised during the deploy):

* add a Worker var, e.g. `LEGACY_INSTALLATION_INGEST=1`, read only by the
  `/ingest/v1/*` handler;
* when enabled, accept the Phase 12 installation-wide token and route it to a
  configured default instance (`LEGACY_DEFAULT_INSTANCE`), logging a deprecation
  counter (never the token);
* default **off**; flip it off after the last collector is migrated.

This keeps the deploy reversible at the credential layer: roll the flag back to
restore the old behaviour without a Worker rollback.

### Reversibility

* `wrangler rollback` restores the previous Worker version. R2 objects are never
  deleted by a rollback or by analysis.
* The new `IngestDO`/`AuthDO` storage is additive and dormant under an old
  Worker; nothing is destroyed.
* `ShotDO` SQLite storage is preserved because the class is unchanged.

## 5. Deploy sequence

Only after §2 (image proven) and preflight are green:

```bash
# Read-only preflight (config invariants first, then full).
scripts/cloudflare-preflight.sh --config-only
CLOUDFLARE_API_TOKEN=... scripts/cloudflare-preflight.sh

# Deploy Worker + Container together (builds ../Dockerfile).
(cd cloudflare && wrangler deploy)

curl -fsS https://<host>/healthz
```

### New bindings added by this jump

`cloudflare/wrangler.jsonc` adds, relative to Phase 12:

* `INGEST_DO` (`IngestDO`, SQLite) — instance registry, 24h push tokens, raw
  journal ledger, segment manifest/coverage, window/shot registry;
* `AUTH_DO` (`AuthDO`, SQLite) — optional per-person token registry;
* the `BUCKET -> burstel-shots` R2 binding is now also used for the raw journal.

The `ShotDO` container binding, image and `max_instances` are unchanged. The
preflight already validates the SQLite `ShotDO`/`IngestDO`/`AuthDO` exports,
`BUCKET -> burstel-shots`, no anonymous admin and no plaintext tokens in `vars`.

## 6. Post-deploy verification (bounded, no benchmark)

```bash
BURSTEL_BASE_URL=https://<host> BURSTEL_API_TOKEN=... BURSTEL_INTERNAL_TOKEN=... \
  scripts/cloudflare-smoke.sh
```

This checks health, auth boundaries, named-instance create + 24h token, real
OTLP traces/metrics/logs through `/ingest`, a frozen shot, REST
query/analyze/schema/compare, MCP, admin/instance scope separation, and that the
retired token endpoints return `410`.

Then verify **historical data is preserved and analyzable**:

```bash
# The legacy frozen artifacts must still list and analyze.
burstel-cli shot get <a-phase-12-shot-id>
burstel-cli shot analyze <a-phase-12-shot-id> --top-n 5

# The new raw journal must be present under a distinct prefix.
wrangler r2 object list burstel-shots --prefix "journal/" | head
# and the old frozen archives untouched:
wrangler r2 object list burstel-shots --prefix "shots/" | head
```

Do **not** orphan historical snapshots: never delete `shots/*` during the
migration, and never rewrite an existing `shots/{id}/telemetry.duckdb`. The new
code only reads them.

## 7. Rollback

```bash
(cd cloudflare && wrangler rollback)     # previous Worker version
```

R2 (`shots/*`, `journal/*`) is authoritative and untouched. Containers are
ephemeral. If option B's flag is on, also roll the flag off (or leave it on under
the old Worker, where it is inert).

## 8. Explicit non-goals / blockers for the audit

* No production Cloudflare mutation, no secret change, no bucket creation and no
  deploy were performed by spec 017.
* The container image build and real Container scheduling were **not** executed
  here (no Docker daemon); they are deferred to the separate authorized deploy
  job and to `ci/github-actions/container-image.yml`.
* The live multi-user/window E2E in `scripts/cloudflare-multishot-smoke.sh` was
  not executed (needs a live deploy host).
