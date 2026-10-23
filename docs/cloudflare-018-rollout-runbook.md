# Spec 018 — Cloudflare compactor rollout runbook (code-only; do not deploy here)

This runbook is the executable, production-safe plan for turning on the
spec-018 compactor bridge (`journal/*` → Go compactor Container → immutable
`segments/*` → verified `IngestDO.segment_manifest`/`segment_coverage` → spec-016
window analysis). **Nothing in this repository's spec-018 job deploys, sets
secrets, creates buckets, changes billing or touches production.** The actual
rollout is a separate, explicitly authorized job that owns Docker and the
Cloudflare account credentials.

> The build-host sandbox has no Docker daemon and no Cloudflare credentials, so
> the image build and live Container scheduling below were **not** executed
> here. They are the required continuation steps.

## 0. What changes

| Area | Before | After |
| --- | --- | --- |
| Compaction | local `burstel-compact -store-dir` mirror only | Cron/DO-alarm `CompactorDO` drives a short-lived `burstel-compact` Container |
| R2 access | Worker `BUCKET` binding | Worker `BUCKET` binding only; the Container stays credential-free |
| New Durable Object | — | `CompactorDO` (SQLite), independent of `ShotDO` |
| New Container | `ShotDO` (recorder) | + `CompactorDO` (`Dockerfile.compactor`, `max_instances: 1`) |
| New bindings | `SHOT_DO`, `INGEST_DO`, `AUTH_DO`, `BUCKET` | + `COMPACTOR_DO` |
| Segment manifest | manual/absent | automatic verified `segment_manifest` + `segment_coverage` |
| Window analysis | needed manual publish | reads verified segments automatically |
| GC / retention | n/a | present but **disabled by default** (`COMPACTION_GC=0`) |
| Frozen `shots/*` | untouched | untouched (read-only; never rewritten or deleted) |

No existing binding, class or storage is removed. `ShotDO` and the legacy
recording image are unchanged; the compactor is a distinct class.

## 1. Preconditions

| Requirement | Why | How to confirm |
| --- | --- | --- |
| Workers Paid | Containers | `scripts/cloudflare-preflight.sh` |
| R2 bucket `burstel-shots` exists (never auto-created) | journal + segments + freeze | preflight R2 lookup |
| `docker` + `buildx` on the deploy host | both images build at deploy | preflight Docker checks |
| `BURSTEL_INTERNAL_TOKEN` secret present and **distinct** from the admin token | container auth | `wrangler secret list` / preflight |
| Current Worker version id | rollback target | `wrangler deployments list` (read-only) |

## 2. Prove both images first (no Cloudflare mutation)

```bash
# Recorder (unchanged) + compactor (new), both linux/amd64, real cgo DuckDB.
docker buildx build --platform linux/amd64 --load -t burstel:deploy .
docker buildx build --platform linux/amd64 --load -f Dockerfile.compactor -t burstel-compactor:deploy .

scripts/container-smoke.sh burstel:deploy            # OTLP + freeze + nonroot + restart
scripts/compactor-smoke.sh burstel-compactor:deploy  # health + auth + real segment + checksum + nonroot
```

`wrangler deploy --dry-run --containers-rollout=none` validates the Wrangler
config and bundles the Worker but does **not** build the images; it is not
evidence the images build.

## 3. Read-only preflight

```bash
scripts/cloudflare-preflight.sh --config-only
CLOUDFLARE_API_TOKEN=... scripts/cloudflare-preflight.sh
```

The config-only pass must include `container_compactordo_singleton`,
`container_compactor_image`, `container_compactor_max_instances_bounded`,
`do_binding_compactordo`, `export_compactordo_sqlite`, `compaction_cron_trigger`
and `compaction_gc_opt_in`.

## 4. Deploy sequence (separate authorized job)

```bash
(cd cloudflare && wrangler deploy)        # builds ../Dockerfile and ../Dockerfile.compactor
curl -fsS https://<host>/healthz
```

Then issue an instance token, send a small real OTLP batch, and let the cron /
alarm run:

```bash
# 1. Create an instance and a 24h token.
curl -fsS -X POST https://<host>/api/instances -H "Authorization: Bearer $ADMIN" \
  -d '{"name":"web-1"}'
curl -fsS -X POST https://<host>/api/instances/web-1/tokens -H "Authorization: Bearer $ADMIN"

# 2. Send a real export through /ingest (writes journal/*).
#    (use an OTLP exporter or scripts/cloudflare-smoke.sh)

# 3. Wait for the next bounded tick, then inspect backlog/health.
curl -fsS https://<host>/api/compaction/status -H "Authorization: Bearer $ADMIN"

# 4. Force one bounded tick if you do not want to wait for the alarm (admin only).
curl -fsS -X POST https://<host>/api/compaction/run -H "Authorization: Bearer $ADMIN"

# 5. Create a window shot over the data and analyze it — no manual publish.
#    A shot is a window by default (no mode needed); this backdated form leaves a
#    5s margin on both sides so client/server clock skew cannot clip the run.
curl -fsS -X POST https://<host>/api/shots/window -H "Authorization: Bearer $ADMIN" \
  -d '{"started_at":"...-5s","ended_at":"...","instances":["web-1"]}'
# `instances` must name registered instances: an unknown name is a
# 400 unknown_instance instead of a silently empty window.
# then shot_query / shot_analyze / shot_compare / shot_schema, or the MCP tools.
```

`compaction_status` must report `unprocessed_journal: 0`, `consecutive_failures:
0`, `scheduler_active: true` and a non-null `last_success_at` once a segment is
published.

## 5. Post-deploy verification (bounded, no benchmark)

```bash
BURSTEL_BASE_URL=https://<host> BURSTEL_API_TOKEN=... BURSTEL_INTERNAL_TOKEN=... \
  scripts/cloudflare-smoke.sh
BURSTEL_BASE_URL=https://<host> BURSTEL_API_TOKEN=... BURSTEL_INTERNAL_TOKEN=... \
  scripts/cloudflare-multishot-smoke.sh
```

Confirm historical data is preserved and analyzable:

```bash
burstel-cli shot get <a-phase-12-shot-id>
burstel-cli shot analyze <a-phase-12-shot-id> --top-n 5
wrangler r2 object list burstel-shots --prefix "shots/"   | head   # untouched
wrangler r2 object list burstel-shots --prefix "journal/" | head
wrangler r2 object list burstel-shots --prefix "segments/" | head
```

## 6. Enabling GC (optional, later, and only after trust)

GC is off by default. Only after observing stable verified coverage, flip
`COMPACTION_GC` to `1` and redeploy. A shot protects its receive-time range with
a TTL-bounded pin, and GC never deletes an object that is not attributed to a
*verified* segment. To force one GC pass without changing config:

```bash
curl -fsS -X POST https://<host>/api/compaction/run -H "Authorization: Bearer $ADMIN" -d '{"gc":true}'
```

## 7. Rollback

```bash
(cd cloudflare && wrangler rollback)   # previous Worker version
```

R2 (`shots/*`, `journal/*`, `segments/*`) is authoritative and untouched by a
rollback. Containers are ephemeral. The additive `CompactorDO` storage is
dormant under an old Worker.

## 8. Explicit blockers / non-goals for this job

* No Docker daemon here: neither image was built and neither smoke script ran.
* No Cloudflare credentials here: no deploy, no secret change, no bucket
  creation, no live Container scheduling, no live E2E.
* The live multi-user/window E2E (`scripts/cloudflare-multishot-smoke.sh`) needs
  a live deploy host.
* No production or staging resource was created or modified by spec 018.
