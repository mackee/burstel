# ISUCON telemetry runbook (spec 027)

This is the operator runbook for sending a real ISUCON-style benchmark run to a
deployed burstel Worker. It records what the isolated client settings do, how to
verify them, and what to do at the end of a 1–3 minute shot. It does **not**
deploy or mutate any Cloudflare resource; deployment stays in
[`cloudflare-deploy.md`](./cloudflare-deploy.md).

The measured numbers below come from the real Cloudflare staging benchmark in
spec 026 (`026-staging-benchmark-result.md`; client on a Linux host in Japan
with a ~22 MB/s uplink, `workers.dev` served from colo SIN, ~66 ms RTT). Spec
026 lives in the itako project, not in this source tree.

## Sending telemetry

Configure every exporter once per instance token. The endpoint and header are
the same for all three signals:

```bash
export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
export OTEL_EXPORTER_OTLP_ENDPOINT=https://<host>/ingest
export OTEL_EXPORTER_OTLP_HEADERS="X-Burstel-Token=<instance token>"

# Recommended starting point for an ISUCON benchmark host:
export OTEL_EXPORTER_OTLP_COMPRESSION=gzip
export OTEL_BSP_MAX_EXPORT_BATCH_SIZE=8192
export OTEL_BSP_MAX_QUEUE_SIZE=131072
# If you also ship logs with the OTel Go SDK:
export OTEL_BLRP_MAX_EXPORT_BATCH_SIZE=8192
export OTEL_BLRP_MAX_QUEUE_SIZE=131072
```

`/ingest` is the stable, shot-independent base URL. The SDK appends
`/v1/traces`, `/v1/metrics` and `/v1/logs`. `X-Burstel-Token` carries the
24-hour push-only **instance** token (the admin/member token is rejected on
`/ingest`). Never put the token in argv or a committed file.

### Why these settings

The stock OTel Go SDK uses a BatchSpanProcessor with a **512** span batch and no
compression. Its exporter loop is serial: with the staging RTT, one export took
**~330–430 ms** in the B1 topology, which caps a single exporter at roughly
**1.2k spans/s**. Anything above that fills the queue and is dropped **inside
the SDK**, silently: the report shows zero exporter errors even while spans are
lost. Raising the span rate does not raise this ceiling.

Measured on staging:

| Client setting | Result |
| --- | --- |
| SDK defaults (batch 512, no gzip) | 330–430 ms per export; ~1.2k spans/s per exporter |
| gzip only | p50 export 377 → 330 ms; still queue-bound |
| gzip + BSP batch 8192 + queue 131072 | **0.0 % drop at 3 exporters × 15k spans/s (B1)** |
| same settings, B0 topology | still **14 %** drop |

Gzip alone only trims the export time. The large batch and queue are what let a
serial exporter keep up with the offered rate. Batch/queue settings apply to
traces and logs (separate `OTEL_BSP_*` / `OTEL_BLRP_*` variables); the metric
periodic reader exports one collection per interval and has no equivalent
batch/queue knob.

### Expected ceiling

The safe ceiling is a function of export latency, not of the SDK's default
batch size. With the recommended settings, **3 exporters × 15k spans/s** was
measured at **0 % drop** (B1). That is a staging result, not a guarantee: a
longer RTT, a different colo or a B0 workload can still drop. Watch the
generated-vs-exported difference in the loadgen report, and remember that a
clean `exporter_errors` field does **not** prove that spans were delivered.

### Shutdown

Large batches take time to drain. At bench end, every app must call
`TracerProvider.Shutdown`, `MeterProvider.Shutdown` and `LoggerProvider.Shutdown`
with a timeout of **at least 20 s**:

```go
ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
defer cancel()
_ = tp.Shutdown(ctx)
_ = mp.Shutdown(ctx)
_ = lp.Shutdown(ctx)
```

Staging measured **6–15 s** just to flush the large batches, so a shorter
timeout truncates the final export and loses exactly the tail of the shot.
Compile-time instrumentation (agents that inject instrumentation at build time)
must also flush on process exit; `Shutdown` on a provider only flushes the
providers your process owns.

## Routing and colo

`workers.dev` is anycast and may be served from a non-local Cloudflare colo. In
the staging run, traffic from Japan was served from **SIN**, not NRT. Do not
assume that the benchmark host's city predicts the serving colo.

Use a **custom domain on a Cloudflare zone** you control for the ingest host
(for example `telemetry.example.com`). It gives you a stable hostname, your own
DNS/TLS and zone-level routing controls. Check the actual serving colo before a
shot:

```bash
curl -fsS https://<host>/cdn-cgi/trace | grep '^colo='
# colo=SIN
```

A custom domain does **not** guarantee a specific colo. Treat the result as
observed routing, not as a contract.

## Pre-shot checklist

1. Create the named instance and issue its 24-hour push token
   (`POST /api/instances`, `POST /api/instances/{name}/tokens`). Configure the
   environment once; `/ingest` is independent of shots.
2. Probe public reachability and get batch advice (no token required):

   ```bash
   burstel-cli doctor --base-url https://<host> --target-spans-per-second 15000
   ```

   It prints the measured `/healthz` p50/p95 RTT, the serving colo and a
   suggested batch size. The suggestion is
   `ceil(target_spans_per_s × p50_export_s)`, rounded up to a power of two,
   clamped to `[512, 16384]`. By default the measured `/healthz` RTT is used as
   the p50 export proxy; pass `--p50-export 330ms` to use the staging number
   explicitly. The output is JSON:

   ```json
   {
     "base_url": "https://<host>",
     "healthz_samples": 10,
     "healthz_rtt_ms_p50": 66.1,
     "healthz_rtt_ms_p95": 71.2,
     "serving_colo": "SIN",
     "target_spans_per_s": 15000,
     "p50_export_ms_used": 66.1,
     "suggested_batch_size": 1024
   }
   ```

3. Start a window shot around the benchmark (`burstel-cli shot start`), or use
   `shot create --started-at/--ended-at` with a clock-skew margin.
4. Run the benchmark, then flush the apps with the ≥20 s shutdown budget above.
5. Stop the shot (`burstel-cli shot stop <shot-id>`) and query/analyze it.

## Reproducing the client settings with burstel-loadgen

`burstel-loadgen` uses the official OTel Go SDK and now exposes the same knobs
for a controlled ceiling test:

```bash
burstel-loadgen -endpoint https://<host> -variant b1 -token "$PUSH" \
  -exporters 3 -spans-per-second 15000 -duration 60s \
  -compression gzip -batch-size 8192 -queue-size 131072
```

Defaults are unchanged from the stock SDK: `-compression none`,
`-batch-size 0` and `-queue-size 0` resolve to 512 and 2048. The JSON report
contains the **effective** values under `client_settings`, so a result can be
compared with the table above without guessing:

```json
"client_settings": {
  "compression": "gzip",
  "batch_size": 8192,
  "queue_size": 131072
}
```

The report also carries generated/exported counts, `exporter_errors`, latency
percentiles and `shutdown_ms`. A nonzero `generated - exported` is client-side
loss even when `exporter_errors` is zero.

## 180 s client-drop investigation (spec 033)

Staging measured the following with the stock OTel Go exporters and gzip:

| Run | Client settings | Generated | Exported | Client loss |
| --- | --- | --- | --- | --- |
| 60 s | gzip, BSP batch 8192, queue 131072 | — | — | **0.0 %** |
| 180 s | gzip, BSP batch 8192, queue 131072 | 8,060,850 | 7,267,044 | **≈ 10 %** |

`exporter_errors` stayed **0** and p50 export was **607 ms** in the 180 s run.
The SDK does not expose a queue-drop counter, so the loss is only visible as
`generated - exported`. A longer run worsens the queue: at 15k spans/s per
exporter the serial BatchSpanProcessor drains one export while the producer
keeps enqueueing; any export slower than the offered rate eventually overflows
the 131072-entry queue silently.

`burstel-loadgen` now reports the inference directly:

```bash
burstel-loadgen -endpoint https://<host> -variant b1 -token "$PUSH" \
  -exporters 6 -spans-per-second 7500 -duration 180s \
  -compression gzip -batch-size 8192 -queue-size 262144 \
  -shot-id <shot-id> -api-token "$BURSTEL_API_TOKEN"
```

The JSON report contains:

* `signals.*.dropped` / `drop_pct` (aggregate), and
* `per_exporter_signals[*].signals` (one host at a time), and
* `client_drop.per_exporter[*].samples` — a per-second series of
  `generated`/`exported`/`dropped` so a growing gap is visible before the run
  ends. `client_drop.method` is `inferred_generated_minus_exported`; the SDK
  exposes no queue counter.

### Documented 180 s settings target

For a 180 s run, keep the per-exporter offered rate at or below what one serial
exporter can drain, instead of only enlarging the queue:

* `OTEL_EXPORTER_OTLP_COMPRESSION=gzip`
* `OTEL_BSP_MAX_EXPORT_BATCH_SIZE=8192`
* `OTEL_BSP_MAX_QUEUE_SIZE=262144` (headroom for the bigger batch)
* **two exporters per host** for a 15k spans/s/host workload (each exporter
  carries 7.5k spans/s), or equivalently an explicit per-exporter rate limit.
* `TracerProvider.Shutdown` / `MeterProvider.Shutdown` /
  `LoggerProvider.Shutdown` timeout **≥ 20 s** (measured flush 6–15 s).

That combination is the target the staging operator must confirm with the
`client_drop` series above: **0 dropped for the full 180 s**. The previous
one-exporter-per-host, 131072-queue setting is not sufficient at 180 s even
though it passed at 60 s. Record the actual `client_settings`,
`per_exporter_signals` and `client_drop` block with the staging result; do not
infer delivery from a clean `exporter_errors` field.
