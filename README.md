# burstel

ISUCON の 1〜3 分の benchmark shot ごとに **traces / metrics / logs** を
OTLP/HTTP で全量収集し、DuckDB に保存して停止時に freeze する shot recorder。

これは **Phase 1** のローカル実装(Go 単一バイナリ)である。Cloudflare Containers /
R2 / MCP はまだ含まない。収集した DuckDB ファイルを後から SQL で分析するための
土台を提供する。

> **Phase 2** ([`cloudflare/`](./cloudflare)) は、この Phase 1 の Go 実装をそのまま
> コンテナとして動かし、Cloudflare Worker + per-shot `ShotDO`(SQLite)+ Container +
> R2 で shot を freeze する。詳細は [`cloudflare/README.md`](./cloudflare/README.md) を参照。

## できること

- OTLP/HTTP protobuf (`application/x-protobuf`, 任意の gzip) の 3 シグナルを受信
- 受け取ったリクエストの生 protobuf バイト列を **lossless** に DuckDB BLOB へ保存
- 分析用の `spans` / `log_records` / `metric_points` 射影テーブルを同時に保存
- `start` / `stop` は冪等、`stop` で drain → `CHECKPOINT` → サイズ/SHA-256 記録し freeze
- freeze 済み `telemetry.duckdb` を読み取り専用で再オープン可能
- Cloudflare ShotDO が将来呼ぶ想定の internal API を提供

## アーキテクチャ

```
                 OTLP/HTTP (protobuf, gzip)
 exporter ───────────────────────────────► POST /v1/{traces,metrics,logs}
                                                  │
                                            internal/server  (HTTP, auth, limits)
                                                  │
                                            internal/otlp    (decode → 射影)
                                                  │
 ShotDO ── POST /internal/start ──────────► internal/recorder (lifecycle)
        ── POST /internal/stop  ──────────►        │
        ◄─ GET  /internal/artifact ───────         ▼
        ◄─ GET  /internal/metadata ────  internal/storage (DuckDB, single writer)
```

主要パッケージ:

| パッケージ | 役割 |
| --- | --- |
| `cmd/burstel` | 設定読み込み、HTTP サーバ起動、graceful shutdown |
| `cmd/otlpsend` | ローカル動作確認用の小さな OTLP 送信クライアント |
| `internal/config` | フラグ/環境変数による設定 |
| `internal/otlp` | 公式 OTLP protobuf を decode し、射影行を生成 |
| `internal/storage` | DuckDB スキーマ、生バッチ + 射影のトランザクション書込、CHECKPOINT/close |
| `internal/recorder` | `created → recording → stopping → frozen`(失敗時 `failed`)の状態機械 |
| `internal/server` | ルーティング、internal 認証、サイズ制限、エラー→HTTP 変換 |
| `internal/fixtures` | テスト/デモ用の本物の protobuf リクエスト生成 |

## HTTP API

### OTLP ingest (認証なし。信頼境界は Worker/DO 側)

| Method | Path | 説明 |
| --- | --- | --- |
| POST | `/v1/traces` | `ExportTraceServiceRequest` |
| POST | `/v1/metrics` | `ExportMetricsServiceRequest` |
| POST | `/v1/logs` | `ExportLogsServiceRequest` |

成功時は対応する `Export*ServiceResponse`(protobuf)を `200` で返す。
`Content-Encoding: gzip` に対応。

### internal (信頼された Worker/DO からのみ到達させる)

| Method | Path | 説明 |
| --- | --- | --- |
| GET | `/healthz` | 常に `200` |
| GET | `/readyz` | 受付可能なら `200`、`failed` なら `503` |
| POST | `/internal/start` | `{"shot_id": "...", "metadata": {...}}`。同一入力は冪等 |
| POST | `/internal/stop` | `{"shot_id": "..."}`。drain → checkpoint → freeze。冪等 |
| GET | `/internal/artifact` | freeze 済み `telemetry.duckdb`(`Content-Length` 付き)。stop 前は `409` |
| GET | `/internal/metadata` | shot メタデータ / カウンタ / schema version / size / checksum |

`BURSTEL_INTERNAL_TOKEN` を設定すると `/internal/*` は
`Authorization: Bearer <token>` を要求する(OTLP パスは対象外)。

## クイックスタート

必要なもの: Go 1.26 と C コンパイラ(cgo で DuckDB をリンクするため)、`gcc`/`g++`。

```bash
go build ./cmd/burstel ./cmd/otlpsend

# ターミナル A
BURSTEL_INTERNAL_TOKEN=dev-secret ./burstel -addr :4318 -data-dir ./data

# ターミナル B: start → 3 シグナル送信 → stop を一括実行
./otlpsend -url http://localhost:4318 -shot demo-1 -token dev-secret

# 成果物
curl -s -H 'Authorization: Bearer dev-secret' \
  http://localhost:4318/internal/metadata | jq .

curl -s -H 'Authorization: Bearer dev-secret' \
  http://localhost:4318/internal/artifact -o /tmp/demo.duckdb

# 読み取り専用で分析
duckdb -readonly /tmp/demo.duckdb \
  "SELECT name, end_time_unix_nano-start_time_unix_nano AS dur FROM spans ORDER BY dur DESC LIMIT 5;"
```

`examples/queries.sql` に有用なクエリ例がある。

### OTel exporter 設定

```bash
export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
# シグナル別にする場合: OTEL_EXPORTER_OTLP_TRACES_ENDPOINT 等
```

Burstel はパス別エンドポイント(`/v1/traces` など)を提供するため、
OTel SDK の標準 HTTP exporter がそのまま利用できる。

## DuckDB スキーマ (schema_version = 1)

`internal/storage/schema.go` が正。主なテーブル:

- `schema_meta(key, value)` — `schema_version` 等。
- `otlp_batches(shot_id, ordinal, signal, received_at, content_encoding,
  record_count, payload_length, resource_summary, payload BLOB)`
  — **生の source of truth**。`payload` は gzip を解凍した
  `Export*ServiceRequest` の protobuf バイト列をそのまま保存する。
  未知フィールド・exemplar・exponential histogram 等も完全に復元できる。
- `spans(...)` — trace/span/parent ID(hex)、name、kind、status、
  start/end ナノ秒(BIGINT で精度損失なし)、service/scope、
  attributes を JSON 文字列で保持。
- `log_records(...)` — trace/span ID、severity、body(JSON)、attributes。
- `metric_points(...)` — metric 名/type/unit、value(double/int)、
  sum の monotonic/temporality、histogram の count/sum/min/max/bucket を保持。
  exponential histogram / summary は `detail` JSON に格納。

ナノ秒タイムスタンプは `BIGINT` で持つため、JS の number(double) を経由しても
精度が失われない。

## Ingest セマンティクスと耐久性

- 受理は **all-or-nothing**。partial success は返さない。
- `200` は「生バッチが DuckDB トランザクションとしてコミットされた後」にのみ返す
  (キューに積んだだけで ack しない)。`stop` の `CHECKPOINT` は、この時点で
  WAL にコミット済みの内容を本体ファイルへ反映する。
- 不正な protobuf / gzip → `400`、未対応 `Content-Encoding` / `Content-Type` → `415`、
  サイズ超過 → `413`、recording 中でない / stopping / storage エラー → `503`
  (exporter が再試行できる)。
- **exact-once は保証しない。** HTTP 応答が曖昧なまま exporter が再送した場合、
  同じ論理レコードが複数回保存され得る。受理したリクエストは必ず完全な形で保持するが、
  ネットワーク断を越えた losslessness は主張しない。
- メモリ/リクエストサイズ(`BURSTEL_MAX_REQUEST_BYTES`)、同時書込数
  (`BURSTEL_MAX_INFLIGHT`)、書込タイムアウト(`BURSTEL_WRITE_TIMEOUT`)で上限を設ける。

## ライフサイクル

```
created ──start──► recording ──stop──► stopping ──checkpoint+close──► frozen
                                     └── I/O error ──► failed(ファイルは削除しない)
```

- `start` は同一 `shot_id` + metadata なら冪等、別 shot の recording 中は `409`。
- `stop` は新規 ingest を止め、in-flight 書込の完了を待ち(RWMutex)、
  `CHECKPOINT` → `close` → サイズ/SHA-256 を記録して `frozen` にする。
  I/O エラー時は `frozen` にせず、ファイルも削除しない。
- 冪等な 2 回目の `stop` は同じ size/SHA-256 を返す。
- データ配置: `<data-dir>/<shot_id>/telemetry.duckdb`(ファイル名は固定)。

## セキュリティ

- `/internal/*` は共有 bearer token による defense-in-depth を提供する。
  加えて Worker/DO のルーティング境界・ネットワークポリシーで保護すること。
- ログに OTLP ペイロードを出さない(サーバはボディを記録しない)。
- 本リポジトリは GitHub secrets / パスワードを保持しない。

## 運用上の注意

- **DuckDB は単一 writer。** burstel は `SetMaxOpenConns(1)` で直列化している。
  freeze 後の読み取りは読み取り専用で開くこと(`?access_mode=read_only`)。
- 高トラフィック時は disk 使用量が増える。`BURSTEL_MAX_REQUEST_BYTES` /
  `BURSTEL_MAX_INFLIGHT` と exporter バッチ設定で調整する。
- 現状の射影書込は 1 行ずつ prepared statement で行う。DuckDB Appender による
  高速化は今後の課題(下記 Limitations)。

## テスト

```bash
go test ./...
```

含まれる検証:

- 3 シグナルの本物の protobuf fixture(trace/span ID、status、attributes、
  histogram exemplar、gzip)を送信し、`stop` 後に freeze 済みファイルを
  読み取り専用で再オープン。
- 保存された各生バッチを `Export*ServiceRequest` に decode し、元リクエストと
  `proto.Equal` で一致することを確認。
- `spans` / `log_records` / `metric_points` の射影行数、schema_version、
  遅い span の分析クエリを検証。
- malformed protobuf → `400`、未対応 Content-Type → `415`。
- 並行 ingest と `stop` の race で、`200` を返した件数と freeze 後の
  バッチ数が一致すること(drain で消失しないこと)を検証。

## Docker

```bash
docker build -t burstel:dev .
docker run --rm -p 4318:4318 -v "$PWD/data:/data" \
  -e BURSTEL_INTERNAL_TOKEN=dev-secret burstel:dev
```

`Dockerfile` は `golang:1.26-bookworm` で cgo ビルドし、`debian:bookworm-slim`
ランタイムに配置する(linux/amd64 想定)。cgo のため g++ が必要。

## 設定

| 環境変数 | フラグ | 既定 |
| --- | --- | --- |
| `BURSTEL_ADDR` | `-addr` | `:4318` |
| `BURSTEL_DATA_DIR` | `-data-dir` | `./data` |
| `BURSTEL_INTERNAL_TOKEN` | `-internal-token` | (空 = 認証なし) |
| `BURSTEL_MAX_REQUEST_BYTES` | `-max-request-bytes` | `67108864` (64 MiB) |
| `BURSTEL_MAX_INFLIGHT` | `-max-inflight` | `8` |
| `BURSTEL_WRITE_TIMEOUT` | `-write-timeout` | `30s` |

## Limitations / Non-goals (Phase 1)

- Cloudflare Containers / R2 / MCP server / dashboard は未実装。
- OTLP/gRPC・OTLP/JSON は未対応(HTTP protobuf のみ)。
- profiles は対象外。
- exact-once 配送は保証しない(上記参照)。
- DuckDB Appender によるバルク書込最適化は未実装(安全性優先)。
- プロセス再起動をまたぐ shot の resume は未対応(1 プロセス 1 shot を想定)。
