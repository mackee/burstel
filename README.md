# burstel

ISUCON の 1〜3 分の benchmark shot ごとに **traces / metrics / logs** を
OTLP/HTTP で全量収集し、DuckDB に保存して停止時に freeze する shot recorder。

これは **Phase 1** のローカル実装(Go 単一バイナリ)である。Cloudflare Containers /
R2 / MCP はまだ含まない。収集した DuckDB ファイルを後から SQL で分析するための
土台を提供する。

> **Phase 2** ([`cloudflare/`](./cloudflare)) は、この Phase 1 の Go 実装をそのまま
> コンテナとして動かし、Cloudflare Worker + per-shot `ShotDO`(SQLite)+ Container +
> R2 で shot を freeze する。詳細は [`cloudflare/README.md`](./cloudflare/README.md) を参照。
>
> **Phase 3** は同じ Worker に remote MCP (`shot_start` / `shot_stop` /
> `shot_query` / `shot_analyze` / `shot_compare`) と、R2 の freeze 済み snapshot を
> 読み取り専用 DuckDB で再解析する仕組みを追加する。記録コンテナが停止していても
> 解析できる。`internal/analysis` が SQL allowlist / リソース上限を強制する。
> 詳細は [`cloudflare/README.md`](./cloudflare/README.md) と
> [`cloudflare/docs/adr-001-analysis-query-safety.md`](./cloudflare/docs/adr-001-analysis-query-safety.md)。
>
> **Stable ingest (spec 006 / spec 013 Part A)** は、shot ごとに exporter を再設定する
> 従来フローをやめ、固定の `https://<host>/ingest` を使う。管理者が shot 作成時に
> **shot ごとの** push 専用トークン (24 時間、`POST /api/shots` または
> `POST /api/shots/{id}/ingest-token` で発行) を一度だけ開示し、トークンがどの shot に
> 届くかを決めるため複数 shot を同時収集できる。24 時間の期限は **トークン** のもので
> shot ではない。SQLite バックエンドの `IngestDO` が token の digest、shot registry、
> 同時 recording 上限 (`MAX_CONCURRENT_RECORDING`) を管理する。旧来の全社トークン
> `POST /api/ingest-token` は廃止され `410 Gone` を返す。詳細は
> [`cloudflare/README.md`](./cloudflare/README.md)。
>
> **Shared multi-user (spec 013 Part B)** は、1 つのデプロイを複数人で共有する。
> admin が `POST /api/tokens` で **人ごとの**トークン (scope `admin` | `member`) を
> 発行し、`member` は全 shot に対する操作権限 (作成/停止、per-shot token 発行、
> query/analyze/compare) を持つがレジストリ管理はできない。token は digest のみを
> SQLite の `AuthDO` に保存し、`GET /api/tokens` は値を返さない。`DELETE
> /api/tokens/{id}` で個人単位に即時失効できる。`owner` は帰属表示専用で認可境界では
> ない。MCP (`/mcp`) も admin|member で利用でき、レジストリ操作は admin 専用。
> 詳細は [`cloudflare/README.md`](./cloudflare/README.md)。

## できること

- OTLP/HTTP protobuf (`application/x-protobuf`, 任意の gzip) の 3 シグナルを受信
- 受け取ったリクエストの生 protobuf バイト列を **lossless** に DuckDB BLOB へ保存
- 分析用の `spans` / `log_records` / `metric_points` 射影テーブルを同時に保存
- `start` / `stop` は冪等、`stop` で drain → `CHECKPOINT` → サイズ/SHA-256 記録し freeze
- freeze 済み `telemetry.duckdb` を読み取り専用で再オープン可能
- Cloudflare ShotDO が将来呼ぶ想定の internal API を提供

## インストール

[GitHub Releases](https://github.com/mackee/burstel/releases) にビルド済みバイナリを置いている。

| アーカイブ | 中身 | 対応プラットフォーム |
| --- | --- | --- |
| `burstel_<tag>_<os>_<arch>.tar.gz` | `burstel`(recorder 本体)、`burstel-inspect`(freeze 済み shot の解析) | linux/amd64, linux/arm64, darwin/amd64, darwin/arm64 |
| `burstel-cli_<tag>_<os>_<arch>.{tar.gz,zip}` | `burstel-cli`(デプロイ済み Worker 用の管理 CLI) | linux / darwin / windows × amd64 / arm64 |

`burstel` は DuckDB を cgo で静的リンクしている。Linux 版は glibc 2.35 以降
(Ubuntu 22.04 / Debian 12 相当)と `libstdc++` が必要。`burstel-cli` は依存なしの
pure Go バイナリ。

```bash
TAG=v0.1.0   # 使いたいリリース
OS=linux     # linux | darwin
ARCH=amd64   # amd64 | arm64

# 単体 recorder + 解析ツール
curl -fsSLO "https://github.com/mackee/burstel/releases/download/${TAG}/burstel_${TAG}_${OS}_${ARCH}.tar.gz"
tar xzf "burstel_${TAG}_${OS}_${ARCH}.tar.gz"
install "burstel_${TAG}_${OS}_${ARCH}/burstel" "burstel_${TAG}_${OS}_${ARCH}/burstel-inspect" ~/.local/bin/

# 管理 CLI
curl -fsSLO "https://github.com/mackee/burstel/releases/download/${TAG}/burstel-cli_${TAG}_${OS}_${ARCH}.tar.gz"
tar xzf "burstel-cli_${TAG}_${OS}_${ARCH}.tar.gz"
install "burstel-cli_${TAG}_${OS}_${ARCH}/burstel-cli" ~/.local/bin/

burstel -version
burstel-cli version
```

private リポジトリの場合は `gh release download "$TAG" -R mackee/burstel` で取得する。
各アーカイブの SHA-256 は `checksums.txt` に入っている。

ソースからビルドする場合は Go 1.26 と C/C++ コンパイラ(cgo)が必要:

```bash
make build   # bin/ に burstel, burstel-inspect, burstel-cli, otlpsend, burstel-trial
```

### リリース手順(メンテナ向け)

`v*` タグを push すると [`.github/workflows/release.yml`](./.github/workflows/release.yml)
が各プラットフォームでビルドし、`checksums.txt` 付きで GitHub Release を作成する。

```bash
git tag v0.1.0
git push origin v0.1.0
```

## 使い方

### 1. 単体で使う(`burstel` + `burstel-inspect`)

Cloudflare 無しで、手元やベンチ対象サーバの隣で recorder を動かす使い方。
1 プロセスで 1 shot を記録し、`stop` で `telemetry.duckdb` を freeze する。

```bash
export BURSTEL_INTERNAL_TOKEN=$(openssl rand -hex 32)

# recorder を起動(OTLP/HTTP を :4318 で待ち受け)
burstel -addr :4318 -data-dir ./data

# 別ターミナル: shot を開始
curl -fsS -H "Authorization: Bearer $BURSTEL_INTERNAL_TOKEN" \
  -X POST -d '{"shot_id":"shot-1","metadata":{"commit":"abc123"}}' \
  http://localhost:4318/internal/start
```

計測対象アプリの OTel SDK を burstel に向けてからベンチマークを流す:

```bash
export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
export OTEL_EXPORTER_OTLP_ENDPOINT=http://<burstel のホスト>:4318
```

ベンチマークが終わったら stop して freeze し、成果物を取り出す:

```bash
curl -fsS -H "Authorization: Bearer $BURSTEL_INTERNAL_TOKEN" \
  -X POST -d '{"shot_id":"shot-1"}' http://localhost:4318/internal/stop

# freeze 済みファイルは ./data/shot-1/telemetry.duckdb にある(HTTP でも取得可)
curl -fsS -H "Authorization: Bearer $BURSTEL_INTERNAL_TOKEN" \
  http://localhost:4318/internal/artifact -o shot-1.duckdb
```

1 プロセスは 1 shot を想定しているので、次の shot は `-data-dir` を変えるか
burstel を再起動して記録する。

freeze 済み shot は `burstel-inspect` で解析・比較できる(p50/p95/p99、遅い span、
エラー、DB 操作、N+1 候補などを表示。2 つ渡すと差分も出す):

```bash
burstel-inspect -left shot-1.duckdb                       # 単体解析
burstel-inspect -left shot-1.duckdb -right shot-2.duckdb  # 2 shot 比較
burstel-inspect -left shot-1.duckdb -out result.json \
  -query "SELECT name, count(*) FROM spans GROUP BY 1 ORDER BY 2 DESC LIMIT 10"
```

DuckDB CLI で直接 SQL を書いてもよい(`duckdb -readonly shot-1.duckdb`)。
クエリ例は [`examples/queries.sql`](./examples/queries.sql)、スキーマは後述。

### 2. デプロイ済み Worker を操作する(`burstel-cli`)

Cloudflare 版([`cloudflare/README.md`](./cloudflare/README.md))をデプロイした後、
shot の開始/停止、24 時間有効な push 専用 ingest トークンの発行、人ごとの
`admin`/`member` トークン管理、freeze 済み snapshot の query / analyze / compare を
`burstel-cli` で行う。

```bash
export BURSTEL_BASE_URL=https://burstel.<account>.workers.dev
# 管理者トークン: --token > $BURSTEL_API_TOKEN > --token-file
#   > ./.secrets/BURSTEL_API_TOKEN > ~/.config/burstel/token の順で解決
mkdir -p ~/.config/burstel && printf '%s' "<admin token>" > ~/.config/burstel/token
chmod 600 ~/.config/burstel/token

burstel-cli health

# shot を作成し、その shot 専用の push トークンを発行する
SHOT=$(burstel-cli shot start --metadata commit=abc123 --metadata scenario=bench)
SHOT_ID=$(printf '%s' "$SHOT" | jq -r .shot_id)
PUSH=$(printf '%s' "$SHOT" | jq -r .ingest_token)
export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
export OTEL_EXPORTER_OTLP_ENDPOINT=$BURSTEL_BASE_URL/ingest
export OTEL_EXPORTER_OTLP_HEADERS="X-Burstel-Token=$PUSH"

# ベンチマーク実行後、freeze する
burstel-cli shot stop "$SHOT_ID"

# freeze 済み snapshot の解析
burstel-cli shot get "$SHOT_ID"
burstel-cli shot schema "$SHOT_ID"
burstel-cli shot analyze "$SHOT_ID" --top-n 10
burstel-cli shot query "$SHOT_ID" \
  --sql "SELECT service_name, count(*) AS n FROM spans GROUP BY 1 ORDER BY n DESC" --max-rows 20
burstel-cli shot compare "$SHOT_ID" --other <other-shot-id> --top-n 10

# 人ごとのトークンを発行・一覧・失効 (admin のみ)
burstel-cli token mint --scope member --owner alice --out .secrets/member-alice
burstel-cli token list
burstel-cli token revoke <token-id>
```

全コマンドは `burstel-cli help` で確認できる。

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
| `cmd/burstel-inspect` | freeze 済み shot を独立プロセスで restore → analyze / compare する CLI |
| `cmd/burstel-cli` | デプロイ済み Worker 用の管理 CLI(shot / ingest token / 解析) |
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

freeze 済み snapshot の解析用 (Phase 3、Worker/DO 経由のみ):

| Method | Path | 説明 |
| --- | --- | --- |
| POST | `/internal/restore` | freeze 済み `telemetry.duckdb` を stream 受信し size/SHA-256 検証して read-only で開く |
| POST | `/internal/query` | allowlist 済み単一 SELECT を実行 (行数/時間/メモリ上限) |
| POST | `/internal/analyze` | p50/p95/p99、slow span、error、DB op、log 相関、metric の SQL 集計 |
| POST | `/internal/compare` | 2 shot の SQL 集計差分 (sample size / unit / disclosure 付き) |
| GET | `/internal/schema` | 問い合わせ可能なテーブル/カラムと安全ポリシー |
| POST | `/internal/release` | 解析セッションを破棄 |

`BURSTEL_INTERNAL_TOKEN` を設定すると `/internal/*` は
`Authorization: Bearer <token>` を要求する(OTLP パスは対象外)。本番では
**必ず非空で、管理者トークンとは別の値**を設定すること。未設定の場合
`/internal/*` は fail closed (503) となる。信頼できるローカル開発に限り
`BURSTEL_DEV_INSECURE_AUTH=1` (または `-dev-insecure-auth`) を明示すると
トークン無しで実行できる。既定では無効。

## 開発用クイックスタート(ソースから)

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

### Standalone field trial (Phase 4)

実 OTel Go SDK で traces / metrics / 相関 logs を OTLP/HTTP で送る
ISUCON 風ワークロード(`cmd/burstel-trial`)と、freeze 済み DuckDB を
独立プロセスで restore → query/analyze/compare する CLI
(`cmd/burstel-inspect`)を使い、Cloudflare 無しで recorder + analyzer を
実測検証できる。

```bash
scripts/local-trial.sh --smoke        # 10s + 20s の短縮スモーク
scripts/local-trial.sh                # 60s baseline + 180s regressed
```

計測値・限界・判定は [`docs/standalone-field-trial.md`](./docs/standalone-field-trial.md)
を参照。すべて loopback + 一時ディレクトリで動作し、Cloudflare/R2/Docker/root を
必要としない。

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

実 image の build と smoke は CI ([`ci/github-actions/container-image.yml`](./ci/github-actions/container-image.yml)) が
Ubuntu Docker ランナーで検証する(`docker buildx build --platform linux/amd64`
→ `scripts/container-smoke.sh`)。`.github/workflows/` への install は
`scripts/install-github-workflow.sh`(workflows 権限が必要)を参照。Cloudflare へのデプロイ手順・read-only
preflight・bounded E2E は [`docs/cloudflare-deploy.md`](./docs/cloudflare-deploy.md)
を参照。

## 設定

| 環境変数 | フラグ | 既定 |
| --- | --- | --- |
| `BURSTEL_ADDR` | `-addr` | `:4318` |
| `BURSTEL_DATA_DIR` | `-data-dir` | `./data` |
| `BURSTEL_INTERNAL_TOKEN` | `-internal-token` | (空 = `/internal/*` は 503 で fail closed) |
| `BURSTEL_DEV_INSECURE_AUTH` | `-dev-insecure-auth` | `false` (明示時のみトークン無しを許可) |
| `BURSTEL_MAX_REQUEST_BYTES` | `-max-request-bytes` | `67108864` (64 MiB) |
| `BURSTEL_MAX_INFLIGHT` | `-max-inflight` | `8` |
| `BURSTEL_WRITE_TIMEOUT` | `-write-timeout` | `30s` |
| `BURSTEL_ANALYSIS_TIMEOUT` | — | `15s` (read-only analysis query/restore) |

## Limitations / Non-goals (Phase 1)

- Phase 1 単体では Cloudflare Containers / R2 / MCP server / dashboard は未実装
  (Phase 2 で Container/R2、Phase 3 で MCP と R2 snapshot 解析を実装)。
- OTLP/gRPC・OTLP/JSON は未対応(HTTP protobuf のみ)。
- profiles は対象外。
- exact-once 配送は保証しない(上記参照)。
- DuckDB Appender によるバルク書込最適化は未実装(安全性優先)。
- プロセス再起動をまたぐ shot の resume は未対応(1 プロセス 1 shot を想定)。
