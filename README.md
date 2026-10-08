# burstel

ISUCON向け短時間OTel shot recorder/analyzer。

1〜3分のbenchmark shotごとにtraces/metrics/logsをOTLP/HTTPで全量収集し、Cloudflare Containers上のDuckDBへ格納、停止時にcheckpointしてR2へfreezeする。後からMCP経由でshot単位のSQL分析・ボトルネック解析・比較を行う。

仕様は itako の `burstel` project で管理する。
