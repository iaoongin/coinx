# ClickHouse API 只读验证闭环报告

验证日期：2026-08-08 13:08（Asia/Shanghai）

验证目标：在不修改写入逻辑、不调用 refresh 接口的前提下，验证使用 6 张行情历史表的查询接口，并确认进程内缓存不会把旧结果误判为新结果。

## 连接与范围

- MySQL：本地测试实例 `10.0.0.128:13306/coinx`
- ClickHouse：`http://10.0.0.128:8123/coinx`
- 调度器：关闭
- Web 鉴权：测试环境关闭
- ClickHouse shadow：开启，只读异步对比
- 写入/刷新接口：未调用

## 接口结果

15 个 GET 请求全部返回 HTTP 200，响应 JSON 的 `status` 均为 `success`。

| 接口 | 结果 | 耗时 |
|---|---:|---:|
| `/api/funding-rate?page=1&page_size=20` | PASS | 137 ms |
| `/api/funding-rate/abnormal` | PASS | 112.69 s |
| `/api/funding-rate/history/BTCUSDT?hours=24` | PASS | 8 ms |
| `/api/funding-rate/history/ETHUSDT?hours=24` | PASS | 5 ms |
| `/api/market-rank?type=price_change&direction=down&limit=20` | PASS | 24 ms |
| `/api/coins?nocache=1` 首次请求 | PASS | 4.76 s |
| `/api/coins` 缓存请求 | PASS | 38 ms |
| `/api/coins?nocache=1` 强制绕过 | PASS | 2.05 s |
| `/api/coin-detail/BTCUSDT` | PASS | 238 ms |
| `/api/coin-detail/BTCUSDT/series?range=24h` | PASS | 257 ms |
| `/api/coin-detail/BTCUSDT/structure-score` | PASS | 1.67 s |
| `/api/coin-detail/BTCUSDT/trade-opportunity` | PASS | 107 ms |
| `/api/market-structure-score?symbol=BTCUSDT&limit=1` | PASS | 78 ms |
| `/api/trade-opportunities?scope=all&limit=1` | PASS | 109 ms |
| `/api/trade-opportunities?scope=all&limit=1` 重复请求 | PASS | 11 ms |

## 缓存证据

1. 首页第一次请求写入缓存对象，缓存对象 ID 为 `2782090548672`。
2. 第二次不带 `nocache` 返回相同对象 ID，响应内容完全相同，确认命中缓存。
3. 第三次带 `nocache=1` 使用新对象 ID `2782068529088`，确认绕过并替换缓存。
4. 交易机会缓存条目数保持为 1，重复请求复用了同一个缓存对象。

缓存检查：3 项通过，0 项失败。

## ClickHouse Shadow

- Shadow 事件：0
- 资金费率和行情 shadow 查询均未产生 mismatch/error。
- 验证过程中发现零值归一化问题：MySQL 业务代码原先用真假判断，把 `0` 转成 `None`；已改为 `is not None`。
- 回归检查：`MUUSDT`、`QQQUSDT` 的 `funding_rate=0.0` 已正确保留。

## 需要处理的问题

`/api/funding-rate/abnormal` 功能通过，但耗时约 118 秒。该接口当前在 MySQL 上按币种求最新记录后再筛选，存在全表聚合性能风险。该问题不影响本次 HTTP 正确性，但在正式切换读取前必须优化或改为 ClickHouse 查询。

另外两个接口虽然返回 HTTP 200，但业务内容需要继续处理：

- `/api/coins` 返回 36 个币种，但 `homepage_complete=false`，说明首页数据仍有缺口。
- `/api/market-structure-score?symbol=BTCUSDT&limit=1` 返回 `data=[]`、`cache_update_time=null`，只能证明接口可用，不能证明评分数据完整。

## 迁移门禁结论

- 查询接口可用性：通过
- 首页/交易机会缓存行为：通过
- 基础 MySQL/ClickHouse 数据一致性：22 项检查通过
- Shadow 异步隔离：通过
- 异常资金费率性能：不通过，需优化
- 首页数据完整性：不通过，`homepage_complete=false`
- 结构评分数据完整性：不通过，本次返回空数据
- 所有复合接口逐字段 MySQL/ClickHouse 对比：尚未完成
- 写入双写/正式切读：暂不允许

## 可复现命令

```powershell
$env:MYSQL_PASSWORD = "<mysql-password>"
$env:CLICKHOUSE_PASSWORD = "<clickhouse-password>"

python scripts/verify_api_readonly.py `
  --mysql-host 10.0.0.128:13306 `
  --mysql-database coinx `
  --mysql-user root `
  --mysql-password $env:MYSQL_PASSWORD `
  --clickhouse-url http://10.0.0.128:8123 `
  --clickhouse-database coinx `
  --clickhouse-user root `
  --clickhouse-password $env:CLICKHOUSE_PASSWORD `
  --shadow
```

完整机器报告：`clickhouse-api-readonly-report.json` 和 `clickhouse-api-readonly-report.md`，由同一脚本生成。
