# 行情写入 ClickHouse 迁移

## 目标

`DB_TYPE` 保持 `mysql`。控制面、任务运行记录和告警状态继续由 MySQL 事务管理；六张行情表由 ClickHouse 读写，MySQL 中的六张表作为只读历史副本保留：

- `market_klines`
- `market_open_interest_hist`
- `market_taker_buy_sell_vol`
- `market_funding_rate`
- `market_tickers`
- `market_snapshots`

应用使用 `READ_BACKEND=clickhouse` 读取行情，使用 `MARKET_WRITE_BACKEND=clickhouse` 写入行情。ClickHouse 写失败会抛出异常，当前采集批次失败，不能静默写回 MySQL。

## 配置

```dotenv
DB_TYPE=mysql
MARKET_BACKEND=clickhouse
CLICKHOUSE_URL=http://10.0.0.128:8123
CLICKHOUSE_DATABASE=coinx
CLICKHOUSE_USER=root
CLICKHOUSE_PASSWORD=<password>
CLICKHOUSE_WRITE_TIMEOUT_SECONDS=120
CLICKHOUSE_WRITE_RETRIES=3
CLICKHOUSE_WRITE_BATCH_SIZE=500
```

启动调度器时会检查六张表、`ReplacingMergeTree` 引擎、排序键和关键 `DateTime64(3, 'Asia/Shanghai')` 列。旧的 `MergeTree` 表不会被应用自动使用。

## 一次性切换

1. 停止调度器和所有采集进程，记录 MySQL 六张表的行数、时间边界和固定样本摘要。
2. 确认 CK 目标表已经由 `sql/schema_clickhouse.sql` 创建，并运行可断点导入：

```powershell
python scripts/import_mysql_to_clickhouse.py `
  --clickhouse-url http://10.0.0.128:8123 `
  --clickhouse-database coinx `
  --clickhouse-user root `
  --clickhouse-password $env:CLICKHOUSE_PASSWORD `
  --mysql-host 10.0.0.128:13306 `
  --mysql-database coinx `
  --mysql-user root `
  --mysql-password $env:MYSQL_PASSWORD
```

3. 在仍停写的窗口执行 `_v2` 准备和原子切换。脚本只重建 `market_tickers`、`market_snapshots` 两张需要改变引擎的表；其余四张表直接复用并由预检验证：

```powershell
python scripts/prepare_clickhouse_market_write.py `
  --clickhouse-url http://10.0.0.128:8123 `
  --clickhouse-database coinx `
  --clickhouse-user root `
  --clickhouse-password $env:CLICKHOUSE_PASSWORD
```

脚本先回填 `_v2`，再用一次 `RENAME TABLE` 将两张表切换，旧表保留为带时间后缀的备份。重复执行会先清理 staging 表，不会覆盖旧备份。

4. 启动新配置应用。启动预检必须返回 `healthy=true`；否则停止采集并修复 schema。
5. 手动执行一次六类采集，确认 CK `system.query_log` 出现 `INSERT`，并确认 MySQL 六张表行数没有增加。
6. 开启调度器，观察至少一个完整调度周期。日志会记录表名、批次、行数、重试次数、耗时、`query_id` 和错误原因。

## 读取与幂等

ClickHouse 查询统一经过行情 Repository；`ReplacingMergeTree` 表使用 `FINAL`，避免后台合并完成前暴露重复逻辑行。四张序列表按业务键覆盖，ticker 按 `(symbol, close_time)` 覆盖，snapshot 按 `(symbol, snapshot_time, batch_id)` 覆盖。进程内写锁替代 MySQL named lock，跨表采集不使用分布式事务；任一序列失败时任务失败，下一轮按 CK 缺口补采。

## 回滚

运行时不启用自动 MySQL fallback。紧急回滚必须停止采集，先用反向补偿脚本把 CK 最近窗口写回 MySQL，再把 `MARKET_WRITE_BACKEND=mysql`、`READ_BACKEND=mysql` 后重启：

```powershell
python scripts/export_clickhouse_market_to_mysql.py `
  --clickhouse-url http://10.0.0.128:8123 `
  --clickhouse-database coinx `
  --clickhouse-user root `
  --clickhouse-password $env:CLICKHOUSE_PASSWORD `
  --mysql-host 10.0.0.128:13306 `
  --mysql-database coinx `
  --mysql-user root `
  --mysql-password $env:MYSQL_PASSWORD `
  --since-ms <window_start_ms> --until-ms <window_end_ms> --apply
```

该脚本默认 dry-run；`--apply` 前必须确认窗口和备份。回滚后检查 MySQL/CK 时间水位，再恢复调度器。

## 验收门禁

- 六张 CK 表预检通过，最新时间持续推进。
- MySQL 六张行情表行数和更新时间保持不变。
- 固定 `as_of_ms` 的接口 JSON 与迁移前 MySQL 完全一致。
- 写失败进入调度任务失败记录并在下一轮重试。
- 告警规则、状态 CAS、冷却和发送记录仍在 MySQL 同一事务中提交。
