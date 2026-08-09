# ClickHouse API 固定时间回放验证

本验证用于本地 MySQL 数据不是最新时，对两个数据库重放同一个历史时间点。`as_of_ms` 是 Unix 毫秒时间戳；所有时间序列只读取该时间点之前的数据，首页、结构评分和交易机会的 5 分钟锚点以及缓存键也由它决定。

## 命令

推荐让脚本自动从两边的共同数据范围选择时间点：

```powershell
python scripts/verify_api_readonly.py `
  --mysql-host 10.0.0.128:13306 `
  --mysql-database coinx `
  --mysql-user root `
  --mysql-password '<MYSQL_PASSWORD>' `
  --clickhouse-url http://10.0.0.128:8123 `
  --clickhouse-database coinx `
  --clickhouse-user root `
  --clickhouse-password '<CLICKHOUSE_PASSWORD>' `
  --compare-clickhouse `
  --auto-as-of `
  --report-file data/clickhouse-dual-backend-report.json
```

也可以手工指定历史时间点，例如 `1785593537000`：

```powershell
python scripts/verify_api_readonly.py `
  --mysql-host 10.0.0.128:13306 `
  --mysql-database coinx `
  --mysql-user root `
  --mysql-password '<MYSQL_PASSWORD>' `
  --clickhouse-url http://10.0.0.128:8123 `
  --clickhouse-database coinx `
  --clickhouse-user root `
  --clickhouse-password '<CLICKHOUSE_PASSWORD>' `
  --compare-clickhouse `
  --as-of-ms 1785593537000 `
  --report-file data/clickhouse-api-asof.json
```

脚本启动时只执行两库的 `MIN/MAX` 查询，自动模式取所有非空表的共同最大上界，再向下对齐到 5 分钟；实际接口查询统一追加 `as_of_ms`，不会把本地缺少的最新数据当成迁移错误。脚本只调用 GET 接口，不调用 refresh/write 接口。报告的接口检查表会同时列出 MySQL API 与 ClickHouse 等价读取的返回摘要（状态、数据量、时间锚点、缓存时间等），完整 JSON 和逐字段差异保存在配套 JSON 文件中；无法构造 ClickHouse 等价查询的接口会标为 `NOT_COMPARABLE`，不会被误报成通过。

## 当前可比接口

验证器已覆盖当前只读行情接口，包括：

- 资金费率分页、异常资金费率、BTCUSDT/ETHUSDT 资金费率历史
- 行情排行
- 首页冷启动、缓存命中、强制绕过缓存
- 合约详情、合约详情图表、结构评分、交易机会
- 市场结构评分、交易机会列表及缓存重复请求

复合接口使用临时 SQLite 仅承载 ClickHouse 拉取的数据，复用正式业务聚合函数；不会修改生产连接或写入任何业务表。图表使用直接 ClickHouse 等价查询。无法构造等价查询的接口会标为 `NOT_COMPARABLE`，不会被误报为通过。

## 最近一次闭环结果

2026-08-09 使用自动共同时间模式完成真实两库验证，并执行回滚演练：

- MySQL：`10.0.0.128:13306/coinx`
- ClickHouse：`http://10.0.0.128:8123/coinx`
- 共同原始上界：`1785593537002`
- 统一回放点：`1785593400000`（向下对齐到 5 分钟）
- HTTP 接口：24 项检查，0 个失败
- 缓存检查：6 项通过，0 个失败
- 逐字段比较：12 个接口全部 `PASS`，0 个差异，0 个 `NOT_COMPARABLE`
- ClickHouse 写保护：4/4 返回 503
- 回滚演练：MySQL 行数不变、ClickHouse 行数不变、最终端口不可访问、流量归零
- 比较门禁：通过

机器可读 JSON 和详细 Markdown 报告由脚本写入系统临时目录，运行结束时会打印完整路径，当前结果为：

```text
C:\Users\38963\AppData\Local\Temp\coinx-dual-backend-current.json
C:\Users\38963\AppData\Local\Temp\coinx-dual-backend-current.md
```

最新回放 `content_warnings=0`；首页核心序列、结构评分和交易机会的完整性判断均通过。

## 时间点选择

用两边的 `MAX` 时间边界取交集，并优先选择已闭合的 5 分钟点。不要使用当前时间直接比较，否则本地库缺少最新数据会把“数据范围差异”误判为迁移错误。

性能和回滚的详细证据分别见：

- `C:\Users\38963\AppData\Local\Temp\coinx-dual-backend-benchmark-current.md`
- `C:\Users\38963\AppData\Local\Temp\coinx-dual-backend-current.md`
