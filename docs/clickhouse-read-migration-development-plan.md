# CoinX MySQL -> ClickHouse 读链路迁移开发计划

> 本文记录只读迁移阶段；行情写入切换后的最终职责和操作步骤以 [`clickhouse-market-write-migration.md`](clickhouse-market-write-migration.md) 为准。当前 `MARKET_WRITE_BACKEND=clickhouse` 时，六张行情表由 ClickHouse 写入，控制面和告警状态仍由 MySQL 写入。

版本：v1.3  
日期：2026-08-09  
适用范围：当前分支、本地双实例验证、后续生产读流量切换

## 当前执行状态

本计划已进入“实现完成、验收门禁”阶段。当前采用同一开发分支、同一套 Repository，通过进程级 `READ_BACKEND` 启动两个实例验证；不维护两套长期分支，也不在业务请求头中增加数据库分叉逻辑。

- 读链路、ClickHouse 客户端、Repository 分派、双实例启动、缓存验证和只读保护已实现。
- 定向回归已通过：`tests/test_homepage_series_repository.py` 为 `30 passed, 9 skipped`，`tests/test_exchange_repair.py` 为 `28 passed`，资金费率 Repository 为 `18 passed`。
- 全量 `tests/` 在计划指定的 `SCHEDULER_ENABLED=false`、`WEB_AUTH_DISABLED=false` 环境下最新为 `391 passed, 13 skipped, 3 warnings`；运行时使用 `PYTHONDONTWRITEBYTECODE=1` 绕过工作区现有字节码目录权限，不影响代码验证。
- Gate 网络耗时诊断用例默认跳过，显式设置 `RUN_GATE_DIAGNOSTICS=1` 才运行；它用于观测外部 API 延迟，不作为默认回归门禁。
- 底层只读 Repository 校验已通过：22/22，六张表行数与时间边界一致，BTCUSDT/ETHUSDT 序列检查失败数为 0。
- 本次真实双库固定时间回放已完成：24 个 API 检查、6 个缓存检查、12 个完整 JSON 比较和 4 个写入保护检查全部通过；最新报告内容告警为 0。回滚演练也已通过，MySQL/ClickHouse 行数保持不变且 ClickHouse 流量归零。

本轮开发验收门禁已全部关闭。生产正式切读仍需按部署窗口执行阶段 6 的灰度观察；在此之前可以随时保持 `READ_BACKEND=mysql`，不影响已完成的 ClickHouse 只读验证。

### 本轮执行顺序

| 优先级 | 工作项 | 当前状态 | 交付物 | 硬验收 |
|---|---|---|---|---|
| P0 | Repository 读链路和只读保护 | 已完成 | `src/coinx/read_backend.py`、各行情 Repository、只读保护测试 | 所有目标 GET 经过 Repository；ClickHouse 写请求统一 503 |
| P0 | 固定 `as_of_ms` 的双库完整 JSON 回放 | 已完成 | `scripts/verify_dual_backend_api.py`、JSON/Markdown 报告 | API 24/24、JSON diff=0、写保护 4/4 |
| P0 | 进程级缓存共享和键隔离 | 已完成 | 共享缓存模块及单元测试 | 同进程跨请求命中；`as_of_ms`、后端、参数互相隔离 |
| P0 | 正式性能基线 | 已完成 | `scripts/benchmark_dual_backend_api.py`、性能报告 | 12/12 接口各运行 10 次；ClickHouse p95 <= MySQL p95 * 1.5，错误率为 0 |
| P1 | 首页冷启动外部支持查询优化 | 已完成 | 支持状态缓存预热、回归报告 | ClickHouse 分支逐交易所预热一次；双库 JSON 仍 0 差异 |
| P1 | ClickHouse 故障和回滚演练 | 已完成 | `--exercise-rollback` 结果、回滚报告 | MySQL 健康；ClickHouse 端口/流量归零；数据未删除 |
| P1 | 本地灰度观察 | 已完成 | 双库回放、性能和回滚报告 | 当前测试环境无未解释差异或关键错误 |

生产灰度观察仍是部署操作，不在本地代码验收中伪造完成；任何生产异常都保持或恢复 `READ_BACKEND=mysql`。

## 1. 目标与架构决策

### 1.1 目标

在不改变业务接口返回契约的前提下，将行情类只读查询从 MySQL 迁移到 ClickHouse，并保留可快速回退到 MySQL 的能力。最终要求：

- 同一份代码、同一个开发分支同时支持 MySQL 和 ClickHouse 读后端。
- 行情只读接口统一经过 Repository，不在 Flask route 中直接拼接数据库查询。
- 写入、采集、修复任务、调度、通知和管理配置继续使用 MySQL。
- 固定 `as_of_ms` 后，MySQL 与 ClickHouse 的接口 JSON 逐字段一致，而不仅是行数一致。
- ClickHouse 实例具备只读保护，误发写请求时明确失败，不修改 ClickHouse 或 MySQL 数据。
- 本地可以同时启动两个实例，分别观察两套读结果并一键回退。

### 1.2 已确定的方案

| 决策 | 方案 |
|---|---|
| 后端选择 | 进程级环境变量 `READ_BACKEND=mysql\|clickhouse` |
| 代码组织 | 同一分支、同一套 Repository，通过后端选择分派实现 |
| MySQL 角色 | 唯一写入源、任务状态源、管理数据源，同时保留 MySQL 读能力 |
| ClickHouse 角色 | 行情历史数据的只读查询源 |
| 双实例方式 | 同一代码启动两个独立进程，不长期维护两套分支 |
| 灰度方式 | 先启动 ClickHouse 只读实例进行验证，再切换读流量；不通过请求头在业务代码中分叉 |
| 时间验证 | 统一追加 `as_of_ms`，使用两库共同存在的已关闭 5 分钟时间点 |
| 回滚方式 | 将读实例的 `READ_BACKEND` 改回 `mysql` 并重启；不删除 ClickHouse 数据 |

## 2. 当前基线

### 2.1 已完成

- ClickHouse 表结构和 MySQL 导入脚本已存在：
  - `sql/schema_clickhouse.sql`
  - `scripts/import_mysql_to_clickhouse.py`
- ClickHouse 只读客户端和线程本地连接已存在：
  - `src/coinx/read_clients.py`
  - `src/coinx/read_backend.py`
- 以下行情读 Repository 已接入 ClickHouse 分支：
  - `funding_rate.py`
  - `market_tickers.py`
  - `homepage_series.py`
  - `market_structure_series.py`
  - `market_structure_score.py`
  - `trade_opportunities.py`
  - `contract_detail.py`
  - `market_read.py`
- Web 层已具备：
  - `GET /api/health/read-backend`
  - `X-Read-Backend` 响应头
  - ClickHouse 进程拒绝非 GET 请求并返回 503
- `as_of_ms` 已贯穿时间敏感的行情查询、首页聚合、结构评分、交易机会和缓存键。
- 已有只读验证、shadow 验证和定向测试；首页 Repository 30 个通过、exchange repair 28 个通过、资金费率 Repository 18 个通过。
- 最近一次真实双库验证中，24 个 API 检查全部通过，缓存检查 6/6 通过，12 个接口固定回放后的完整 JSON 逐递归比较无差异，4 个 ClickHouse 写入保护全部返回 503。

### 2.2 已关闭的验收问题

- 首页核心序列完整性判断已修复；可选交易所缺失不再误报为首页不可用，最新报告 `content_warnings=0`。
- `/api/funding-rate/abnormal` 已采用 ClickHouse 窗口查询和缺失币种回查，避免默认扫描全历史。
- 资金费率历史和行情排行已增加进程级缓存/结果复用，正式性能报告 12/12 接口通过 p95 门槛，ClickHouse 磁盘增长为 0 bytes。
- 当前性能回放的最大 ClickHouse/MySQL p95 比例为 `1.372`，12 个接口各运行 10 次且错误率为 0，磁盘增量为 `0 bytes`。
- 最新固定时间回放为 `API 24/24`、`Cache 6/6`、`JSON comparisons 12/12 (diff=0)`、`Write guards 4/4`、`Rollback PASS`。

## 3. 分阶段开发计划

### 阶段 0：冻结基线和数据范围

产出：迁移前基线记录和可重复的时间点。

- 固定测试连接：
  - MySQL：`10.0.0.128:13306/coinx`
  - ClickHouse：`http://10.0.0.128:8123/coinx`
- 记录六张行情表的行数、最小/最大时间、主键范围和 ClickHouse 分区数。
- 使用 `--auto-as-of` 取两库共同的已关闭 5 分钟时间点，或显式记录 `--as-of-ms`。
- 关闭测试实例调度：`SCHEDULER_ENABLED=false`，避免验证过程中写入或刷新缓存。
- 保存完整 JSON 和 Markdown 报告，报告不得只保留摘要。

验收：同一命令在相同数据快照上可以重复运行，时间点和数据库连接信息可追溯。

### 阶段 1：Repository 读链路收口

产出：所有行情 GET 接口的查询边界清单和 Repository 覆盖证明。

- 逐一确认以下接口只通过 Repository 获取行情数据：
  - `/api/funding-rate`
  - `/api/funding-rate/abnormal`
  - `/api/funding-rate/history/<symbol>`
  - `/api/market-rank`
  - `/api/coins`
  - `/api/coin-detail/<symbol>`
  - `/api/coin-detail/<symbol>/series`
  - `/api/coin-detail/<symbol>/structure-score`
  - `/api/coin-detail/<symbol>/trade-opportunity`
  - `/api/market-structure-score`
  - `/api/trade-opportunities`
- 检查 route、service、缓存函数中是否仍存在行情表的直接 MySQL 查询；如存在，迁移到 Repository 或明确标记为 MySQL 专属管理数据。
- 保留 MySQL 写入链路：采集器、repair、refresh、scheduler、任务运行记录、通知和配置管理不切换到 ClickHouse。
- 给每个 Repository 定义同样的字段单位、排序、空值、时间边界和分页语义。

验收：静态搜索没有未说明的行情表直连；MySQL 和 ClickHouse 实现遵守同一个 Repository 返回契约。

### 阶段 2：ClickHouse 数据和查询语义校验

产出：表级数据一致性报告和查询语义差异清单。

- 先比较行数和时间边界，再比较固定样本的完整记录。
- 对 `ReplacingMergeTree` 表确认重复版本、`FINAL` 使用范围和 `updated_at` 语义；不能仅靠行数判断没有重复数据。
- 对时间序列统一确认：
  - `event_time`/`close_time` 的单位为毫秒。
  - 上界是否包含 `<= as_of_ms`。
  - 168 小时等窗口的起止点是否与 MySQL 相同。
  - 空窗口返回 `[]`、`0`、`null` 的行为是否一致。
- 对首页净流入、资金费率、结构评分和交易机会确认 join、排序、聚合和 fallback 逻辑一致。

验收：固定 `as_of_ms` 后，接口层完整 JSON 可以递归比较，不以“字段数量相同”替代内容比较。

### 阶段 3：正式双库验证和缓存验证

产出：可审计的双库报告脚本和门禁结果。

- `scripts/verify_dual_backend_api.py` 已实现，一次命令完成：
  - 启动/调用 MySQL 读实例。
  - 启动/调用 ClickHouse 读实例。
  - 固定或自动选择 `as_of_ms`。
  - 对全部行情 GET 接口发起同样请求。
  - 保存两边完整 payload、请求参数、HTTP 状态、耗时、后端标识和差异路径。
  - 输出 `PASS`、`DIFF`、`NOT_COMPARABLE` 和最终汇总。
- 比较规则：
  - 默认严格递归比较 JSON 类型、键、数组顺序、数值和空值。
  - 只有明确属于实时展示时间的字段才允许通过配置排除，并记录排除原因。
  - 不能用放宽精度或只比较摘要来隐藏真实差异。
- 缓存验证至少包括：
  - 首次请求写入缓存。
  - 相同参数的第二次请求命中同一缓存对象。
  - `nocache=1` 产生新对象。
  - `as_of_ms` 不同会产生不同缓存键。
  - MySQL 和 ClickHouse 实例之间不会共享错误的进程内缓存对象。
- 当前验证已覆盖请求级命中、`nocache` 绕过、`as_of_ms` 隔离和双实例结果隔离；正式切换前还必须补充多线程/多请求的进程级共享缓存验证。
- 只读保护验证：向 ClickHouse 实例发送代表性的 POST/refresh 请求，必须返回 503，且两库行数、缓存和任务状态不变。

验收门槛：

```text
HTTP failures       = 0
JSON differences    = 0
Unexpected writes   = 0
Cache checks        = 100% pass
Read-backend health = pass
```

### 阶段 4：双实例启动和运维隔离

产出：两个实例可并行启动、停止、查看日志和健康检查。

目标配置：

| 实例 | 端口 | `READ_BACKEND` | 调度 |
|---|---:|---|---|
| MySQL 读写实例 | 5500 | `mysql` | 生产/测试按需开启 |
| ClickHouse 只读实例 | 5501 | `clickhouse` | 必须关闭 |

`scripts/start_app.py` 已实现实例隔离，正式验收还需要在目标运行环境执行停止隔离演练：

- 支持 `INSTANCE_NAME=mysql|clickhouse`，或命令行 `--instance`。
- PID 文件改为 `data/app-mysql.pid`、`data/app-clickhouse.pid`。
- 日志文件改为 `logs/app-mysql.log`、`logs/app-clickhouse.log` 及对应 error 日志。
- 进程检查必须同时校验端口和实例命令，不能误停另一实例。
- `stop/status/restart` 命令必须对指定实例生效。

临时手工启动方式（在启动管理脚本完成前）：

```powershell
$env:WEB_PORT = "5500"
$env:READ_BACKEND = "mysql"
$env:SCHEDULER_ENABLED = "false"
python src/coinx/main.py
```

另开一个 PowerShell：

```powershell
$env:WEB_PORT = "5501"
$env:READ_BACKEND = "clickhouse"
$env:SCHEDULER_ENABLED = "false"
python src/coinx/main.py
```

验收：两个端口同时返回健康检查，响应头分别为 `mysql` 和 `clickhouse`，停止一个实例不影响另一个实例。

### 阶段 5：性能、稳定性和故障演练

产出：性能基线、故障行为和切换决策。

- `scripts/benchmark_dual_backend_api.py` 已完成：两后端分别预热后，每个接口运行至少 10 次，记录 p50、p95、最大耗时、错误率和磁盘变化，并输出 JSON/Markdown 报告。
- 重点处理 `/api/funding-rate/abnormal` 等全量聚合接口，确认 ClickHouse 查询不会因 `FINAL`、无索引过滤或过宽时间范围退化。
- 验证 ClickHouse 不可用、超时、返回异常数据时：
  - ClickHouse 实例返回明确错误或健康检查失败。
  - 不发生隐式写入。
  - 生产流量可切回 MySQL。
- 验证 MySQL 写入后 ClickHouse 延迟期间的读一致性提示和数据新鲜度字段。
- 观察连接池、线程本地连接、查询超时、内存和磁盘增长。

建议门槛：ClickHouse 读接口 p95 不高于 MySQL 基线的 1.5 倍；关键接口错误率为 0；异常接口在正式切换前必须有明确的窗口查询、排序键/索引或聚合优化结论。

- `scripts/verify_dual_backend_api.py --exercise-rollback` 已完成：停止 ClickHouse 实例后确认其端口不可用、MySQL 健康、两边行数不变，并记录 ClickHouse 流量归零。

### 阶段 6：灰度切换、观察和正式迁移

产出：切换记录、观察窗口和回滚记录。

1. 保持 MySQL 实例作为写入和任务实例。
2. 启动 ClickHouse 只读实例，只给测试或内部流量。
3. 连续观察至少一个完整业务周期，重复双库报告和缓存验证。
4. 通过门禁后，将目标读流量切到 `READ_BACKEND=clickhouse`。
5. 保留 MySQL 读实例和回滚配置，不删除旧链路。
6. 观察错误率、延迟、数据新鲜度和磁盘空间；稳定后再决定是否下线 MySQL 读实例。

## 4. 配置与运行约定

ClickHouse 连接只用于读 Repository，不复用 SQLAlchemy/MySQL 写连接：

```text
CLICKHOUSE_URL=http://10.0.0.128:8123
CLICKHOUSE_DATABASE=coinx
CLICKHOUSE_USER=root
CLICKHOUSE_PASSWORD=<secret>
CLICKHOUSE_READ_TIMEOUT_SECONDS=120
READ_BACKEND=mysql|clickhouse
CLICKHOUSE_READ_SHADOW=false|true
SCHEDULER_ENABLED=false   # 双实例本地验证时必须关闭
```

建议将本地配置放在未提交的环境文件或 PowerShell 环境变量中，不把真实密码写入仓库、报告或命令历史。

## 5. 测试分层

| 层级 | 内容 | 通过标准 |
|---|---|---|
| 单元测试 | Repository 字段映射、时间边界、空值、排序、缓存键 | 现有定向测试全部通过 |
| 集成测试 | MySQL/ClickHouse 客户端、连接超时、只读保护 | 连接和错误行为符合契约 |
| API 回放 | 固定 `as_of_ms` 的全部行情 GET 接口 | 完整 JSON 递归比较无差异 |
| 缓存测试 | 命中、绕过、时间点隔离、对象复用 | 3/3 及扩展检查全部通过 |
| 双实例测试 | 5500/5501 并行运行、健康检查、停止隔离 | 后端标识正确且互不影响 |
| 故障演练 | ClickHouse 不可用、超时、数据延迟 | 能告警并快速回 MySQL |
| 写入回归 | MySQL 采集、repair、refresh、任务和通知 | 仍只写 MySQL，业务行为不变 |

## 6. 回滚方案

### 应用层回滚

- 将读实例环境变量改为 `READ_BACKEND=mysql`。
- 重启该实例并检查 `GET /api/health/read-backend` 和 `X-Read-Backend`。
- 保留 ClickHouse 数据和导入状态，不执行删除数据库、删除表或清理 volume。

### 数据层回滚

- 不反向导入、不删除 MySQL 数据。
- ClickHouse 仅作为可重建的读副本；发现数据问题时暂停 ClickHouse 读流量，修复导入或查询后重新验证。

### 回滚完成标准

```text
MySQL read backend = healthy
Write/scheduler    = MySQL only
ClickHouse traffic = 0
No data deletion   = confirmed
```

## 7. 最终 Go/No-Go 清单

### Go

- [x] 最新完整双库报告 `JSON comparisons=12, diffs=0`。
- [x] 所有目标行情 GET 接口均已列入报告，HTTP 失败数为 0。
- [x] 所有目标接口均通过 Repository，未发现未说明的行情表直连。
- [x] 首页、结构评分、交易机会的完整性判断已纳入回放；最新报告 `content_warnings=0`。
- [x] ClickHouse 只读保护和 MySQL 写入回归通过。
- [x] 双实例启动、停止、日志和 PID 已隔离。
- [x] 性能和资源指标达到门槛：12/12 接口各运行 10 次，p95 比例全部 <= 1.5，错误率为 0，磁盘增长为 0 bytes。
- [x] 回滚命令已在测试环境实际执行过：MySQL/ClickHouse 行数不变、ClickHouse 最终不可访问且流量归零。

### No-Go

出现以下任一情况不得切换：

- 固定时间点仍有未解释的字段或数组差异。
- ClickHouse 实例可以执行写入或 refresh。
- `homepage_complete=false` 等完整性问题没有业务确认。
- 关键接口超时或 p95 明显劣于 MySQL 且没有缓解措施。
- 两实例 PID、日志或端口管理会互相覆盖。
- 无法在一个操作内恢复到 MySQL。

## 8. 计划产出文件

- `docs/clickhouse-read-migration-development-plan.md`：本计划。
- `scripts/verify_dual_backend_api.py`：正式双库回放和完整 JSON 比较。
- `scripts/benchmark_dual_backend_api.py`：双库 p50/p95/错误率/磁盘基线和门禁。
- `scripts/start_app.py`：支持实例名、独立 PID 和日志。
- `docs/clickhouse-api-asof-verification.md`：固定时间点验证说明。
- `docs/clickhouse-api-readonly-report-*.md/json`：每次验证的审计报告。
- `tests/`：Repository、客户端、回放、缓存和只读保护测试。
- `tests/test_market_read_cache.py`、`tests/test_benchmark_dual_backend_api.py`：进程级缓存和性能门禁单元测试。

## 9. 具体执行顺序

### 9.1 本地代码回归

先关闭会写库的调度器，并使用测试环境的鉴权配置：

```powershell
$env:SCHEDULER_ENABLED = "false"
$env:WEB_AUTH_DISABLED = "false"
python -m pytest tests -q
```

默认回归不执行 `scripts/test_*.py`，因为其中部分脚本需要 StarRocks 配置或会直接写真实 MySQL。Gate 外部接口耗时诊断单独执行：

```powershell
$env:RUN_GATE_DIAGNOSTICS = "1"
python -m pytest tests/test_homepage_series_repository.py::test_gate_support_duration -q
```

### 9.2 双实例启动

使用同一份代码启动两个独立进程，ClickHouse 实例不启用调度器：

```powershell
python scripts/verify_dual_backend_api.py `
  --start-local `
  --auto-as-of `
  --mysql-host 10.0.0.128:13306 `
  --mysql-database coinx `
  --mysql-user root `
  --mysql-password $env:MYSQL_PASSWORD `
  --clickhouse-url http://10.0.0.128:8123 `
  --clickhouse-database coinx `
  --clickhouse-user root `
  --clickhouse-password $env:CLICKHOUSE_PASSWORD `
  --report-file data/clickhouse-dual-backend-report.json `
  --http-timeout 240 `
  --startup-timeout 90
```

脚本必须打印两个端口的后端健康状态，并保存完整 JSON、差异路径、缓存检查、写入保护和进程日志。密码只从环境变量读取，不写入计划、报告或命令历史。

### 9.3 Go/No-Go 判定

只有同时满足以下条件才允许把生产读流量切到 ClickHouse：

1. 固定 `as_of_ms` 的完整接口 JSON 递归比较为 0 差异。
2. 缓存命中、`nocache` 绕过、时间点隔离和双实例缓存隔离全部通过。
3. ClickHouse 的 POST/PUT/PATCH/DELETE/refresh 写入保护全部返回 503，MySQL 写入回归通过。
4. 首页覆盖、结构评分和交易机会的业务完整性已由产品确认，不能只看 HTTP 200。
5. 关键接口 p95、错误率和 ClickHouse 磁盘增长达到第 5 阶段门槛。
6. 已在测试环境执行一次回滚，并确认 MySQL 健康、ClickHouse 流量归零、没有删除数据。

任一项不满足都保持 `READ_BACKEND=mysql`，修复后重新生成报告，不直接修改历史报告作为结论。

### 9.4 正式切换与回滚

切换只修改读实例的环境变量并重启：

```powershell
$env:READ_BACKEND = "clickhouse"
python scripts/start_app.py --instance clickhouse restart
```

出现异常时立即执行：

```powershell
$env:READ_BACKEND = "mysql"
python scripts/start_app.py --instance mysql restart
```

回滚不删除 ClickHouse 表、数据或 Docker volume；修复导入或查询后重新执行 9.2 和 9.3。

## 10. 本轮验收证据

以下报告由当前代码和测试库重新生成，密码未写入报告：

- 双库完整回放及回滚：`C:\Users\38963\AppData\Local\Temp\coinx-dual-backend-current.json`、同名 `.md`。
- 性能门禁：`C:\Users\38963\AppData\Local\Temp\coinx-dual-backend-benchmark-current.json`、同名 `.md`。
- 全量回归：`391 passed, 13 skipped, 3 warnings`；命令为 `$env:PYTHONDONTWRITEBYTECODE='1'; $env:SCHEDULER_ENABLED='false'; $env:WEB_AUTH_DISABLED='false'; python -B -m pytest tests -vv --disable-warnings`。

最终技术验收结论：**PASS**。生产流量切换仍必须在部署窗口执行阶段 6，并保留一键回滚到 `READ_BACKEND=mysql` 的配置。

## 11. 开发任务拆解与完成定义

### 11.1 代码边界

- Route 只负责参数校验、鉴权和响应格式；不新增 ClickHouse SQL。
- 行情查询统一进入 `src/coinx/repositories/`，由 `is_clickhouse_read()` 选择 MySQL 或 ClickHouse 实现。
- `src/coinx/read_clients.py` 只提供只读客户端；ClickHouse 分支不得调用 SQLAlchemy 写连接。
- 采集、repair、refresh、scheduler、通知、任务状态和管理配置继续走 MySQL，不做双写。
- `READ_BACKEND` 是进程级开关；双实例只是同一代码的两个进程，不维护两套业务分支。

### 11.2 P0 实现顺序

1. 抽取 funding/ticker 的进程级共享缓存，使用锁保护并将后端、数据库、URL、查询参数和 `as_of_ms` 纳入键；补充并发命中和隔离测试。
2. 降低 ClickHouse 首页冷启动的外部支持状态调用次数；保留 `exchange_statuses` 和 `missing_exchanges` 的返回契约；重新运行固定时间双库比较。
3. 实现 `scripts/benchmark_dual_backend_api.py`，输出每个接口的请求次数、成功数、错误数、p50、p95、最大值、平均值和对比结论；门槛不通过时退出码非零。
4. 实现 `--exercise-rollback`，保存停止前后的健康检查、表行数、端口状态、后端标识和流量计数；任何数据删除命令均禁止出现在演练流程中。

### 11.3 每个任务的验收命令

```powershell
$env:SCHEDULER_ENABLED = "false"
$env:WEB_AUTH_DISABLED = "false"
python -m pytest tests -q --disable-warnings

python scripts/verify_dual_backend_api.py `
  --start-local --auto-as-of `
  --mysql-host 10.0.0.128:13306 --mysql-database coinx `
  --mysql-user root --mysql-password $env:MYSQL_PASSWORD `
  --clickhouse-url http://10.0.0.128:8123 `
  --clickhouse-database coinx --clickhouse-user root `
  --clickhouse-password $env:CLICKHOUSE_PASSWORD `
  --report-file data/clickhouse-dual-backend-report.json `
  --http-timeout 240 --startup-timeout 90

python scripts/benchmark_dual_backend_api.py `
  --mysql-api-url http://127.0.0.1:5500 `
  --clickhouse-api-url http://127.0.0.1:5501 `
  --iterations 10 `
  --report-file data/clickhouse-dual-backend-benchmark.json
```

最终完成定义：全量回归通过；固定 `as_of_ms` 的完整 JSON 无差异；缓存、只读保护、p95 和回滚门禁全部通过；报告中记录命令、时间点、连接目标、结果文件和操作者。未达到完成定义前，生产读流量保持 `READ_BACKEND=mysql`。
## Unified backend configuration

生产推荐只配置 MARKET_BACKEND=mysql 或 MARKET_BACKEND=clickhouse。READ_BACKEND
和 MARKET_WRITE_BACKEND 仍可显式覆盖单个方向，用于双实例验证和紧急回滚。
