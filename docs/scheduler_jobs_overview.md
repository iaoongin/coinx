# 定时任务说明文档

## 目的

这份文档记录当前项目中的定时任务与启动补全任务，包括：

- 任务名称
- 任务作用
- 处理范围
- 主要处理的数据
- 是否属于核心数据任务

本文档描述的是当前代码实现，便于后续排查任务重叠、确认数据覆盖范围，以及统一命名口径。

## 总览

当前项目中与运行时调度相关的任务可分为三类：

1. 行情刷新任务
2. 数据修补任务
3. 配置刷新任务

另外还有一个“启动期补全任务”，它不是 APScheduler 的周期任务，但会在服务启动后立即执行一次。

## 运行状态语义

任务运行记录使用以下状态：

| 层级 | 状态值 | 页面显示 | 含义 |
|---|---|---|---|
| 调度任务运行记录 | `running` | 运行中 | 本次任务仍在执行 |
| 调度任务运行记录 | `success` | 成功 | 本次任务完成，未发现未处理结果 |
| 调度任务运行记录 | `partial` | 部分成功 | 本次任务已执行结束，但仍有跳过项、缺口、未写入记录或部分失败 |
| 调度任务运行记录 | `error` | 失败 | 任务执行过程发生异常或服务中断 |
| 调度任务运行记录 | `timeout` | 已超时 | 超过 `TASK_RUN_STALE_TIMEOUT_SECONDS` 未更新心跳；页面按超时展示，原始记录仍保留用于排查 |
| 单个采集项 | `success` | 成功 | 当前币种、交易所和序列类型的数据已采集并写入 |
| 单个采集项 | `skipped` | 跳过 | 因接口能力、无数据或预检结果等原因未执行写入 |
| 单个采集项 | `error` | 失败 | 当前采集项请求或处理失败 |

`partial_success` 是修补阶段内部汇总状态，合并到调度任务运行记录后统一为 `partial`，页面统一显示为“部分成功”。“历史缺口未全部补齐”等文字属于结果原因说明，不是独立状态值。

任务控制状态（例如 `paused`、`ready`、`offline`）描述的是调度器或任务注册状态，不代表本次运行结果。

长任务在每个可观测进度点更新心跳。服务启动时会把上次遗留的 `running` 记录标记为 `error`；页面读取到超过心跳阈值但尚未完成的记录时显示为 `timeout`，避免服务中断或任务卡住后长期显示为“运行中”。

## 任务清单

| 任务 ID | 函数名 | 类型 | 触发方式 | 作用 | 处理范围 | 主要数据/结果 | 是否核心 |
|---|---|---|---|---|---|---|---|
| `market_rank_refresh_job` | `scheduled_market_rank_refresh` | 行情刷新 | `interval`，每 `UPDATE_INTERVAL` 秒 | 刷新行情榜快照 | 全市场行情榜币种 | 行情榜快照数据 | 是 |
| `repair_market_rolling_job` | `scheduled_repair_market_rolling` | 滚动修补 | `interval`，每 `REPAIR_TRACKED_INTERVAL` 秒 | 滚动修补市场币种最新序列 | 跟踪币 + 成交额前 N | `klines`（历史 K 线）、`open_interest_hist`（历史持仓量）、`taker_buy_sell_vol`（主动买卖成交量，按交易所能力） | 是 |
| `repair_market_history_job` | `scheduled_repair_market_history` | 历史补齐 | `interval`，每 `REPAIR_HISTORY_INTERVAL` 秒 | 按 `REPAIR_HISTORY_SYMBOL_BATCH_SIZE` 分批补市场币种历史缺口，并按记录数或时间增量刷写 | 跟踪币 + 成交额前 N | `klines`（历史 K 线）、`open_interest_hist`（历史持仓量）、`taker_buy_sell_vol`（主动买卖成交量，按交易所能力） | 是 |
| `update_coins_config_job` | `scheduled_coins_config_update` | 配置刷新 | `cron`，每天 `00:00` | 刷新跟踪币配置 | 币种配置 | 跟踪币配置列表 | 否 |
| `cleanup_data_job` | `scheduled_cleanup_data` | 数据清理 | `cron`，每天 `00:20` | 清理超过保留期限的行情数据、聪明钱快照和运行日志 | 全部已落库数据 | 行情数据 15 天、聪明钱 15 天、运行日志 15 天 | 否 |

## 启动期补全任务

| 名称 | 函数链路 | 触发方式 | 作用 | 处理范围 | 主要数据/结果 | 是否核心 |
|---|---|---|---|---|---|---|
| 启动期市场滚动补全 | `start_startup_repair` -> `scheduled_repair_market_rolling` | 服务启动后立即一次 | 启动时先补一轮市场币种最新序列 | 跟踪币 + 成交额前 N | 与 `repair_market_rolling_job` 相同 | 是 |

## 各任务详细说明

### 1. `market_rank_refresh_job`

- 任务类型：行情刷新
- 主要作用：刷新行情榜快照数据
- 特点：
  - 不负责修补历史序列
  - 更偏展示层数据刷新

### 2. `repair_market_rolling_job`

- 任务类型：滚动修补
- 主要作用：修补市场币种“最近几根”的最新序列
- 范围：
  - 跟踪币
  - 成交额前 N 币种
- 主要数据：
  - `klines`（历史 K 线）
  - `open_interest_hist`（历史持仓量）
  - `taker_buy_sell_vol`（主动买卖成交量）

说明：

- 这是当前统一的滚动修补任务
- 原先重复的“跟踪币滚动修补”和“成交额前 N 滚动修补”已经并入该任务

### 3. `repair_market_history_job`

- 任务类型：历史补齐
- 主要作用：补历史窗口内缺失的数据
- 范围：
  - 跟踪币
  - 成交额前 N 币种
- 主要数据：
  - `klines`（历史 K 线）
  - `open_interest_hist`（历史持仓量）
  - `taker_buy_sell_vol`（主动买卖成交量）

说明：

- 与滚动修补不同，这个任务不是补最新点，而是补历史缺口
- 用于保证长窗口计算时数据连续

### 4. `update_coins_config_job`

- 任务类型：配置刷新
- 主要作用：更新跟踪币配置
- 范围：
  - 配置层，不是市场序列
- 结果：
  - 影响后续哪些币会进入跟踪和修补集合

说明：

- 这个任务不是数据修补任务
- 但它会影响数据修补任务的目标币池

### 5. `cleanup_data_job`

- 任务类型：数据清理
- 主要作用：统一删除超过保留期限的数据
- 触发方式：每天 `00:20`
- 保留策略：
  - 行情数据：15 天
  - 聪明钱快照：15 天
  - 运行日志：15 天
- 清理范围：
  - 行情快照、ticker、K 线、持仓量、主动买卖成交量、资金费率
  - 合约聪明钱快照
  - `scheduled_job_runs` 任务运行记录

说明：

- 三类数据在同一个任务中按各自数据表清理。
- 聪明钱快照不再纳入普通行情清理列表，避免保留策略重复执行。
- 某一类清理失败时，其他类别继续执行，最终状态记录为“部分成功”。
- 全部清理成功后，按表的 `DATA_FREE / (DATA_LENGTH + INDEX_LENGTH + DATA_FREE)` 检查空闲比例；超过 `DATA_CLEANUP_OPTIMIZE_FREE_PERCENT`（默认 10%）的独立表空间才考虑执行 `OPTIMIZE TABLE`。这是估算值，可能漏掉页内碎片。
- 优化前使用无缓存的 MySQL 统计，并检查数据盘和临时盘空间；每个盘需至少有表数据及索引估算大小的 1.2 倍，加上 `DATA_CLEANUP_OPTIMIZE_RESERVE_GB`（默认 5 GiB）。这只是保守估算，不保证重建一定有足够空间。
- `DATA_CLEANUP_OPTIMIZE_DATADIR_PATH` 须指向应用可读取的 MySQL 数据目录映射，程序通过 `auto.cnf` 核对服务端 UUID；`DATA_CLEANUP_OPTIMIZE_TMPDIR_PATH` 须由部署人员显式指定为 MySQL 临时目录所在磁盘的映射。两者默认未配置，未配置或无法确认时跳过优化。部署人员负责确认临时盘映射实际对应 MySQL 的 `tmpdir` / `innodb_tmpdir`，多临时盘应分别评估；外部 MySQL 不可使用应用本地磁盘冒充数据库磁盘。
- MySQL 用户需要有读取 `information_schema.INNODB_TABLES` 的 `PROCESS` 权限，以及执行目标表 `OPTIMIZE TABLE` 所需的表权限；无法读取表空间信息时跳过该表。
- 优化逐表执行；跳过、失败和完成记录到运行日志。清理有失败时跳过全部优化，优化失败时任务记为“部分成功”。

## 已移除的重复任务

以下任务已被统一能力覆盖，因此不再保留：

| 原任务 | 原作用 | 当前替代任务 |
|---|---|---|
| `repair_tracked_rolling_job` | 只修跟踪币最新点 | `repair_market_rolling_job` |
| `repair_top_volume_job` | 只修成交额前 N 最新点 | `repair_market_rolling_job` |
| `cleanup_market_data_job` | 清理行情数据和聪明钱数据 | `cleanup_data_job` |
| `cleanup_task_run_history_job` | 清理任务运行日志 | `cleanup_data_job` |

说明：

- 现在 `repair_market_rolling_job` 已经覆盖：
  - 跟踪币
  - 成交额前 N
- 因此这两个旧任务属于重复修补，已合并删除

## 功能覆盖结论

### 当前不会丢的能力

- 跟踪币最新点滚动修补
- 成交额前 N 最新点滚动修补
- 市场币种历史缺口补齐
- 启动期立即补一轮市场币种最新序列

### 当前不属于这些定时任务的能力

- `funding_rate`（资金费率）落库
  - 当前不是通过这些定时任务持久化
  - 评分页主要按实时批量拉取使用

## 推荐理解方式

可以把当前任务体系理解为：

- 一个行情刷新任务：`market_rank_refresh_job`
- 一个统一滚动修补任务：`repair_market_rolling_job`
- 一个统一历史补齐任务：`repair_market_history_job`
- 一个配置刷新任务：`update_coins_config_job`
- 一个统一数据清理任务：`cleanup_data_job`
- 一个启动即执行一次的市场补全任务：`start_startup_repair`

这样就能快速区分：

- 哪些任务负责“最新”
- 哪些任务负责“历史”
- 哪些任务负责“配置”
