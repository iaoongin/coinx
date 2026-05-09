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

## 任务清单

| 任务 ID | 函数名 | 类型 | 触发方式 | 作用 | 处理范围 | 主要数据/结果 | 是否核心 |
|---|---|---|---|---|---|---|---|
| `market_rank_refresh_job` | `scheduled_market_rank_refresh` | 行情刷新 | `interval`，每 `UPDATE_INTERVAL` 秒 | 刷新行情榜快照 | 全市场行情榜币种 | 行情榜快照数据 | 是 |
| `repair_market_rolling_job` | `scheduled_repair_market_rolling` | 滚动修补 | `interval`，每 `REPAIR_TRACKED_INTERVAL` 秒 | 滚动修补市场币种最新序列 | 跟踪币 + 成交额前 N | `klines`（历史 K 线）、`open_interest_hist`（历史持仓量）、`taker_buy_sell_vol`（主动买卖成交量，按交易所能力） | 是 |
| `repair_market_history_job` | `scheduled_repair_market_history` | 历史补齐 | `interval`，每 `REPAIR_HISTORY_INTERVAL` 秒 | 补市场币种历史缺口 | 跟踪币 + 成交额前 N | `klines`（历史 K 线）、`open_interest_hist`（历史持仓量）、`taker_buy_sell_vol`（主动买卖成交量，按交易所能力） | 是 |
| `binance_series_repair_job` | `scheduled_binance_series_repair_update` | 专项修补 | `interval`，每 `BINANCE_SERIES_REPAIR_INTERVAL` 秒 | 修补 Binance 专有序列 | Binance 币种 | Binance 专有情绪与结构序列 | 是 |
| `update_coins_config_job` | `scheduled_coins_config_update` | 配置刷新 | `cron`，每天 `00:00` | 刷新跟踪币配置 | 币种配置 | 跟踪币配置列表 | 否 |

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

### 4. `binance_series_repair_job`

- 任务类型：专项修补
- 主要作用：修补 Binance 独有序列
- 范围：
  - Binance
- 主要数据：
  - 大户持仓比
  - 大户账户比
  - 全市场账户比

说明：

- 这些序列不属于当前多交易所统一修补任务
- 因此需要单独保留该任务

### 5. `update_coins_config_job`

- 任务类型：配置刷新
- 主要作用：更新跟踪币配置
- 范围：
  - 配置层，不是市场序列
- 结果：
  - 影响后续哪些币会进入跟踪和修补集合

说明：

- 这个任务不是数据修补任务
- 但它会影响数据修补任务的目标币池

## 已移除的重复任务

以下任务已被统一能力覆盖，因此不再保留：

| 原任务 | 原作用 | 当前替代任务 |
|---|---|---|
| `repair_tracked_rolling_job` | 只修跟踪币最新点 | `repair_market_rolling_job` |
| `repair_top_volume_job` | 只修成交额前 N 最新点 | `repair_market_rolling_job` |

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
- Binance 专有序列补齐
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
- 一个 Binance 专项任务：`binance_series_repair_job`
- 一个配置刷新任务：`update_coins_config_job`
- 一个启动即执行一次的市场补全任务：`start_startup_repair`

这样就能快速区分：

- 哪些任务负责“最新”
- 哪些任务负责“历史”
- 哪些任务负责“配置”
