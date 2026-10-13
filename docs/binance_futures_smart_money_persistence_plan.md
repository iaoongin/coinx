# 币安合约聪明钱页面数据持久化方案

> 目标：将币安合约“聪明钱”页面的全量列表、`overview` 和 `details/stats` 数据按时间保存，支持历史曲线、当前状态、与价格/OI/资金费率联查。
>
> 关联接口说明：[`docs/binance_smart_money_api.md`](binance_smart_money_api.md)
>
> 设计日期：2026-10-13

## 1. 结论

### 当前实施状态

当前已落地第一阶段：每 15 分钟请求 `signal/list?timeRange=30m` 和 `signal/list?timeRange=ALL`，分别保存接口返回的全部列表行；不限制币种数量，不调用 `exchangeInfo` 做二次过滤。页面默认展示 `ALL`，可切换到 `30m`。`overview`、`details/stats` 和对应详情表仅保留设计，不在本阶段采集。

新增一条独立的合约聪明钱采集链路，不复用 `market_snapshots`：

```text
CoinX 跟踪合约
      |
      v
APScheduler: collect_smart_money_signals_job
      |
      v
Binance www BAPI
  signal/list(30m, ALL)
  overview + details/stats(按需或重点合约)
      |
      v
解析、校验、补充本地采集时间
      |
      +----------------------+
      |                      |
      v                      v
market_futures_smart_money_signal  market_futures_smart_money_stats
             |
             v
      market_futures_smart_money
      |                      |
      +----------+-----------+
                 v
        MySQL 或 ClickHouse
```

推荐默认策略：

| 项目 | 建议值 |
| --- | --- |
| 采集范围 | `signal/list` 返回的全部合约，不设置币种数量上限，不自行过滤 |
| 列表统计窗口 | `30m`、`ALL` |
| 采集周期 | `15m` |
| 重点合约详情 | Phase 1 不启用，后续再配置 |
| 请求并发 | 全局 1-2 个，带随机间隔；不要按币种大并发 |
| 保存周期 | 默认 90 天，按独立配置控制 |
| 历史补齐 | 不支持接口级补齐；接口只返回当前状态，失败时保留缺口 |
| 写入后端 | 复用 `MARKET_BACKEND`：MySQL 或 ClickHouse |

## 2. 为什么不能复用现有表

`market_snapshots` 的 `data_json` 可以暂时存原始返回，但不适合作为正式方案：

- 页面数据需要按多空、鲸鱼、盈利人数和平均开仓价过滤/排序；全部放 JSON 会导致查询复杂且慢。
- 当前 `overview` 与 `details/stats` 是两种不同粒度的数据，不能可靠地混合到普通行情快照中。
- 现有行情表的唯一键、保留策略和 ClickHouse 排序键都是按行情序列设计的。
- 后续需要与 `market_klines`、`market_open_interest_hist`、`market_funding_rate` 按时间联查。

因此采用“类型化字段 + 原始 JSON”的方式：常用字段直接查询，完整响应保留用于审计和字段变更兼容。

## 3. 数据采集策略

### 3.1 采集对象

列表页存在全量接口，优先使用它，不需要先从 `exchangeInfo` 发现 symbol：

```python
for time_range in ("30m", "ALL"):
    rows = fetch_smart_money_signal_list(time_range=time_range)
    persist_all(rows)
```

全量列表请求：

```text
GET https://www.binance.com/bapi/futures/v1/public/future/smart-money/signal/list?timeRange={timeRange}
```

采集约束：

- 保存接口实际返回的全部 `data[]`，不按 symbol 数量截断；
- 不用 `exchangeInfo` 二次过滤，页面可能包含币安支持的非传统加密合约；
- 当前实施 `timeRange=30m` 和 `timeRange=ALL`，页面默认查询 `ALL`；其他时间范围留待后续扩展；
- 若全量列表行需要补充详情，再从列表结果中的 symbol 请求 `overview` 和 `details/stats`，详情采集不影响列表主链路。

建议增加配置：

```text
SMART_MONEY_ENABLED=true
SMART_MONEY_INTERVAL=900
SMART_MONEY_PERIOD=15m
SMART_MONEY_TIME_RANGES=30m,ALL
SMART_MONEY_FOCUS_ENABLED=false
SMART_MONEY_FOCUS_INTERVAL=300
SMART_MONEY_FOCUS_SYMBOLS=BTCUSDT,ETHUSDT
SMART_MONEY_RETENTION_DAYS=90
SMART_MONEY_REQUEST_TIMEOUT_SECONDS=20
SMART_MONEY_MAX_WORKERS=1
SMART_MONEY_JITTER_SECONDS=3
```

`SMART_MONEY_FOCUS_SYMBOLS` 只用于重点合约的详情接口高频采集，不限制全量列表任务的返回范围。全量列表中的 symbol 集合以每次接口返回为准。

### 3.2 全量列表和详情请求

列表页每个采集周期按窗口各请求一次：

```text
GET https://www.binance.com/bapi/futures/v1/public/future/smart-money/signal/list?timeRange={30m|ALL}
```

合约详情页再按 symbol 请求：

```text
GET https://www.binance.com/bapi/futures/v1/public/future/smart-money/signal/overview?symbol={symbol}

GET https://www.binance.com/bapi/futures/v1/public/future/smart-money/signal/details/stats?symbol={symbol}&timeRange=30m
```

请求头至少包含：

```http
User-Agent: Mozilla/5.0
clienttype: web
referer: https://www.binance.com/zh-CN/smart-money/signal/{symbol}
```

接口是币安网页内部 BAPI，不是带版本承诺的公开交易 API。采集器必须：

1. 使用现有 `requests.Session` 和代理配置；
2. 复用现有 Binance 403/429 冷却机制，但对 418/429 优先尊重 `Retry-After`；
3. 同一周期对每个配置窗口各请求一次全量列表，不重复请求 `timeRange=30m` 或 `timeRange=ALL`；
4. 任务 `max_instances=1`、`coalesce=True`，避免上一个周期未结束时叠加请求；
5. `overview` / `details/stats` 只对重点合约或页面实际访问的合约请求，避免无意义地把全量列表再次展开成数千个请求；
6. 失败只记录错误和任务统计，不写入全零数据。
7. 全量列表行数随币安页面变化，任务摘要记录各窗口返回行数和重复 symbol 数量。

### 3.3 时间和幂等

每条数据保留两个时间：

| 字段 | 含义 |
| --- | --- |
| `snapshot_time` | CoinX 本地采集时间，按周期对齐，例如 15 分钟边界；用于历史曲线、唯一键和 `as_of` 查询。 |
| `source_update_time` | Binance `overview.data.updateTime`；可能为空或重复，只作为来源更新时间，不作为唯一键。 |

原因：币安 `updateTime` 是上游当前状态时间，不能保证每次请求变化；本地 `snapshot_time` 才能稳定表达“本周期采集过一次”。

列表表推荐唯一键：

```text
(exchange, time_range, symbol, period, snapshot_time)
```

详情表推荐唯一键：

```text
(exchange, symbol, period, snapshot_time)
```

同一周期重试时执行 upsert/ReplacingMergeTree 去重。`collected_at` 作为版本字段，后写入的成功结果覆盖前一次结果。

### 3.4 缺口语义

这些网页接口不能按历史时间查询。因此：

- 请求失败不补写一条 `NULL` 或零值记录；
- 查询历史曲线时允许出现时间缺口；
- 任务摘要记录 `success_count`、`failure_count`、`failed_symbols`；
- 页面可以显示“最近成功采集时间”和“数据陈旧”标识；
- 不能把上一条快照复制到当前周期，否则会把陈旧数据误当成当前聪明钱仓位。

## 4. 表结构

### 4.1 `market_futures_smart_money_signal`

保存 `signal/list` 全量列表快照，是持久化主表。

| 字段 | 类型建议 | 说明 |
| --- | --- | --- |
| `exchange` | `VARCHAR(20)` / `LowCardinality(String)` | 固定 `binance`。 |
| `time_range` | `VARCHAR(10)` / `LowCardinality(String)` | 当前为 `30m` 或 `ALL`，为后续扩展保留维度。 |
| `symbol` | `VARCHAR(40)` / `LowCardinality(String)` | 完整保存接口返回的 symbol，不自行过滤。 |
| `period` | `VARCHAR(10)` | CoinX 采集周期，例如 `15m`。 |
| `snapshot_time` | `BIGINT` / `UInt64` | 本地采集时间，毫秒。 |
| `side` | `VARCHAR(10)` | 主导方向。 |
| `net_notional` | `DECIMAL(38,12)` | 净名义价值。 |
| `long_notional` / `short_notional` | `DECIMAL(38,12)` | 多空名义价值。 |
| `long_traders` / `short_traders` | `INT` | 多空交易员数。 |
| `long_whales` / `short_whales` | `INT` | 多空鲸鱼数。 |
| `long_qty` / `short_qty` | `DECIMAL(38,12)` | 多空数量。 |
| `long_avg_entry_price` / `short_avg_entry_price` | `DECIMAL(38,12)` | 多空平均开仓价。 |
| `raw_json` | `JSON` / `String` | 完整列表行。 |
| `collected_at` | `DATETIME(3)` / `DateTime64(3)` | CoinX 写入时间。 |
| `updated_at` | `DATETIME(3)` / `DateTime64(3)` | 幂等覆盖版本时间。 |

唯一键：

```text
UNIQUE(exchange, time_range, symbol, period, snapshot_time)
```

### 4.2 `market_futures_smart_money`

保存 `overview` 的当前状态快照。

逻辑字段：

| 字段 | 类型建议 | 说明 |
| --- | --- | --- |
| `exchange` | `VARCHAR(20)` / `LowCardinality(String)` | 固定 `binance`，为多交易所扩展保留。 |
| `symbol` | `VARCHAR(20)` / `LowCardinality(String)` | 例如 `BTCUSDT`。 |
| `period` | `VARCHAR(10)` | `15m` 或 `5m`，表示采集周期。 |
| `snapshot_time` | `BIGINT` / `UInt64` | 本地采集时间，毫秒。 |
| `source_update_time` | `BIGINT` / `Nullable(UInt64)` | Binance `updateTime`，毫秒。 |
| `total_positions` | `DECIMAL(38,8)` / `Nullable(Decimal(38,8))` | 页面返回值，单位未稳定公开，保留但不作为 USD 使用。 |
| `total_traders` | `INT` / `Nullable(UInt32)` | 聪明钱交易员总数。 |
| `long_short_ratio` | `DECIMAL(20,8)` | 聪明钱多空比。 |
| `long_traders` | `INT` | 多头交易员数。 |
| `long_traders_qty` | `DECIMAL(38,8)` | 多头数量。 |
| `long_traders_avg_entry_price` | `DECIMAL(38,12)` | 多头平均开仓价。 |
| `long_whales` | `INT` | 多头鲸鱼数。 |
| `long_whales_qty` | `DECIMAL(38,8)` | 多头鲸鱼数量。 |
| `long_whales_avg_entry_price` | `DECIMAL(38,12)` | 多头鲸鱼平均开仓价。 |
| `short_traders` | `INT` | 空头交易员数。 |
| `short_traders_qty` | `DECIMAL(38,8)` | 空头数量。 |
| `short_traders_avg_entry_price` | `DECIMAL(38,12)` | 空头平均开仓价。 |
| `short_whales` | `INT` | 空头鲸鱼数。 |
| `short_whales_qty` | `DECIMAL(38,8)` | 空头鲸鱼数量。 |
| `short_whales_avg_entry_price` | `DECIMAL(38,12)` | 空头鲸鱼平均开仓价。 |
| `long_profit_traders` | `INT` | 当前盈利多头交易员数。 |
| `short_profit_traders` | `INT` | 当前盈利空头交易员数。 |
| `long_profit_whales` | `INT` | 当前盈利多头鲸鱼数。 |
| `short_profit_whales` | `INT` | 当前盈利空头鲸鱼数。 |
| `raw_json` | `JSON` / `String` | 完整 `overview` 返回体或 `data` 对象。 |
| `collected_at` | `DATETIME(3)` / `DateTime64(3)` | CoinX 写入时间。 |
| `updated_at` | `DATETIME(3)` / `DateTime64(3)` | 幂等覆盖版本时间。 |

唯一键和索引：

```text
UNIQUE(exchange, symbol, period, snapshot_time)
INDEX(symbol, period, snapshot_time)
INDEX(snapshot_time)
```

### 4.3 `market_futures_smart_money_stats`

保存 `details/stats` 的窗口变化数据。不要把 `30m` 字段硬编码到 overview 表中，这样后续支持 `5m`、`1h` 时无需改表。

| 字段 | 类型建议 | 说明 |
| --- | --- | --- |
| `exchange` | `VARCHAR(20)` / `LowCardinality(String)` | `binance`。 |
| `symbol` | `VARCHAR(20)` / `LowCardinality(String)` | 合约 symbol。 |
| `time_range` | `VARCHAR(10)` | 详情设计示例为 `30m`，可扩展到其他窗口。 |
| `period` | `VARCHAR(10)` | CoinX 采集周期，当前为 `15m`。 |
| `snapshot_time` | `BIGINT` / `UInt64` | 本地采集时间，毫秒。 |
| `source_update_time` | `BIGINT` / `Nullable(UInt64)` | 如果 stats 接口未来返回来源时间，保存；当前可为空。 |
| `long_traders` / `short_traders` | `INT` | 窗口内多空交易员指标。 |
| `long_whales` / `short_whales` | `INT` | 窗口内多空鲸鱼指标。 |
| `long_positions` / `short_positions` | `DECIMAL(38,8)` | 窗口内多空仓位指标。 |
| `long_qty` / `short_qty` | `DECIMAL(38,8)` | 窗口内多空数量指标。 |
| `long_whale_positions` / `short_whale_positions` | `DECIMAL(38,8)` | 窗口内多空鲸鱼仓位指标。 |
| `long_whale_qty` / `short_whale_qty` | `DECIMAL(38,8)` | 窗口内多空鲸鱼数量指标。 |
| `raw_json` | `JSON` / `String` | 完整 stats 返回体或 `data` 对象。 |
| `collected_at` | `DATETIME(3)` / `DateTime64(3)` | CoinX 写入时间。 |
| `updated_at` | `DATETIME(3)` / `DateTime64(3)` | 幂等覆盖版本时间。 |

唯一键：

```text
UNIQUE(exchange, symbol, time_range, period, snapshot_time)
```

### 4.4 MySQL DDL 草案

列表主表 DDL：

```sql
CREATE TABLE market_futures_smart_money_signal (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    exchange VARCHAR(20) NOT NULL DEFAULT 'binance',
    time_range VARCHAR(10) NOT NULL,
    symbol VARCHAR(40) NOT NULL,
    period VARCHAR(10) NOT NULL DEFAULT '15m',
    snapshot_time BIGINT NOT NULL,
    side VARCHAR(10) NULL,
    net_notional DECIMAL(38, 12) NULL,
    long_notional DECIMAL(38, 12) NULL,
    short_notional DECIMAL(38, 12) NULL,
    long_traders INT NULL,
    long_whales INT NULL,
    short_traders INT NULL,
    short_whales INT NULL,
    long_qty DECIMAL(38, 12) NULL,
    short_qty DECIMAL(38, 12) NULL,
    long_avg_entry_price DECIMAL(38, 12) NULL,
    short_avg_entry_price DECIMAL(38, 12) NULL,
    raw_json JSON NULL,
    collected_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
        ON UPDATE CURRENT_TIMESTAMP(3),
    UNIQUE KEY uk_fsm_signal_scope
        (exchange, time_range, symbol, period, snapshot_time),
    KEY idx_fsm_signal_symbol_time
        (symbol, time_range, period, snapshot_time),
    KEY idx_fsm_signal_snapshot_time (snapshot_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='币安合约聪明钱 signal/list 全量列表快照';
```

```sql
CREATE TABLE market_futures_smart_money (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    exchange VARCHAR(20) NOT NULL DEFAULT 'binance',
    symbol VARCHAR(20) NOT NULL,
    period VARCHAR(10) NOT NULL DEFAULT '15m',
    snapshot_time BIGINT NOT NULL,
    source_update_time BIGINT NULL,
    total_positions DECIMAL(38, 8) NULL,
    total_traders INT NULL,
    long_short_ratio DECIMAL(20, 8) NULL,
    long_traders INT NULL,
    long_traders_qty DECIMAL(38, 8) NULL,
    long_traders_avg_entry_price DECIMAL(38, 12) NULL,
    long_whales INT NULL,
    long_whales_qty DECIMAL(38, 8) NULL,
    long_whales_avg_entry_price DECIMAL(38, 12) NULL,
    short_traders INT NULL,
    short_traders_qty DECIMAL(38, 8) NULL,
    short_traders_avg_entry_price DECIMAL(38, 12) NULL,
    short_whales INT NULL,
    short_whales_qty DECIMAL(38, 8) NULL,
    short_whales_avg_entry_price DECIMAL(38, 12) NULL,
    long_profit_traders INT NULL,
    short_profit_traders INT NULL,
    long_profit_whales INT NULL,
    short_profit_whales INT NULL,
    raw_json JSON NULL,
    collected_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
        ON UPDATE CURRENT_TIMESTAMP(3),
    UNIQUE KEY uk_fsm_exchange_symbol_period_time
        (exchange, symbol, period, snapshot_time),
    KEY idx_fsm_symbol_period_time (symbol, period, snapshot_time),
    KEY idx_fsm_snapshot_time (snapshot_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='币安合约聪明钱 overview 快照';
```

`market_futures_smart_money_stats` 使用同样的公共字段，加上 stats 专属列和对应唯一键。正式实现时应同步维护 `sql/schema.sql`、`sql/schema_starrocks.sql` 和 `models.py`。

### 4.5 ClickHouse DDL 要点

```sql
CREATE TABLE IF NOT EXISTS coinx.market_futures_smart_money (
    exchange LowCardinality(String),
    symbol LowCardinality(String),
    period LowCardinality(String),
    snapshot_time UInt64,
    source_update_time Nullable(UInt64),
    total_positions Nullable(Decimal(38, 8)),
    total_traders Nullable(UInt32),
    long_short_ratio Nullable(Decimal(20, 8)),
    long_traders Nullable(UInt32),
    long_traders_qty Nullable(Decimal(38, 8)),
    long_traders_avg_entry_price Nullable(Decimal(38, 12)),
    long_whales Nullable(UInt32),
    long_whales_qty Nullable(Decimal(38, 8)),
    long_whales_avg_entry_price Nullable(Decimal(38, 12)),
    short_traders Nullable(UInt32),
    short_traders_qty Nullable(Decimal(38, 8)),
    short_traders_avg_entry_price Nullable(Decimal(38, 12)),
    short_whales Nullable(UInt32),
    short_whales_qty Nullable(Decimal(38, 8)),
    short_whales_avg_entry_price Nullable(Decimal(38, 12)),
    long_profit_traders Nullable(UInt32),
    short_profit_traders Nullable(UInt32),
    long_profit_whales Nullable(UInt32),
    short_profit_whales Nullable(UInt32),
    raw_json String DEFAULT '{}',
    collected_at DateTime64(3, 'Asia/Shanghai') DEFAULT now64(3),
    updated_at DateTime64(3, 'Asia/Shanghai') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(toDateTime(intDiv(snapshot_time, 1000), 'Asia/Shanghai'))
ORDER BY (exchange, symbol, period, snapshot_time);
```

stats 表沿用相同策略，排序键为：

```text
(exchange, symbol, time_range, period, snapshot_time)
```

ClickHouse 接入必须同步修改：

- `src/coinx/write_backend.py` 的 `MARKET_TABLE_COLUMNS`；
- `MARKET_TABLE_SCHEMA` 的 engine、sorting key、时间字段校验；
- `sql/schema_clickhouse.sql`；
- ClickHouse read repository 的表白名单/查询函数；
- 市场数据保留清理的 ClickHouse 表列表。

## 5. 代码落点

建议按现有职责拆分，不把 HTTP、解析、存储和查询全部放进 scheduler：

| 文件 | 责任 |
| --- | --- |
| `src/coinx/collector/binance/smart_money.py` | URL、请求头、请求重试、响应解析、字段归一化。 |
| `src/coinx/repositories/smart_money.py` | MySQL upsert、ClickHouse insert、最新/历史查询。 |
| `src/coinx/models.py` | 列表、overview、stats 三个 SQLAlchemy model。 |
| `src/coinx/scheduler.py` | 注册 `smart_money_snapshot_job`，控制并发和任务运行记录。 |
| `src/coinx/config.py` | 采集开关、周期、列表时间范围、详情 symbol、超时、保留天数。 |
| `src/coinx/write_backend.py` | ClickHouse 表字段白名单、schema 检查和写入支持。 |
| `src/coinx/repositories/retention.py` | MySQL/ClickHouse 保留策略。 |
| `sql/schema.sql` | MySQL 初始化 DDL。 |
| `sql/schema_starrocks.sql` | StarRocks 控制面/兼容环境 DDL。 |
| `sql/schema_clickhouse.sql` | ClickHouse DDL。 |
| `tests/test_smart_money.py` | 解析、幂等、失败和查询测试。 |

采集器建议返回统一摘要：

```json
{
  "status": "success",
  "period": "15m",
  "time_range": "30m",
  "list_rows": 730,
  "list_written": 730,
  "duplicate_symbols": 0,
  "list_error": null,
  "detail_symbols_total": 2,
  "overview_success": 2,
  "stats_success": 2,
  "failed_symbols": [],
  "written_overview": 2,
  "written_stats": 2,
  "snapshot_time": 1791785700000,
  "duration_ms": 42300
}
```

任务本身继续复用 `_mark_job_started` / `_mark_job_finished`，这样管理页可以看到执行耗时、失败 symbol 和写入数量。

## 6. 查询设计

### 6.1 最新快照

列表页按 `(time_range, symbol)` 取 `snapshot_time` 最大的一条；详情页按 `symbol` 取 `snapshot_time` 最大的一条。ClickHouse 使用 `argMax` 或 `FINAL`，避免 ReplacingMergeTree 未合并时出现重复。

```sql
SELECT s.*
FROM market_futures_smart_money_signal s
JOIN (
    SELECT exchange, time_range, symbol, period,
           MAX(snapshot_time) AS snapshot_time
    FROM market_futures_smart_money_signal
    WHERE exchange = 'binance' AND time_range = '30m' AND period = '15m'
    GROUP BY exchange, time_range, symbol, period
) latest
  ON latest.exchange = s.exchange
 AND latest.time_range = s.time_range
 AND latest.symbol = s.symbol
 AND latest.period = s.period
 AND latest.snapshot_time = s.snapshot_time;
```

```sql
SELECT m.*
FROM market_futures_smart_money m
JOIN (
    SELECT exchange, symbol, period, MAX(snapshot_time) AS snapshot_time
    FROM market_futures_smart_money
    WHERE exchange = 'binance' AND period = '15m'
    GROUP BY exchange, symbol, period
) latest
  ON latest.exchange = m.exchange
 AND latest.symbol = m.symbol
 AND latest.period = m.period
 AND latest.snapshot_time = m.snapshot_time;
```

### 6.2 历史曲线

常用曲线：

- `long_short_ratio`；
- `long_traders`、`short_traders`；
- `long_whales`、`short_whales`；
- 多空平均开仓价；
- 多空盈利人数占比：

```text
long_profit_ratio = long_profit_traders / max(long_traders, 1)
short_profit_ratio = short_profit_traders / max(short_traders, 1)
```

这些比例应在查询或应用层计算，不在采集时固化，避免上游字段缺失时产生误导性零值。

### 6.3 与现有行情数据联查

所有表使用毫秒时间戳，因此可以按最近时间点关联：

```text
smart_money.snapshot_time
  -> market_klines.open_time / close_time
  -> market_open_interest_hist.event_time
  -> market_funding_rate.event_time
```

不要强制要求四类数据同一毫秒完全相等，使用“向前最近点”或统一 15m 桶聚合。页面历史回放要支持 `as_of_ms`，规则为：

```text
smart_money.snapshot_time <= as_of_ms
```

这样不会把未来聪明钱状态泄漏到历史回放和评分结果中。

## 7. 失败、限流和数据质量

### 7.1 HTTP 错误

按以下顺序处理：

1. `200 + code == 000000`：解析并写入；
2. `200 + code != 000000`：记录业务错误，不写入；
3. `403/418/429`：读取 `Retry-After`，更新 Binance 冷却状态，停止本轮剩余请求或降低速率；
4. 超时/连接错误：单 symbol 有界重试，超过次数后跳过；
5. 返回 HTML 或字段无法解析：记录原始响应摘要，不能写全零记录。

### 7.2 字段质量

- 金额、数量、价格使用 Decimal，不通过 float 做持久化转换。
- 所有来自接口的数值字段允许为空；缺失字段写 `NULL`，不要写 `0`。
- `total_positions` 的单位未被币安公开稳定定义，保留原值但不参与 USD 计算。
- `longTradersQty`/`shortTradersQty` 是数量，不是 USD；需要 USD 时必须结合对应价格重新计算，并明确这是派生值。
- `longShortRatio`、盈利人数和鲸鱼数量均是页面口径，不要与 Binance Futures 公共 API 的 Top Trader 多空比直接混用。
- `source_update_time` 只作为上游元数据，历史顺序和去重使用本地 `snapshot_time`。

## 8. 保留和容量估算

设每个窗口返回 `N` 个 symbol，15 分钟采集、90 天的列表表容量为：

```text
N symbols × 2 windows × 96 snapshots/day × 90 days
```

以当前约 745 行计算：

```text
745 symbols × 2 windows × 96 snapshots/day × 90 days
= 12,873,600 rows
```

这还不包含 `overview`、`stats` 详情表、重试重复写入和索引开销。

建议：

- ClickHouse 作为长期查询和历史分析主存储；
- MySQL 保持同样的数据表以兼容当前市场写入切换和小规模部署；
- `raw_json` 保留 30-90 天，若 MySQL 体积压力明显，可后续将 raw payload 单独迁移到 ClickHouse；
- 沿用 `MARKET_DATA_RETENTION_DAYS` 或新增 `SMART_MONEY_RETENTION_DAYS`，不把该表遗漏在清理任务之外；
- 不做历史接口补采，因为上游没有按时间查询能力。

## 9. 分阶段实施

### Phase 1：采集和落库

- 增加列表、overview、stats 三张表和初始化/迁移 DDL；
- 增加采集器和 repository；
- 增加 15m 全量列表定时任务；
- 支持 MySQL、ClickHouse 两种写入后端；
- 记录各窗口返回行数、失败信息和详情采集摘要；
- 增加 90 天清理。

验收：连续运行 24 小时，成功率、重复数、失败数和表行数可核对；同一周期手动重跑不产生重复业务记录。

### Phase 2：查询和页面

- 最新聪明钱列表；
- 合约详情页历史曲线；
- `as_of_ms` 历史回放；
- 聪明钱与 OI、资金费率、价格的时间对齐；
- 展示数据陈旧时间和采集缺口。

### Phase 3：重点合约与告警

- 重点 symbol 的 `overview`/`stats` 5m 采集；
- 多空比突变、鲸鱼仓位突变、盈利人数变化告警；
- 与现有交易机会评分模型组合，但保持聪明钱为独立特征源；
- 根据 Web BAPI 的限流和任务耗时情况，调整请求间隔、并发度或采集周期；不通过截断 symbol 数量控制范围。

## 10. 测试清单

- 正常 `overview` 和 `stats` 响应解析；
- `data = null`、字段缺失、数值字符串和大精度数值；
- `code != 000000`、403、418、429、超时；
- `Retry-After` 冷却生效；
- 同一 `(exchange, time_range, symbol, period, snapshot_time)` 重试幂等；
- MySQL upsert 后只有一条记录；
- ClickHouse `ReplacingMergeTree` 查询不重复；
- `source_update_time` 重复但不同 `snapshot_time` 时仍能保存两个本地快照；
- 采集失败不会写入零值；
- retention 能删除三张表的过期数据；
- `as_of_ms` 不读取未来快照；
- `signal/list` 返回多少行就保存多少行；
- 全量列表任务与重点合约详情任务不会重复采集同一接口。

## 11. 不建议的做法

- 不要直接把所有字段塞进 `market_snapshots.data_json` 后再依赖 JSON 查询。
- 不要把上游 `updateTime` 当作唯一时间键。
- 不要把 `total_positions` 直接标成 USD。
- 不要使用 10-20 个线程并发扫描全部合约。
- 不要在 418/429 后立即无限重试。
- 不要在采集失败时复制上一条记录填补当前时间点。
- 不要把 `longShortRatio` 与公共 Futures Top Trader 指标直接拼成一个指标。
