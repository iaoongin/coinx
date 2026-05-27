# 交易所序列数据可行性说明

## 目的

这份文档记录当前代码里四家交易所在以下几类数据上的支持情况，避免后续讨论时把“官方接口可能支持什么”“项目当前已经实现什么”和“业务当前实际在消费什么”混在一起：

- `klines`：历史 K 线
- `open_interest_hist`：历史持仓量
- `taker_buy_sell_vol`：主动买卖成交量
- `funding_rate`：资金费率

本文档统一区分三层口径：

- `官方支持`：只写官方文档已核实的能力或周期
- `项目当前实现`：只写当前仓库已经接入的能力
- `业务当前实际使用`：只写调度、修补、页面当前实际在消费的能力

除非明确写成“官方支持”，否则下文不代表交易所官方接口的全部能力。

## 术语说明

- `volume`：成交量
- `quote_volume`：成交额
- `taker`：主动成交方
- `taker_buy`：主动买
- `taker_sell`：主动卖
- `taker_buy_base_volume`：主动买入成交量
- `taker_buy_quote_volume`：主动买入成交额
- `buy_vol`：主动买量
- `sell_vol`：主动卖量
- `buy_sell_ratio`：主动买卖比

## 官方周期支持（已核实）

下表只汇总本轮已经从官方文档直接核到的周期支持，不包含推断项。若某类接口官方已确认存在但当前未拿到周期枚举原文，会明确标注“待核实”。

| 交易所 | K 线官方周期 | OI / 多空比 / taker 统计官方周期 |
|---|---|---|
| Binance | `1m 3m 5m 15m 30m 1h 2h 4h 6h 8h 12h 1d 3d 1w 1M` | `5m 15m 30m 1h 2h 4h 6h 12h 1d` |
| OKX | `1m 3m 5m 15m 30m 1H 2H 4H 6H 12H 1D 1W 1M 3M 6M 1Y`，另有 UTC 版本 | `5m 1H 1D` |
| Bybit | `1 3 5 15 30 60 120 240 360 720 D W M` | `5min 15min 30min 1h 4h 1d` |
| Gate | `10s 1m 5m 15m 30m 1h 4h 8h 1d 7d 30d` | `10s 1m 5m 15m 30m 1h 4h 8h 1d 7d` |

## 当前实现结论

### Binance

- `klines`（历史 K 线）：已实现，字段最完整
  - 支持 `volume`（成交量）
  - 支持 `quote_volume`（成交额）
  - 支持 `trade_count`（成交笔数）
  - 支持 `taker_buy_base_volume`（主动买入成交量）
  - 支持 `taker_buy_quote_volume`（主动买入成交额）
- `open_interest_hist`（历史持仓量）：已实现
  - 支持 `sum_open_interest`（持仓量）
  - 支持 `sum_open_interest_value`（持仓价值）
- `taker_buy_sell_vol`（主动买卖成交量）：已实现
  - 支持 `buy_vol`（主动买量）
  - 支持 `sell_vol`（主动卖量）
  - 支持 `buy_sell_ratio`（买卖比）
- `funding_rate`（资金费率）：已实现，支持全量加载

结论：

- Binance 目前是四家里数据最完整的
- 既能从 K 线拿到主动买字段，也有独立主动买卖接口

### OKX

- `klines`（历史 K 线）：已实现，但主动买卖字段不完整
  - 支持 `volume`（成交量）
  - 支持 `quote_volume`（成交额）
  - 不支持 `taker_buy_base_volume`
  - 不支持 `taker_buy_quote_volume`
- `open_interest_hist`（历史持仓量）：已实现
  - 支持 `sum_open_interest`（持仓量，部分返回结构下可解析）
  - 支持 `sum_open_interest_value`（持仓价值）
- `taker_buy_sell_vol`（主动买卖成交量）：已实现
  - 支持 `buy_vol`（主动买量）
  - 支持 `sell_vol`（主动卖量）
  - 支持 `buy_sell_ratio`（买卖比）
- `funding_rate`（资金费率）：已实现，支持全量加载

结论：

- OKX 的 K 线当前只适合做价格、成交量、成交额类分析
- 如果需要主动买卖压力，应该优先依赖 `taker_buy_sell_vol`（主动买卖成交量）接口
- 不应把 Binance K 线里的 `taker_buy_*` 能力直接类比到 OKX

#### OKX Rubik 官方已确认接口

以下接口已从 OKX 官方文档、changelog 或官方接口实际返回确认存在。当前代码里的 OKX 持仓历史实现曾误接到 `open-interest-volume`，后续应以 `open-interest-history` 为准重新校正：

- `Get contract open interest history`
- `Get contract taker volume`
- `Get top traders contract long/short ratio`
- `Get top traders contract long/short ratio (by position)`
- `Get contract long/short ratio`

补充说明：

- `GET /api/v5/rubik/stat/contracts/open-interest-history` 会校验 `instId`，缺失时返回 `50014 instId can’t be empty`
- 官方返回的错误提示明确为 `support [5m,1H,1D]`
- 因此项目里 OKX OI 的逻辑长周期虽然仍按 `1h` 处理，但请求与存储口径必须映射为 `1H`

#### OKX 当前实际使用接口与限流

下表只统计项目当前实际使用到的 OKX REST 接口，便于评估采集可行性、排查 `429` 和设计节流策略。

| 接口 | 本地用途 | 关键参数 | 官方限流 | 限流规则 | 当前建议节流值 |
|---|---|---|---|---|---|
| `GET /api/v5/public/instruments` | 拉 OKX 可用 USDT 永续合约列表，做 `supports_symbol` 缓存 | `instType=SWAP` | `20次/2s` | `IP + Instrument Type` | 走缓存，避免高频刷新 |
| `GET /api/v5/market/history-candles` | 拉 K 线历史 | `instId`, `bar`, `before`, `after`, `limit` | `20次/2s` | `IP` | `limit` 最大 `300`；实测需按 `before=start_time`、`after=end_time` 传参 |
| `GET /api/v5/rubik/stat/contracts/open-interest-history` | 拉持仓历史 `open_interest_hist` | `instId`, `period`, `begin`, `end` | `5次/2s` | `IP` | 当前实测默认仅返回 `100` 条，分页方式仍需继续确认 |
| `GET /api/v5/rubik/stat/contracts/open-interest-volume` | 合约持仓统计量 | `ccy`, `period`, `begin`, `end` | `5次/2s` | `IP` | 不再作为 `open_interest_hist` 主接口候选 |
| `GET /api/v5/rubik/stat/taker-volume-contract` | 拉合约维度主动买卖量 `taker_buy_sell_vol` | `instId`, `period`, `begin`, `end` | `5次/2s` | `IP` | 已切换到合约维度新接口，不再使用 `ccy + instType=CONTRACTS` 的聚合接口；仍建议按 `400-500ms` 间隔串行 |
| `GET /api/v5/public/funding-rate` | 拉单币/全量资金费率 | `instId` | `10次/2s` | `IP + Instrument ID` | 建议按 `200ms+` 间隔控制，全量加载避免频繁触发 |

项目中的 OKX 相关实现位于 [series.py](/Z:/Resource/Code/project/coinx/src/coinx/collector/okx/series.py)。
其中 `/api/v5/rubik/` 路径已在代码里归为单独的节流组，建议把这组接口视为高敏接口处理。
OKX Rubik 本地实现不再对 `5m`/`1H`/`1D` 历史窗口做额外裁剪，修补任务会保留调用方传入的 `begin`/`end`，以支持 `168h` 等长窗口补齐。

### Gate

- `klines`（历史 K 线）：已实现，但只包含基础字段
  - 支持 `volume`（成交量）
  - 支持 `quote_volume`（成交额）
  - 不支持 `taker_buy_base_volume`
  - 不支持 `taker_buy_quote_volume`
- `open_interest_hist`（历史持仓量）：已实现
  - 支持 `sum_open_interest`（持仓量）
  - 支持 `sum_open_interest_value`（持仓价值，通过 `open_interest_usd` 字段）
- `taker_buy_sell_vol`（主动买卖成交量）：已实现
  - 支持 `buy_vol`（主动买量）
  - 支持 `sell_vol`（主动卖量）
  - 支持 `buy_sell_ratio`（买卖比）
- `funding_rate`（资金费率）：已实现，支持全量加载

#### Gate 主动买卖数据可行性（2026-05 调研）

Gate 当前已调用的 `GET /api/v4/futures/{settle}/contract_stats` 接口，返回数据中已包含以下主动买卖相关字段，当前代码在 `parse_open_interest_hist` 中未提取：

| 响应字段 | 说明 | 对应标准字段 |
|---|---|---|
| `long_taker_size` | 主动买入成交量（合约张数） | `buy_vol` |
| `short_taker_size` | 主动卖出成交量（合约张数） | `sell_vol` |
| `lsr_taker` | 主动买卖比 | `buy_sell_ratio` |
| `time` | Unix 时间戳（秒），需转毫秒 | `event_time` |

支持周期：`5m 15m 30m 1h 4h 8h 1d 7d 30d`

关键优势：

- 该接口为公开接口，无需 API Key 认证
- 项目已经在调用该接口获取持仓历史数据，可复用同一请求
- 限流规则与 `open_interest_hist` 相同（`200次/10s/endpoint`），不会产生额外限流压力

可选方案（未采用）：

- Gate 另有 `GET /api/v4/futures/{settle}/taker_buy_sell_vol` 专用接口，但需要私有认证（KEY + SIGN + Timestamp），增加复杂度，且与 `contract_stats` 数据语义一致，无额外收益

实施路径：

1. 在 `gate/series.py` 的 `SUPPORTED_SERIES_TYPES` 中添加 `'taker_buy_sell_vol'`
2. 新增 `fetch_taker_buy_sell_vol`（复用 `contract_stats` 端点）和 `parse_taker_buy_sell_vol`（提取 `long_taker_size` / `short_taker_size` / `lsr_taker`）
3. 在 `exchange_adapters.py` 的 Gate 适配器中配置 `taker_period_by_interval`
4. 注意 `time` 字段为秒级 Unix 时间戳，需转换为毫秒

### Bybit

- `klines`（历史 K 线）：已实现，但只包含基础字段
  - 支持 `volume`（成交量）
  - 支持 `quote_volume`（成交额）
  - 不支持 `taker_buy_base_volume`
  - 不支持 `taker_buy_quote_volume`
- `open_interest_hist`（历史持仓量）：已实现
  - 支持 `sum_open_interest`（持仓量）
  - 当前不支持 `sum_open_interest_value`（持仓价值）
- `taker_buy_sell_vol`（主动买卖成交量）：未实现，且官方接口不具备能力
- `funding_rate`（资金费率）：已实现，支持全量加载

#### Bybit 主动买卖数据可行性（2026-05 调研）

Bybit V5 API **没有**提供合约维度的 taker buy/sell volume 接口。调研结果：

| 接口 | 返回内容 | 能否替代 taker volume |
|---|---|---|
| `/v5/market/account-ratio` | 多空**账户数**比例（`buyRatio` / `sellRatio`） | 不能。语义是账户数量比，非成交量比 |
| `/v5/market/tickers` | spot 分类下可能含 `takerBuyVolume` / `takerSellVolume` | 不能。仅 24h 聚合值，非历史时序；linear/inverse 分类未确认 |
| `/v5/market/recent-trade` | 逐笔成交含 `side` 字段 | 理论上可手动聚合，但开销极大，不实际 |
| K 线 `/v5/market/kline` | 仅 7 字段（OHLCV + turnover），无 taker 字段 | 不能 |

支持周期（`account-ratio`，非 taker volume）：`5min 15min 30min 1h 4h 1d`

结论：

- Bybit V5 API 当前不提供合约 taker buy/sell volume 数据
- `/v5/market/account-ratio` 返回的是账户数多空比，与主动买卖成交量语义不同，不应作为替代
- 如果未来需要 Bybit 主动买卖数据，只能依赖第三方数据服务（如 Coinalyze、Laevitas）或等待官方新增接口

## 四家交易所对照表

| 交易所 | K 线成交量 | K 线成交额 | K 线主动买入量 | K 线主动买入额 | 独立主动买卖接口 | 历史持仓量 | 持仓价值 | 资金费率 |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Binance | 支持 | 支持 | 支持 | 支持 | 支持 | 支持 | 支持 | 支持 |
| OKX | 支持 | 支持 | 不支持 | 不支持 | 支持 | 支持 | 支持 | 支持 |
| Bybit | 支持 | 支持 | 不支持 | 不支持 | 不支持（官方不具备） | 支持 | 不支持 | 支持 |
| Gate | 支持 | 支持 | 不支持 | 不支持 | 支持 | 支持 | 支持 | 支持 |

## 对首页的影响

- 首页依赖：
  - `klines`（历史 K 线）
  - `open_interest_hist`（历史持仓量）
  - `taker_buy_sell_vol`（主动买卖成交量）
- 对 Binance：
  - 首页数据支持最完整
- 对 OKX：
  - 首页净流入和主动买卖压力应主要依赖 `taker_buy_sell_vol`
- 对 Gate：
  - `contract_stats` 接口已包含主动买卖字段，接入后可与 Binance/OKX 同等使用
- 对 Bybit：
  - 官方不提供 taker volume 接口，首页相关指标能力较弱，短期内无法改善

## 对评分页的影响

- 评分页当前主要依赖：
  - `klines`（历史 K 线）
  - `open_interest_hist`（历史持仓量）
  - `funding_rate`（资金费率）
- 评分页当前不强依赖 OKX 的 `taker_buy_sell_vol`
- 评分页如果要提升 OKX 的主动买卖压力准确性，后续可以单独评估是否引入 `taker_buy_sell_vol`

## 可行性结论

### 结论 1：Binance 适合作为完整数据基线

- K 线字段完整
- 主动买卖接口完整
- 持仓量与持仓价值完整

### 结论 2：OKX 适合作为价格、持仓和资金费率来源

- K 线基础字段可用
- 持仓量可用
- 资金费率可用
- 主动买卖需要依赖单独接口，不能只靠 K 线

### 结论 3：Gate 可通过现有接口补充主动买卖数据

- `contract_stats` 接口已返回 `long_taker_size` / `short_taker_size` / `lsr_taker`，项目当前在获取 OI 时已调用该接口但未提取 taker 字段
- 接入成本低：复用同一端点，新增解析逻辑即可，无限流增量
- 建议优先实施，实现四家中三家支持主动买卖数据

### 结论 4：Bybit 当前更适合作为补充源

- 可补充价格和持仓量
- 官方 V5 API 不提供合约 taker volume 接口，不具备主动买卖数据采集能力
- `/v5/market/account-ratio` 返回账户数多空比，与主动买卖成交量语义不同，不建议作为替代

## 后续讨论建议

后续讨论如果涉及交易所字段能力，统一使用下面的中文口径：

- `klines`：历史 K 线
- `open_interest_hist`：历史持仓量
- `taker_buy_sell_vol`：主动买卖成交量
- `funding_rate`：资金费率
- `taker_buy_quote_volume`：主动买入成交额
- `buy_vol`：主动买量
- `sell_vol`：主动卖量

这样可以避免把“主动买入字段”和“完整主动买卖拆分”混为一谈。

## Gate / Bybit 限流补充

### Bybit 当前实际使用接口与限流

下表只统计项目当前实际使用到的 Bybit REST 接口。Bybit 官方当前最明确的公开限制是 `600 requests / 5 seconds / IP`，同时还会返回 UID 级 API limit 响应头。

| 接口 | 本地用途 | 关键参数 | 官方限流 | 限流规则 | 当前建议节流值 |
|---|---|---|---|---|---|
| `GET /v5/market/instruments-info` | 拉 Bybit 可用 USDT 合约列表，做 `supports_symbol` 缓存 | `category`, `status`, `cursor` | `600次/5s` | `IP` | 走缓存，避免高频刷新 |
| `GET /v5/market/kline` | 拉 K 线历史 | `category`, `symbol`, `interval`, `start`, `end`, `limit` | `600次/5s` | `IP` | 正常节奏即可，批量任务避免无意义重试 |
| `GET /v5/market/open-interest` | 拉持仓历史 `open_interest_hist` | `category`, `symbol`, `intervalTime`, `startTime`, `endTime`, `limit` | `600次/5s` | `IP` | 正常节奏即可，批量任务避免无意义重试 |
| `GET /v5/market/tickers` | 拉单币/全量资金费率 | `category`, `symbol` | `600次/5s` | `IP` | 单币查询和全量查询避免并发叠加 |

补充说明：

- Bybit 官方还说明 API 会返回 `X-Bapi-Limit-Status`、`X-Bapi-Limit`、`X-Bapi-Limit-Reset-Timestamp`，用于反映 UID 级限流窗口状态。
- 项目中的 Bybit 相关实现位于 [series.py](/Z:/Resource/Code/project/coinx/src/coinx/collector/bybit/series.py)。

### Gate 当前实际使用接口与限流

下表只统计项目当前实际使用到的 Gate REST 接口。Gate 官方明确说明所有 public endpoints 均为 `200 requests / 10 seconds / endpoint / IP`，并返回 `x-gate-ratelimit-*` 响应头。

| 接口 | 本地用途 | 关键参数 | 官方限流 | 限流规则 | 当前建议节流值 |
|---|---|---|---|---|---|
| `GET /api/v4/futures/{settle}/contracts` | 拉 Gate 可用合约列表，做 `supports_symbol` 缓存 | 无 | `200次/10s/endpoint` | `IP` | 走缓存，避免高频刷新 |
| `GET /api/v4/futures/{settle}/candlesticks` | 拉 K 线历史 | `contract`, `interval`, `from`, `to`, `limit` | `200次/10s/endpoint` | `IP` | 建议结合 `x-gate-ratelimit-*` 头按预算串行控制 |
| `GET /api/v4/futures/{settle}/contract_stats` | 拉持仓历史 `open_interest_hist` | `contract`, `interval`, `from`, `limit` | `200次/10s/endpoint` | `IP` | 建议结合 `x-gate-ratelimit-*` 头按预算串行控制 |
| `GET /api/v4/futures/{settle}/funding_rate` | 拉单币资金费率 | `contract` | `200次/10s/endpoint` | `IP` | 正常节奏即可，避免和历史补齐高峰叠加 |
| `GET /api/v4/futures/{settle}/tickers` | 拉全量 ticker/资金费率 | 无 | `200次/10s/endpoint` | `IP` | 建议优先使用全量接口，减少单币请求数 |

补充说明：

- Gate 返回头会带 `x-gate-ratelimit-limit`、`x-gate-ratelimit-requests-remain`、`x-gate-ratelimit-reset-timestamp`，适合直接做预算式流控。
- `contract_stats` 官方参数不包含 `to`。实测如果额外传 `to`，接口会退化成只返回最近一小段数据，无法按长窗口稳定取历史。
- `contract_stats` 在 `interval=5m` 下单次返回会被 `limit` 截断；实测 `limit=1000` 时单页约覆盖 `83h`，补 `168h` 必须按返回结果的最后 `event_time` 继续翻页。
- 项目中的 Gate 相关实现位于 [series.py](/Z:/Resource/Code/project/coinx/src/coinx/collector/gate/series.py)。

### Binance 当前实际使用接口与限流

下表只统计项目当前实际使用到的 Binance Futures REST 接口。Binance 官方的公开 REST 更偏向 `REQUEST_WEIGHT` 模型，不同 endpoint 的权重不同，最终共同消耗 IP 级请求权重预算。

| 接口 | 本地用途 | 关键参数 | 官方限流 | 限流规则 | 当前建议节流值 |
|---|---|---|---|---|---|
| `GET /fapi/v1/exchangeInfo` | 拉 Binance Futures 交易对和系统限流信息 | 无 | 按返回 `rateLimits` / `REQUEST_WEIGHT` 计算 | `IP` | 走缓存，避免频繁刷新 |
| `GET /fapi/v1/klines` | 拉 K 线 / 聚合 K 线 | `symbol`, `interval`, `startTime`, `endTime`, `limit` | 按 `REQUEST_WEIGHT` 计算 | `IP` | 正常节奏即可，批量补齐按分页控制 |
| `GET /fapi/v2/ticker/price` | 拉最新价格 | `symbol` | 按 `REQUEST_WEIGHT` 计算 | `IP` | 高频使用时优先聚合或复用已有价格结果 |
| `GET /fapi/v1/ticker/24hr` | 拉单币/全量 24h 行情 | `symbol` | 按 `REQUEST_WEIGHT` 计算 | `IP` | 全量查询避免过于频繁触发 |
| `GET /fapi/v1/openInterest` | 拉当前持仓量 | `symbol` | 按 `REQUEST_WEIGHT` 计算 | `IP` | 单币按需查询即可 |
| `GET /fapi/v1/premiumIndex` | 拉单币/全量资金费率相关信息 | `symbol` | 按 `REQUEST_WEIGHT` 计算 | `IP` | 单币与全量查询避免并发叠加 |
| `GET /futures/data/openInterestHist` | 拉持仓历史 `open_interest_hist` | `symbol`, `period`, `startTime`, `endTime`, `limit` | 按 `REQUEST_WEIGHT` 计算 | `IP` | 正常节奏即可，分页补齐时避免无意义重试 |
| `GET /futures/data/takerlongshortRatio` | 拉主动买卖量 `taker_buy_sell_vol` | `symbol`, `period`, `startTime`, `endTime`, `limit` | 按 `REQUEST_WEIGHT` 计算 | `IP` | 正常节奏即可，适合首页补齐链路 |
| `GET /futures/data/topLongShortPositionRatio` | 拉大户持仓人数比 | `symbol`, `period`, `startTime`, `endTime`, `limit` | 按 `REQUEST_WEIGHT` 计算 | `IP` | 正常节奏即可，适合评分/情绪链路 |
| `GET /futures/data/topLongShortAccountRatio` | 拉大户账户数多空比 | `symbol`, `period`, `startTime`, `endTime`, `limit` | 按 `REQUEST_WEIGHT` 计算 | `IP` | 正常节奏即可，适合评分/情绪链路 |
| `GET /futures/data/globalLongShortAccountRatio` | 拉全市场账户数多空比 | `symbol`, `period`, `startTime`, `endTime`, `limit` | 按 `REQUEST_WEIGHT` 计算 | `IP` | 正常节奏即可，适合评分/情绪链路 |

补充说明：

- Binance 的 `exchangeInfo` 会返回 `rateLimits`，请求响应头也会带 `X-MBX-USED-WEIGHT-*`，适合做全局权重观测。
- 与 OKX `rubik`、Gate WAF 相比，Binance 当前这组公开接口通常更适合批量修补和全量采集。
- 项目中的 Binance 相关实现位于 [series.py](/Z:/Resource/Code/project/coinx/src/coinx/collector/binance/series.py) 和 [market.py](/Z:/Resource/Code/project/coinx/src/coinx/collector/binance/market.py)。
## 工程限流策略补充

当前工程已经在 `repair` 相关请求层落地统一的轻量限流框架，核心目标是“稳优先”，优先减少批量修补时的 `429/403` 和无效重试，而不是追求极限吞吐。

### 当前实现摘要

| 交易所 | 当前工程策略 | 说明 |
|---|---|---|
| OKX | 分组冷却 + 最小间隔 | `/api/v5/rubik/` 归为高敏 `rubik` 组，优先按 `Retry-After` / `RateLimit-Reset` 进入整组冷却；无头时走 fallback。 |
| Gate | 预算头驱动 + budget unavailable | 以 `x-gate-ratelimit-limit` / `remain` / `reset-timestamp` 推进预算；若 `403` 且拿不到 `remain/reset`，直接标记 host/api budget unavailable，停止盲重试。 |
| Bybit | 头部预算感知 + fallback 冷却 | 使用 `X-Bapi-Limit-*` 响应头感知预算；`403/429` 时按 reset 或 fallback 冷却。 |
| Binance | 最小冷却适配 | 这轮不做复杂 `REQUEST_WEIGHT` 建模，只在 `403/429` 后进入短 cooldown，避免 repair 连续硬打。 |

### Repair 链路行为

- `rolling` 和 `history` 都已接入统一的 `budget_unavailable` 语义。
- 某交易所某组一旦进入 cooldown / budget unavailable，当前批次该交易所后续任务会直接返回 `skipped`。
- `skipped reason` 统一为 `okx_budget_unavailable`、`gate_budget_unavailable`、`bybit_budget_unavailable`、`binance_budget_unavailable`。
- 一个交易所进入冷却，不会阻塞其它交易所继续执行。

### 配置原则

- 本轮实现不要求新增任何必填环境变量。
- 仍兼容已有的 `OKX_RUBIK_MIN_INTERVAL_MS`、`OKX_429_RETRY_FALLBACK_SECONDS`、`GATE_MIN_INTERVAL_MS`、`GATE_403_RETRY_FALLBACK_SECONDS`。
- 如果没有配置，代码会直接使用内置保守默认值。

## 官方来源

- Binance Skills Hub / USDⓈ-M Futures: [USDS Futures](https://www.binance.com/hu/skills/detail/binance/derivatives-trading-usds-futures)
- Bybit V5 `Get Kline`: [docs](https://bybit-exchange.github.io/docs/v5/market/kline)
- Bybit V5 `Get Open Interest`: [docs](https://bybit-exchange.github.io/docs/v5/market/open-interest)
- Gate API v4 Futures: [docs](https://www.gate.com/docs/developers/apiv4/en/futures/)
- OKX API docs v5: [docs-v5](https://www.okx.com/docs-v5/en)
- OKX API changelog: [log_en](https://www.okx.com/docs-v5/log_en/)
- 补充实测结论：
  - `history-candles` 若把 `before/after` 方向传反，会直接返回空数组。
  - `open-interest-volume` 的 `5m` 实测只会返回最近约 `48h`（约 `570-575` 条）切片；更早时间会返回 `50030 Illegal time range`，说明它并不适合做精确的持仓历史补齐。
  - `open-interest-history` 才是更贴近“持仓量历史”的接口，但当前实测仅返回最近 `100` 条，且尚未确认有效的翻页/游标参数，暂时不能直接替换上线。
