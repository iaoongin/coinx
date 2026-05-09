# 交易所序列数据可行性说明

## 目的

这份文档记录当前代码里三家交易所在以下几类数据上的实际支持情况，避免后续讨论时把“官方接口可能支持什么”和“项目当前已经实现什么”混在一起：

- `klines`：历史 K 线
- `open_interest_hist`：历史持仓量
- `taker_buy_sell_vol`：主动买卖成交量
- `funding_rate`：资金费率

本文档只描述当前代码实现，不代表交易所官方接口的全部能力。

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

- Binance 目前是三家里数据最完整的
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

### Bybit

- `klines`（历史 K 线）：已实现，但只包含基础字段
  - 支持 `volume`（成交量）
  - 支持 `quote_volume`（成交额）
  - 不支持 `taker_buy_base_volume`
  - 不支持 `taker_buy_quote_volume`
- `open_interest_hist`（历史持仓量）：已实现
  - 支持 `sum_open_interest`（持仓量）
  - 当前不支持 `sum_open_interest_value`（持仓价值）
- `taker_buy_sell_vol`（主动买卖成交量）：未实现
- `funding_rate`（资金费率）：已实现，支持全量加载

结论：

- Bybit 当前只支持基础 K 线和持仓量能力
- 目前没有独立主动买卖成交量数据接入

## 三家交易所对照表

| 交易所 | K 线成交量 | K 线成交额 | K 线主动买入量 | K 线主动买入额 | 独立主动买卖接口 | 历史持仓量 | 持仓价值 | 资金费率 |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Binance | 支持 | 支持 | 支持 | 支持 | 支持 | 支持 | 支持 | 支持 |
| OKX | 支持 | 支持 | 不支持 | 不支持 | 支持 | 支持 | 支持 | 支持 |
| Bybit | 支持 | 支持 | 不支持 | 不支持 | 不支持 | 支持 | 不支持 | 支持 |

## 对首页的影响

- 首页依赖：
  - `klines`（历史 K 线）
  - `open_interest_hist`（历史持仓量）
  - `taker_buy_sell_vol`（主动买卖成交量）
- 对 Binance：
  - 首页数据支持最完整
- 对 OKX：
  - 首页净流入和主动买卖压力应主要依赖 `taker_buy_sell_vol`
- 对 Bybit：
  - 当前不具备主动买卖成交量能力，因此首页相关指标能力较弱

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

### 结论 3：Bybit 当前更适合作为补充源

- 可补充价格和持仓量
- 暂不适合作为主动买卖结构分析主源

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
