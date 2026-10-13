# 币安聪明钱数据接口整理

> 核验日期：2026-10-13（Asia/Shanghai）
>
> 本文整理币安 Web3 钱包和合约“聪明钱”页面当前实际使用的接口。网页 BAPI 属于产品内部接口，不等同于有版本承诺的 Binance Open API，接入时必须保留超时、限流、字段变更和接口下线处理。

## 结论

币安目前有两类容易混淆的“聪明钱”数据：

| 产品 | 数据内容 | 推荐接口 |
| --- | --- | --- |
| Web3 钱包聪明钱信号 | 链上聪明钱/KOL 买卖信号、触发价、当前价、退出率、历史最大涨幅 | `POST /bapi/defi/v1/public/wallet-direct/buw/wallet/web/signal/smart-money/ai` |
| Web3 聪明钱净流入排名 | 某时间窗口内各代币的聪明钱净流入、参与地址数、交易量和风险信息 | `POST /bapi/defi/v1/public/wallet-direct/tracker/wallet/token/inflow/rank/query/ai` |
| 合约聪明钱 | 合约列表聚合、单合约多空人数/仓位/平均开仓价/盈利人数 | `GET /bapi/futures/v1/public/future/smart-money/signal/list`、`overview` |

如果目标是监控链上新币机会，优先使用前两类；如果目标是分析 BTCUSDT、ETHUSDT 等合约多空持仓，使用第三类。

## 1. Web3 聪明钱信号

### 1.1 接口地址

```text
POST https://web3.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/web/signal/smart-money/ai
```

该接口当前无需 API Key，也不要求登录 Cookie。请求应从服务端发起，避免浏览器 CORS、地区网络和共享 IP 限流问题。

推荐请求头：

```http
Content-Type: application/json
Accept-Encoding: identity
User-Agent: binance-web3/3.0 (Skill)
```

### 1.2 请求体

```json
{
  "chainId": "56",
  "page": 1,
  "pageSize": 20,
  "smartSignalType": "SMART_MONEY"
}
```

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `chainId` | string | 是 | `56` = BSC；`CT_501` = Solana。当前官方 Skills Hub 还验证过 `1`、`8453`、`4663` 等链，但产品公开说明主要覆盖 BSC/Solana，其他链应先探测并保留降级。 |
| `page` | integer | 否 | 从 `1` 开始，默认 `1`。 |
| `pageSize` | integer | 否 | 每页数量，最大 `100`。 |
| `smartSignalType` | string | 否 | `SMART_MONEY` 过滤普通聪明钱；省略或传空字符串可返回所有信号，可能包含 `SMART_KOL`。 |

### 1.3 响应外层结构

成功响应：

```json
{
  "code": "000000",
  "message": null,
  "messageDetail": null,
  "data": [
    {
      "signalId": 60109,
      "ticker": "OKT",
      "chainId": "56",
      "contractAddress": "0xad7dfe7070cbf28d8ba86f9a992f94a438027777",
      "smartSignalType": "SMART_MONEY",
      "smartMoneyCount": 9,
      "direction": "buy",
      "signalTriggerTime": 1791813568000,
      "timeFrame": 65000,
      "totalTokenValue": "3262.845765389181151647",
      "alertPrice": "0.000111959709554197",
      "currentPrice": "0.000003700519600108445249744234065968",
      "highestPrice": "0.000111959709554197",
      "highestPriceTime": null,
      "exitRate": 96,
      "status": "outDecline",
      "maxGain": "0",
      "signalCount": 2
    }
  ],
  "success": true
}
```

判断成功时以 `code == "000000"` 为准，同时检查 HTTP 状态码。`success` 是辅助字段，不建议只依赖它。

### 1.4 `data[]` 字段

#### 代币身份

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `signalId` | integer | 信号唯一 ID。建议使用它去重。 |
| `ticker` | string | 代币符号，大小写不一定统一。 |
| `chainId` | string | 所属链。 |
| `contractAddress` | string | 代币合约地址；Solana 是 Base58 地址，不能按 EVM 地址处理。 |
| `tokenDecimals` | integer | 代币精度。 |
| `logoUrl` | string | 相对路径，需要拼接 `https://bin.bnbstatic.com`。 |
| `chainLogoUrl` | string | 链图标完整 URL。 |

#### 信号属性

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `smartSignalType` | string | `SMART_MONEY` 或 `SMART_KOL`。 |
| `direction` | string | `buy` 或 `sell`。 |
| `smartMoneyCount` | integer | 触发该信号的聪明钱地址数；它是数量，不是钱包地址列表。 |
| `signalCount` | integer | 该代币历史触发信号次数。 |
| `signalTriggerTime` | integer | 信号触发时间，Unix 毫秒。 |
| `timeFrame` | integer | 观察窗口，单位毫秒。 |
| `status` | string/null | 当前状态。实测/官方 CLI 文档出现 `valid`、`timeout`、`outDecline`、`exitRate` 和 `null`。初始信号可能为 `null`。 |

#### 价格和表现

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `alertPrice` | string | 触发时价格，USD。它不是实时下单价。 |
| `alertMarketCap` | string | 触发时市值，USD。 |
| `currentPrice` | string/null | 当前价格，USD。 |
| `currentMarketCap` | string/null | 当前市值，USD。 |
| `highestPrice` | string/null | 触发后观测到的最高价格。 |
| `highestPriceTime` | integer/null | 最高价格时间，Unix 毫秒。 |
| `totalTokenValue` | string | 触发窗口内聚合交易金额，USD。 |
| `maxGain` | string/null | 最高价相对触发价的收益比例，通常是小数比例：`0.25` 表示 `25%`，`5.7699` 表示 `576.99%`。展示百分比时乘以 `100`。 |
| `exitRate` | integer/null | 聪明钱退出比例，通常 `0-100`。达到 `70` 以上时应视为大量退出或信号已经较晚。 |

#### 标签和附加属性

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `tokenTag` | object | 按类别分组的标签，例如 `Launch Platform`、`Sensitive Events`、`Wash Trading Behavior`。每类通常是 `{tagName, languageKey}` 数组。 |
| `isAlpha` | boolean | 是否为 Alpha 代币。 |
| `alphaPoint` | number/null | Alpha 积分。 |
| `launchPlatform` | string/null | 发射平台，例如 `Pumpfun`、`Moonshot`。 |
| `isExclusiveLaunchpad` | boolean | 是否为独家发射平台代币。 |
| `mark` | string/null | 页面标记，字段可能为空。 |

示例 `tokenTag`：

```json
{
  "Launch Platform": [
    { "tagName": "Pumpfun", "languageKey": "wmp-label-title-pumpfun" }
  ],
  "Sensitive Events": [
    { "tagName": "Smart Money Add Holdings", "languageKey": "wmp-label-title-smart-money-add-position" }
  ],
  "Wash Trading Behavior": [
    { "tagName": "Insider Wash Trading", "languageKey": "wmp-label-title-insider-wash-trading" }
  ]
}
```

### 1.5 curl 示例

```bash
curl --location 'https://web3.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/web/signal/smart-money/ai' \
  --header 'Content-Type: application/json' \
  --header 'Accept-Encoding: identity' \
  --header 'User-Agent: binance-web3/3.0 (Skill)' \
  --data '{"chainId":"CT_501","page":1,"pageSize":50,"smartSignalType":"SMART_MONEY"}'
```

### 1.6 Python 示例

```python
from decimal import Decimal
import requests

URL = "https://web3.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/web/signal/smart-money/ai"

resp = requests.post(
    URL,
    headers={
        "Content-Type": "application/json",
        "Accept-Encoding": "identity",
        "User-Agent": "coinx/1.0",
    },
    json={
        "chainId": "56",
        "page": 1,
        "pageSize": 100,
        "smartSignalType": "SMART_MONEY",
    },
    timeout=10,
)
resp.raise_for_status()
body = resp.json()
if body.get("code") != "000000":
    raise RuntimeError(body.get("message") or "Binance business error")

for signal in body.get("data", []):
    max_gain = signal.get("maxGain")
    display_gain = None if max_gain is None else Decimal(max_gain) * 100
    print(
        signal["ticker"],
        signal["direction"],
        f"smart_money={signal['smartMoneyCount']}",
        f"max_gain={display_gain}%" if display_gain is not None else "max_gain=pending",
        signal.get("status"),
    )
```

## 2. 聪明钱净流入排名

这个接口回答的是“哪个代币在一段时间内获得了更多聪明钱净流入”，不是单次触发信号。

### 2.1 接口地址

```text
POST https://web3.binance.com/bapi/defi/v1/public/wallet-direct/tracker/wallet/token/inflow/rank/query/ai
```

请求体：

```json
{
  "chainId": "56",
  "period": "24h",
  "tagType": 2
}
```

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `chainId` | string | `56` = BSC，`CT_501` = Solana，当前实现还验证过 `8453` = Base。 |
| `period` | string | 统计窗口，常用 `5m`、`1h`、`4h`、`24h`。 |
| `tagType` | integer | 当前聪明钱标签使用 `2`。建议固定传 `2`，不要省略。 |

### 2.2 响应结构

外层仍然是：

```json
{
  "code": "000000",
  "message": null,
  "messageDetail": null,
  "data": [],
  "success": true
}
```

`data[]` 常见字段：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `tokenName` | string | 代币名称。 |
| `tokenIconUrl` | string | 图标相对路径，前缀同上。 |
| `ca` | string | 合约地址。 |
| `price` / `marketCap` | string | 当前价格 / 市值。 |
| `volume` | string | 统计窗口内交易量。 |
| `priceChangeRate` | string | 统计窗口内价格变化百分比。 |
| `liquidity` | string | 流动性。 |
| `holders` / `kycHolders` | string/null | 持有人数 / KYC 持有人数。 |
| `holdersTop10Percent` | string | Top 10 持仓集中度。 |
| `count` | string | 窗口内交易笔数。 |
| `countBuy` / `countSell` | string | 买入 / 卖出交易笔数。 |
| `inflow` | number | 聪明钱净流入金额，USD；正数为净流入，负数为净流出。 |
| `traders` | integer | 参与该代币交易的聪明钱地址数量。 |
| `launchTime` | integer | 代币发行时间，Unix 毫秒。 |
| `tokenDecimals` | integer | 代币精度。 |
| `tokenRiskLevel` | integer | 实测可能为 `-1` 未知，或 `0-3` 风险等级；具体展示建议以返回值和标签为准。 |
| `tokenRiskCodes` | array | 风险代码，例如 `mintable`、`metadata_mutable`。 |
| `link` | array/null | 社交链接数组，元素通常为 `{label, link}`。 |
| `previewLink` | object | 按 `website`、`x`、`telegram` 分类的链接。 |
| `tokenTag` | object | 分类标签。 |

请求示例：

```bash
curl --location 'https://web3.binance.com/bapi/defi/v1/public/wallet-direct/tracker/wallet/token/inflow/rank/query/ai' \
  --header 'Content-Type: application/json' \
  --header 'Accept-Encoding: identity' \
  --header 'User-Agent: binance-web3/3.0 (Skill)' \
  --data '{"chainId":"56","period":"24h","tagType":2}'
```

建议排序和过滤：

```text
优先观察：inflow > 0 且 traders >= 2
谨慎处理：liquidity 很低、holdersTop10Percent 很高、tokenRiskLevel >= 3
买卖方向：countBuy、countSell 只反映交易笔数，不能直接等价为 USD 净流入
```

## 3. 地址级数据和实时推送

### 3.1 重要限制

`smart-money/ai` 信号接口不会返回聪明钱钱包地址，只返回：

```text
smartMoneyCount = 触发该信号的地址数量
```

如果业务需要“哪个地址买了什么、买入金额、交易哈希”，应使用 Binance Wallet Tracker 的公开 `SMY` 数据模式，而不是从信号接口猜地址：

```text
# 需要安装 Binance Agentic Wallet CLI，公开 SMY 查询不需要登录
baw tracker token query -c 56 --tag-type smy --json
baw tracker tx query -c 56 --tag-type smy --json
```

官方 CLI 文档给出的地址级字段包括：

```json
{
  "address": "0x...",
  "label": null,
  "buyCount": 2,
  "sellCount": 1,
  "inflow": 123.45,
  "tokenQty": "1000000",
  "latestTxTime": 1791813568000
}
```

交易明细常见字段包括：

```json
{
  "chainId": "56",
  "txHash": "0x...",
  "logId": "...",
  "address": "0x...",
  "ts": 1791813568,
  "tradeSideCategory": 19,
  "txUsdValue": "3262.84",
  "tokenPrice": "0.00011",
  "currentPrice": "0.00010",
  "ca": "0x...",
  "tokenName": "TOKEN",
  "marketCap": "1000000"
}
```

`tradeSideCategory` 常见映射：`11` 首次买入，`19` 买入，`21` 清仓，`29` 卖出。`tx` 数据的 `ts` 是 Unix 秒，而 `latestTxTime`、`launchTime` 通常是 Unix 毫秒，不能混用。

### 3.2 WebSocket 语义

实时场景可使用：

```text
baw tracker ws --smy --duration 60 --json
```

这是官方 CLI 对 Binance WSP 的封装。它是状态流，不是“每条消息一个新信号”：同一 `signalId` 可能重复推送价格、最大涨幅和退出率更新。消费端必须：

1. 以 `signalId` 去重并合并状态；
2. 以 `(chainId, contractAddress)` 作为业务层代币身份；
3. 初始推送中 `status`、`currentPrice`、`maxGain`、`exitRate` 可能为空；
4. `alertPrice` 是历史触发价，不能在 `currentPrice` 为空时当作实时下单价。

官方文档没有稳定公开 WSP 的底层 URL 和订阅报文格式，因此不建议直接硬编码 WSS 地址。若不使用 `baw`，应从当前版本包的实现或浏览器 Network 面板确认并锁定版本。

## 4. 合约聪明钱页面接口

如果你说的“聪明钱”是币安合约页面 `合约 -> 聪明钱`，当前网页会调用以下公共 BAPI：

### 4.1 全量列表接口

列表页使用全量接口，不需要先枚举 symbol，也不需要对 symbol 做数量截断：

```text
GET https://www.binance.com/bapi/futures/v1/public/future/smart-money/signal/list?timeRange=30m
```

实测 `timeRange=30m` 返回约 730 条，`1h` 返回约 734 条，`24h`、`7D`、`ALL` 返回约 745 条。数量会随币安页面支持的合约变化，采集端应保存接口实际返回的全部 `data[]`，不要使用 `exchangeInfo` 重新过滤或设置 `max_symbols`。

支持的时间范围及大小写应按页面传递：

```text
30m, 1h, 24h, 7D, ALL
```

例如 `7d` 或 `all` 可能返回业务参数错误，不能自行统一转小写。

请求示例：

```bash
curl --location 'https://www.binance.com/bapi/futures/v1/public/future/smart-money/signal/list?timeRange=30m' \
  --header 'clienttype: web' \
  --header 'User-Agent: Mozilla/5.0'
```

响应示例：

```json
{
  "code": "000000",
  "message": null,
  "messageDetail": null,
  "data": [
    {
      "timeRange": "30m",
      "symbol": "BTCUSDT",
      "side": "BUY",
      "netNotional": 110912348.85873988,
      "longNotional": 85661983.66624987,
      "shortNotional": 25250365.19249,
      "longTraders": 170,
      "longWhales": 89,
      "shortTraders": 167,
      "shortWhales": 59,
      "longQty": 1023.775,
      "shortQty": 301.685,
      "longAvgEntryPrice": 83672.66603135,
      "shortAvgEntryPrice": 83697.7814359
    }
  ],
  "success": true
}
```

`data[]` 常见字段：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `timeRange` | string | 统计窗口，例如 `30m`、`1h`、`24h`、`7D`、`ALL`。 |
| `symbol` | string | 页面返回的合约标识；以接口实际返回为准。 |
| `side` | string | 主导方向，常见为 `BUY` 或 `SELL`。 |
| `netNotional` | number/string | 窗口内净名义价值。 |
| `longNotional` / `shortNotional` | number/string | 多空名义价值。 |
| `longTraders` / `shortTraders` | integer | 多空聪明钱交易员数。 |
| `longWhales` / `shortWhales` | integer | 多空鲸鱼数，鲸鱼包含在交易员口径中。 |
| `longQty` / `shortQty` | number/string | 多空数量。 |
| `longAvgEntryPrice` / `shortAvgEntryPrice` | number/string | 多空平均开仓价。 |

这里的数字当前实测以 JSON number 返回，但持久化时仍建议按 Decimal/字符串处理，避免精度损失。这个接口是列表页持久化的主入口。

### 4.2 overview 响应

```text
GET https://www.binance.com/bapi/futures/v1/public/future/smart-money/signal/overview?symbol=BTCUSDT
```

建议请求头：

```http
User-Agent: Mozilla/5.0
clienttype: web
referer: https://www.binance.com/zh-CN/smart-money/signal/BTCUSDT
```

它不需要 API Key，但属于网页内部接口。请求示例：

```bash
curl --location 'https://www.binance.com/bapi/futures/v1/public/future/smart-money/signal/overview?symbol=BTCUSDT' \
  --header 'clienttype: web' \
  --header 'User-Agent: Mozilla/5.0'
```

```json
{
  "code": "000000",
  "message": null,
  "messageDetail": null,
  "data": {
    "symbol": "BTCUSDT",
    "totalPositions": 2807505175.5107707,
    "totalTraders": 2740,
    "longShortRatio": 2.7073,
    "longTraders": 1872,
    "longTradersQty": 24601.806,
    "longTradersAvgEntryPrice": 82356.91124199,
    "longWhales": 486,
    "longWhalesQty": 22888.131,
    "longWhalesAvgEntryPrice": 82387.92833612,
    "shortTraders": 868,
    "shortTradersQty": 9087.336,
    "shortTradersAvgEntryPrice": 81509.10277123,
    "shortWhales": 243,
    "shortWhalesQty": 8266.42,
    "shortWhalesAvgEntryPrice": 81521.04922137,
    "longProfitTraders": 1363,
    "shortProfitTraders": 262,
    "longProfitWhales": 335,
    "shortProfitWhales": 52,
    "updateTime": 1791785458204
  },
  "success": true
}
```

| 字段 | 说明 |
| --- | --- |
| `totalTraders` | 聪明钱交易员总数。 |
| `longTraders` / `shortTraders` | 多头 / 空头交易员数。 |
| `longTradersQty` / `shortTradersQty` | 多头 / 空头数量，通常是合约标的数量，不是 USD；换算 USD 需要乘以当前价格。 |
| `longTradersAvgEntryPrice` / `shortTradersAvgEntryPrice` | 多空平均开仓价。 |
| `longWhales` / `shortWhales` | 多空鲸鱼数量。 |
| `longWhalesQty` / `shortWhalesQty` | 多空鲸鱼持仓数量。 |
| `longProfitTraders` / `shortProfitTraders` | 当前盈利的多空交易员数量。 |
| `longProfitWhales` / `shortProfitWhales` | 当前盈利的多空鲸鱼数量。 |
| `longShortRatio` | 页面定义的聪明钱多空比，实测为 `longTraders / shortTraders` 附近的比例。 |
| `totalPositions` | 页面内部仓位数值，单位和口径未在公开文档中稳定说明，不建议直接当作 USD。 |
| `updateTime` | Unix 毫秒。 |

### 4.3 30 分钟变化

```text
GET https://www.binance.com/bapi/futures/v1/public/future/smart-money/signal/details/stats?symbol=BTCUSDT&timeRange=30m
```

响应的 `data` 常见结构：

```json
{
  "longTraders": 175,
  "longWhales": 109,
  "longPositions": 23237759.26629,
  "longQty": 278.934,
  "longWhalePositions": 21924067.91549,
  "longWhaleQty": 263.168,
  "shortTraders": 121,
  "shortWhales": 54,
  "shortPositions": 16805146.96914,
  "shortQty": 201.716,
  "shortWhalePositions": 11656630.81764,
  "shortWhaleQty": 139.897
}
```

它更适合与同一合约的 `overview` 做差分，判断过去 30 分钟聪明钱是在增加多头还是增加空头，而不是单独解释为净流入金额。

### 4.3 CoinX 展示接口

采集任务将 `30m` 和 `ALL` 全量列表持久化后，由 CoinX 页面读取最新快照；页面默认展示 `ALL`，可切换到 `30m`。页面入口：

```text
GET /smart-money
```

页面数据接口：

```text
GET /api/smart-money
```

支持参数：

| 参数 | 说明 |
| --- | --- |
| `keyword` | 按合约符号模糊搜索，例如 `BTC`。 |
| `side` | `BUY`、`SELL` 或空值。 |
| `min_net_notional` | 净名义绝对值下限，使用 `ABS(net_notional) > min_net_notional`；页面默认 `10000000`（10M），可传自定义数值。 |
| `sort_by` | `long_short_ratio`（多空名义比）、`net_notional`、`long_notional`、`short_notional`、`long_whales`、`short_whales`、`symbol`。 |
| `sort_order` | `asc` 或 `desc`。 |
| `page` / `page_size` | 页面分页参数，`page_size` 最大 500；不影响采集全量保存。 |
| `snapshot_time` | 可选的本地快照毫秒时间戳；不传时读取符合筛选条件的最新快照。 |
| `time_range` | `ALL` 或 `30m`，默认 `ALL`；必须与采集时保存的窗口一致。 |

返回结构示例：

```json
{
  "status": "success",
  "data": [],
  "total_count": 0,
  "snapshot_time": 1791850500000,
  "snapshots": [1791850500000],
  "stats": {
    "net_notional": 0,
    "long_notional": 0,
    "short_notional": 0
  }
}
```

页面只返回展示所需的标准化字段，不返回 `raw_json`，避免把完整原始载荷直接渲染到浏览器。页面分页只是控制渲染数量，数据库仍保留每次接口实际返回的全部币种。

列表中的 `price_trend` 来自本地 `market_klines` 的 Binance `5m` K 线：读取最近 7 天，并按 1 小时保留每小时最后收盘价，最多返回 168 个点/币种。原始 5m 数据为 2016 根/币种；页面每页 10 个币种时，接口返回约 1680 个走势点。

## 5. 推荐采集方案

### 5.1 链上信号入库

```text
按 chainId 分开请求
        |
        v
smart-money/ai 分页拉取（pageSize <= 100）
        |
        v
按 signalId 去重，保留 raw JSON
        |
        v
按 (chainId, contractAddress) 聚合当前信号状态
        |
        v
关联 token-risk / liquidity / holders 后再做策略筛选
```

推荐规范化字段：

```json
{
  "source": "binance_web3_smart_money",
  "chain_id": "56",
  "signal_id": 60109,
  "contract_address": "0x...",
  "symbol": "OKT",
  "signal_type": "SMART_MONEY",
  "direction": "buy",
  "smart_money_count": 9,
  "signal_trigger_time_ms": 1791813568000,
  "alert_price_usd": "0.000111959709554197",
  "current_price_usd": "0.000003700519600108445249744234065968",
  "max_gain_ratio": "0",
  "exit_rate": 96,
  "status": "outDecline",
  "raw_payload": {}
}
```

价格、金额和收益率字符串应使用 Decimal/高精度类型处理；不要先转 binary float 再落库。`signalTriggerTime`、`highestPriceTime` 使用毫秒，地址交易流的 `ts` 使用秒，要在字段名中明确单位。

### 5.2 轮询和去重

- 普通 REST 拉取建议按 30-60 秒或更长间隔，不要并发扫描所有链和所有页。
- 以 `signalId` 做事件去重；同一信号状态更新时执行 upsert。
- 业务告警不要只依赖 `status == valid`：初始事件可能是 `null`。
- `smartMoneyCount >= 5` 只能作为强度特征，不能等价于盈利概率。
- `exitRate >= 70`、`status` 为 `timeout`/`outDecline`/`exitRate` 时，应降低优先级或标记为已过期。
- 页面接口返回 403/418/429 时尊重 `Retry-After`，不要立即重试；对响应做本地缓存和指数退避。

### 5.3 与项目现有数据结合

如果要接入 CoinX 的评分或告警流程，建议把聪明钱作为独立数据源，不直接替代价格、OI、资金费率：

```text
Binance smart money signal
  -> signal freshness / consensus / exit state
  -> token liquidity / risk / holder concentration
  -> existing price and market structure
  -> alert or opportunity score
```

尤其需要排除：低流动性、Top10 高度集中、存在高税/可铸造/元数据可变等风险标签的代币。

## 6. 错误和兼容性

常见业务码：

| `code` | 含义 |
| --- | --- |
| `000000` | 成功。 |
| `100004` | 请求频率过高。 |
| `100002` | 参数错误，重点检查 `chainId`、分页和 `pageSize`。 |
| `000400` | 当前链不支持或参数不被后端接受。 |

实现上应同时处理：

1. HTTP 非 2xx；
2. HTTP 200 但 `code != "000000"`；
3. `data` 为 `null`、空数组或字段缺失；
4. 网关返回 HTML/文本而不是 JSON；
5. 403、418、429 和超时；
6. 字段枚举新增，例如新的 `status` 或 `tradeSideCategory`。

不要把这些 BAPI 当成可长期依赖的公开 SLA 接口。生产系统应保存最近一次成功快照，并在接口失败时标记数据陈旧，而不是把陈旧数据当作实时信号继续下单。

## 7. 来源

- [Binance：币安钱包推出交易信号功能](https://www.binance.com/zh-CN/support/announcement/detail/bf9594f9034a40369399e1dacb324f15)
- [Binance Skills Hub：trading-signal](https://github.com/binance/binance-skills-hub/tree/main/skills/binance-web3/binance-trading-signal)
- [Binance Skills Hub：trading-signal CLI reference](https://github.com/binance/binance-skills-hub/blob/main/skills/binance-web3/binance-trading-signal/references/cli.md)
- [Binance Skills Hub：crypto-market-rank](https://github.com/binance/binance-skills-hub/tree/main/skills/binance-web3/crypto-market-rank)
- [Binance：如何使用币安合约聪明钱](https://www.binance.com/zh-CN/support/faq/detail/8daa1c8c0a114193bbf0d4743db635a7)
- [网页合约聪明钱接口的社区逆向实现](https://github.com/andychien555/binance-smart-money-tracker)
