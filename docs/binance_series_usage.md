# Binance 历史序列使用说明

## 1. 功能范围

当前项目已经支持以下 Binance 历史序列接口的采集、解析与入库：

- `top_long_short_position_ratio`
- `top_long_short_account_ratio`
- `open_interest_hist`
- `klines`
- `global_long_short_account_ratio`

对应数据表：

- `binance_top_long_short_position_ratio`
- `binance_top_long_short_account_ratio`
- `binance_open_interest_hist`
- `binance_klines`
- `binance_global_long_short_account_ratio`

## 2. 手动脚本采集

单个序列采集脚本：

```bash
python scripts/fetch_binance_series.py klines --symbol BTCUSDT --period 5m --limit 20
python scripts/fetch_binance_series.py open_interest_hist --symbol BTCUSDT --period 5m --limit 20
python scripts/fetch_binance_series.py top_long_short_position_ratio --symbol BTCUSDT --period 5m --limit 20
```

参数说明：

- `series_type`：序列类型
- `symbol`：交易对，例如 `BTCUSDT`
- `period`：周期，例如 `5m`、`15m`、`1h`
- `limit`：本次抓取条数

## 3. 手动 API 采集

### 3.1 单个序列采集

接口：

- `POST /api/binance-series/collect`

示例：

```bash
curl -X POST http://127.0.0.1:5000/api/binance-series/collect \
  -H "Content-Type: application/json" \
  -d "{\"series_type\":\"klines\",\"symbol\":\"BTCUSDT\",\"period\":\"5m\",\"limit\":20}"
```

请求体：

```json
{
  "series_type": "klines",
  "symbol": "BTCUSDT",
  "period": "5m",
  "limit": 20
}
```

### 3.2 批量序列采集

接口：

- `POST /api/binance-series/batch-collect`

示例：

```bash
curl -X POST http://127.0.0.1:5000/api/binance-series/batch-collect \
  -H "Content-Type: application/json" \
  -d "{\"symbols\":[\"BTCUSDT\",\"ETHUSDT\"],\"periods\":[\"5m\",\"1h\"],\"series_types\":[\"klines\",\"open_interest_hist\"],\"limit\":10}"
```

请求体：

```json
{
  "symbols": ["BTCUSDT", "ETHUSDT"],
  "periods": ["5m", "1h"],
  "series_types": ["klines", "open_interest_hist"],
  "limit": 10
}
```

说明：

- `symbols`：要采集的交易对列表
- `periods`：要采集的周期列表
- `series_types`：要采集的序列类型列表，不传时默认采集全部支持类型
- `limit`：每个接口单次抓取条数

## 4. 定时采集配置

在 `application.yml` 中新增了 `app.binance_series` 配置：

```yaml
app:
  binance_series:
    enabled: false
    interval: 300
    limit: 30
    types:
      - top_long_short_position_ratio
      - top_long_short_account_ratio
      - open_interest_hist
      - klines
      - global_long_short_account_ratio
    periods:
      - 5m
      - 15m
```

字段说明：

- `enabled`：是否开启定时采集
- `interval`：定时采集间隔，单位秒
- `limit`：每次请求的抓取条数
- `types`：要采集的序列类型
- `periods`：要采集的周期

## 5. 当前已验证内容

已经完成以下验证：

- 单元测试通过
- 仓储幂等写入测试通过
- 集成测试通过
- API 测试通过
- 真实 MySQL 建表成功
- 真实接口抓取成功
- 重复抓取后记录数保持稳定，幂等生效

## 6. 备注

- `limit` 是抓取参数，不是业务主键字段
- 幂等唯一键设计：
  - ratio / open interest：`symbol + period + event_time`
  - klines：`symbol + period + open_time`
- 若后续新增 Binance 历史接口，继续保持“一接口一张表”的策略
