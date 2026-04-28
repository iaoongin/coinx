# Binance 历史序列采集与入库方案（TDD）

## 1. 背景

当前项目已经具备以下基础能力：

- 已有 Binance 采集层，位于 `src/coinx/collector/binance/`
- 已有 MySQL + SQLAlchemy 基础设施
- 已有定时任务能力
- 已有 `market_snapshots` 快照表

但目前缺少一套面向 Binance 历史序列接口的“结构化明细落库”方案。现有快照表适合保存页面展示用的整包聚合数据，不适合保存按 `symbol`、`period`、时间序列检索的原始历史数据。

本方案目标是将指定接口的数据按“一个接口一张表”的方式采集并保存到数据库，同时采用 TDD 模式推进开发。

## 2. 目标接口

基础域名：

- `https://proxy.yffjglcms.com/fapi.binance.com`

目标接口：

- `/futures/data/topLongShortPositionRatio`
- `/futures/data/topLongShortAccountRatio`
- `/futures/data/openInterestHist`
- `/fapi/v1/klines`
- `/futures/data/globalLongShortAccountRatio`

本次主要关注的传入参数：

- `symbol`
- `period`
- `limit`

说明：

- 对于 `/fapi/v1/klines`，Binance 原生参数名为 `interval`，在本项目内部统一映射为 `period`
- `limit` 表示本次抓取条数，不属于数据实体本身，不单独存表字段

## 3. 设计原则

### 3.1 一接口一张表

每个接口单独建表，不做混表设计。原因如下：

- 不同接口的业务语义不同
- 字段含义虽然部分相似，但不完全一致
- 后续接口字段变化时，拆表更容易扩展
- 查询、索引、注释都更清晰
- 避免大量无意义的 `NULL` 字段

### 3.2 原始数据与结构化字段同时保留

每张表都建议同时保存：

- 结构化字段：方便 SQL 查询、排序、分析
- `raw_json`：保存接口原始返回，便于排查、回溯和后续扩字段

### 3.3 幂等写入

相同的 `symbol + period + 时间键` 数据重复抓取时，不应插入重复记录，应采用唯一键 + upsert 的方式实现幂等写入。

### 3.4 先测试后实现

本次采用 TDD，先写失败测试，再写最小实现，最后重构。

## 4. 数据表规划

### 4.1 `binance_top_long_short_position_ratio`

对应接口：

- `/futures/data/topLongShortPositionRatio`

核心字段：

- `id`：主键 ID
- `symbol`：交易对
- `period`：周期
- `event_time`：数据时间戳
- `long_short_ratio`：大户持仓量多空比
- `long_account`：大户多头持仓占比
- `short_account`：大户空头持仓占比
- `raw_json`：原始返回数据
- `created_at`：创建时间
- `updated_at`：更新时间

唯一键建议：

- `(symbol, period, event_time)`

### 4.2 `binance_top_long_short_account_ratio`

对应接口：

- `/futures/data/topLongShortAccountRatio`

核心字段：

- `id`
- `symbol`
- `period`
- `event_time`
- `long_short_ratio`：大户账户数多空比
- `long_account`：大户多头账户占比
- `short_account`：大户空头账户占比
- `raw_json`
- `created_at`
- `updated_at`

唯一键建议：

- `(symbol, period, event_time)`

### 4.3 `binance_open_interest_hist`

对应接口：

- `/futures/data/openInterestHist`

核心字段：

- `id`
- `symbol`
- `period`
- `event_time`
- `sum_open_interest`：总持仓量
- `sum_open_interest_value`：总持仓价值
- `cmc_circulating_supply`：CMC 流通供应量
- `raw_json`
- `created_at`
- `updated_at`

唯一键建议：

- `(symbol, period, event_time)`

### 4.4 `binance_klines`

对应接口：

- `/fapi/v1/klines`

核心字段：

- `id`
- `symbol`
- `period`
- `open_time`：开盘时间
- `close_time`：收盘时间
- `open_price`：开盘价
- `high_price`：最高价
- `low_price`：最低价
- `close_price`：收盘价
- `volume`：成交量
- `quote_volume`：成交额
- `trade_count`：成交笔数
- `taker_buy_base_volume`：主动买入成交量
- `taker_buy_quote_volume`：主动买入成交额
- `raw_json`
- `created_at`
- `updated_at`

唯一键建议：

- `(symbol, period, open_time)`

### 4.5 `binance_global_long_short_account_ratio`

对应接口：

- `/futures/data/globalLongShortAccountRatio`

核心字段：

- `id`
- `symbol`
- `period`
- `event_time`
- `long_short_ratio`：全市场多空账户数比
- `long_account`：全市场多头账户占比
- `short_account`：全市场空头账户占比
- `raw_json`
- `created_at`
- `updated_at`

唯一键建议：

- `(symbol, period, event_time)`

## 5. 接口返回字段认知

### 5.1 `topLongShortPositionRatio`

主要返回字段：

- `symbol`
- `longShortRatio`
- `longAccount`
- `shortAccount`
- `timestamp`

说明：

- 字段名中的 `longAccount`、`shortAccount` 来自 Binance 返回命名
- 在本接口语义下，更接近“多头持仓占比”和“空头持仓占比”

### 5.2 `topLongShortAccountRatio`

主要返回字段：

- `symbol`
- `longShortRatio`
- `longAccount`
- `shortAccount`
- `timestamp`

### 5.3 `openInterestHist`

主要返回字段：

- `symbol`
- `sumOpenInterest`
- `sumOpenInterestValue`
- `CMCCirculatingSupply`
- `timestamp`

### 5.4 `klines`

每条 K 线返回为数组，顺序固定：

1. `openTime`
2. `open`
3. `high`
4. `low`
5. `close`
6. `volume`
7. `closeTime`
8. `quoteVolume`
9. `tradeCount`
10. `takerBuyBaseVolume`
11. `takerBuyQuoteVolume`
12. `ignore`

### 5.5 `globalLongShortAccountRatio`

主要返回字段：

- `symbol`
- `longShortRatio`
- `longAccount`
- `shortAccount`
- `timestamp`

## 6. 代码落地点

计划涉及的主要文件：

- `sql/schema.sql`
- `src/coinx/models.py`
- `src/coinx/collector/binance/market.py`
- `src/coinx/collector/binance/service.py`
- `src/coinx/scheduler.py`

建议新增测试目录与文件，例如：

- `tests/test_binance_market_parsers.py`
- `tests/test_binance_series_repository.py`
- `tests/test_binance_series_integration.py`

如有需要，也可以新增仓储层文件：

- `src/coinx/repositories/binance_series.py`

或新增独立脚本：

- `scripts/fetch_binance_series.py`

## 7. TDD 开发节奏

### 7.1 第一阶段：定义数据库契约

先写失败测试，验证：

- 5 张表的模型是否存在
- 关键字段是否存在
- 唯一键是否符合设计
- 表注释和字段注释是否齐全

再补：

- `sql/schema.sql`
- SQLAlchemy 模型定义

### 7.2 第二阶段：解析层 TDD

先为 5 个接口分别准备样例响应，并编写失败测试，验证：

- 字段映射是否正确
- 时间字段是否正确转换
- 小数字段是否正确转数值
- K 线数组是否正确映射为具名字段
- 缺字段时是否能稳定处理

再实现解析函数。

### 7.3 第三阶段：入库层 TDD

先写失败测试，验证：

- 首次写入成功
- 重复写入不插重
- upsert 可以更新同一唯一键的数据
- 不同 `symbol / period / 时间键` 能并存

再实现 repository 或 service 入库逻辑。

### 7.4 第四阶段：采集入口 TDD

先写失败测试，验证统一采集入口可以：

- 根据接口类型调用对应采集函数
- 接收 `symbol`、`period`、`limit`
- 将响应解析后写入正确的表

再实现统一入口。

### 7.5 第五阶段：集成验证

补集成测试，验证完整链路：

- 发起请求
- 获取响应
- 解析结构化数据
- 入库
- 再次抓取时去重生效

## 8. 实施步骤

### 阶段 A：方案落表

输出物：

- 5 张表 SQL
- 5 个模型类
- 中文表注释
- 中文字段注释

### 阶段 B：接口解析

输出物：

- 5 个接口独立采集函数
- 5 组解析测试

### 阶段 C：入库与幂等

输出物：

- 批量 upsert 入库逻辑
- repository 或 service 封装
- 入库测试

### 阶段 D：统一入口

输出物：

- 统一采集入口
- 手动触发脚本或调度接入

### 阶段 E：验收

输出物：

- 集成测试
- 样例数据验证结果
- 使用说明

## 9. 验收标准

完成后应满足以下条件：

- 5 个接口分别有独立数据表
- 所有表和字段都带中文注释
- 统一支持 `symbol`、`period`、`limit`
- 能将接口原始数据成功解析并写入数据库
- 重复抓取同一批数据不会产生重复记录
- K 线数组字段映射准确
- 至少具备单元测试、入库测试、基础集成测试

## 10. 风险与注意事项

- Binance 接口可能存在限流，需要保留重试与退避机制
- `klines` 返回数组，不是对象，最容易发生字段错位
- 多空比接口字段名相似，但业务含义不同，不能混用
- `limit` 只是抓取参数，不应误设计为唯一业务字段
- 若后续要支持更多接口，继续保持“一接口一张表”即可

## 11. 下一步

文档确认后，按以下顺序推进实现：

1. 在 `sql/schema.sql` 中新增 5 张表
2. 在 `src/coinx/models.py` 中新增模型
3. 先补测试骨架
4. 先写解析测试，再实现解析
5. 先写入库测试，再实现 upsert
6. 接入统一入口和调度或脚本
7. 完成集成验证
