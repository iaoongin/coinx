# Binance 历史序列修补方案（第一阶段，TDD）

> 状态说明：本文记录的是第一阶段的 tail-only 修补方案，当前实现已经升级为 coverage-aware repair。涉及首页长周期覆盖、`coverage_hours`、固定时间窗分页等最新口径，请以 [docs/binance_series_coverage_repair_plan.md](/z:/Resource/Code/project/coinx/docs/binance_series_coverage_repair_plan.md) 为准。

## 1. 目标

第一阶段只处理“已跟踪币种”的历史序列修补，目标是补齐由于系统停机、任务中断等原因造成的尾部缺失数据。

本阶段约束如下：

- 只处理 `tracked coins`
- 只处理 `5m` 周期
- 默认修补最近 `7` 天
- 只修补尾部缺口
- 采用 `TDD` 开发模式

## 2. 适用序列

本阶段修补以下 5 类序列：

- `klines`
- `open_interest_hist`
- `top_long_short_position_ratio`
- `top_long_short_account_ratio`
- `global_long_short_account_ratio`

## 3. 范围说明

### 3.1 本阶段要做的事

- 从 `get_active_coins()` 获取已跟踪币种
- 页面统一放在 `/binance-series`，但明确区分“手动采集”和“缺口修补”
- 按 `5m` 周期修补序列数据
- 本地无数据时，默认补最近 `7` 天
- 本地有数据时，只补最后一条之后的尾部缺口
- 对 Binance 接口做分页抓取
- 使用现有幂等入库逻辑避免重复数据
- 保持低并发和简单限流，避免流控

### 3.2 本阶段暂不做

- 不处理 `15m / 30m / 1h` 等周期
- 不处理全市场币种
- 不处理交易量筛选 watchlist
- 不扫描历史中间断洞
- 不做复杂任务编排
- 不做高并发大规模回补

### 3.3 采集与修补边界

- `collect`
  - 保留为手动能力
  - 用于验证单接口、补单币、临时抓取指定参数
  - 不再接入定时任务
- `repair`
  - 保留为规则化修补能力
  - 只处理 tracked coins 的 `5m` 尾部缺口
  - 允许手动触发，也允许定时任务触发

## 4. 配置设计

建议在 `application.yml` 中新增：

```yaml
app:
  binance_series:
    repair:
      enabled: true
      interval: 900
      period: 5m
      bootstrap_days: 7
      klines_page_limit: 1000
      futures_page_limit: 500
      sleep_ms: 500
```

字段说明：

- `enabled`
  - 是否启用修补任务
- `interval`
  - 修补任务的执行频率，单位秒
- `period`
  - 第一阶段固定为 `5m`
- `bootstrap_days`
  - 当本地没有数据时，默认回补最近几天
- `klines_page_limit`
  - `klines` 接口分页大小，建议 `1000`
- `futures_page_limit`
  - `futures/data/*` 接口分页大小，建议 `500`
- `sleep_ms`
  - 每页请求后的休眠时间，避免撞到限流

## 5. 修补逻辑设计

### 5.1 修补对象

修补对象来自：

- `coinx.coin_manager.get_active_coins()`

也就是当前已经标记为跟踪的币种。

### 5.2 修补终点

修补终点不应取当前时间，而应取最近一个完整的 `5m` 时间点。

例如：

- 当前时间是 `10:07`
- 那么本次最多补到 `10:05`

### 5.3 修补起点

对于每个：

- `symbol`
- `series_type`

分别查询本地最新时间：

- `klines`：取 `max(open_time)`
- 其他 4 个序列：取 `max(event_time)`

规则如下：

- 如果本地没有数据
  - 起点 = `target_end_time - 7天`
- 如果本地有数据
  - 起点 = `last_local_time + 5m`

### 5.4 修补窗口

若：

- `start_time > target_end_time`

则说明当前已追平，本次不需要修补。

### 5.5 分页策略

分页建议：

- `klines`
  - 每页 `1000`
- `openInterestHist`
  - 每页 `500`
- `topLongShortPositionRatio`
  - 每页 `500`
- `topLongShortAccountRatio`
  - 每页 `500`
- `globalLongShortAccountRatio`
  - 每页 `500`

每页处理流程：

1. 发请求
2. 解析响应
3. 幂等入库
4. 推进游标
5. `sleep_ms`
6. 继续下一页直到补完

## 6. 核心函数设计

建议新增模块：

- `src/coinx/collector/binance/repair.py`

建议函数：

### 6.1 `floor_to_completed_5m(now_ms)`

作用：

- 将当前时间对齐到最近一个完整 `5m` 时间点

### 6.2 `get_latest_series_timestamp(symbol, series_type, session=None)`

作用：

- 查询本地指定币种、指定序列的最后时间

规则：

- `klines` 使用 `open_time`
- 其他使用 `event_time`

### 6.3 `build_repair_window(symbol, series_type, now_ms, session=None)`

作用：

- 计算本次修补窗口

返回：

- `start_time`
- `end_time`
- `has_gap`

### 6.4 `repair_single_series(symbol, series_type, session=None)`

作用：

- 修补一个币种的一个序列

内部负责：

- 查询窗口
- 分页拉取
- 幂等写入
- 返回修补摘要

### 6.5 `repair_tracked_symbols(symbols=None, series_types=None)`

作用：

- 遍历已跟踪币种，批量执行修补

### 6.6 `run_series_repair_job()`

作用：

- 调度入口

## 7. 执行顺序

每个币种建议按以下顺序修补：

1. `klines`
2. `open_interest_hist`
3. `global_long_short_account_ratio`
4. `top_long_short_position_ratio`
5. `top_long_short_account_ratio`

原因：

- `klines` 和 `open_interest_hist` 是最基础的时间序列
- ratio 类可以放后面

## 8. 限流策略

第一阶段优先保证稳定性，不追求高并发。

建议：

- 总并发 = `1`
- `klines_page_limit = 1000`
- `futures_page_limit = 500`
- 每页请求后 `sleep 500ms`

说明：

- `klines` 的 request weight 会随 `limit` 增长
- 其他 `futures/data/*` 接口文档有 `1000 requests / 5min` 的限制

第一阶段只处理 tracked coins，采用低并发足够。

## 9. TDD 开发计划

### 9.1 新增测试文件

建议新增：

- `tests/test_binance_series_repair_window.py`
- `tests/test_binance_series_repair_service.py`
- `tests/test_binance_series_repair_integration.py`

### 9.2 第一组失败测试：时间窗口

覆盖：

- `5m` 完整时间对齐
- 无本地数据时窗口 = 最近 `7天`
- 有本地数据时从最后一条之后开始
- 没缺口时不应继续修补

### 9.3 第二组失败测试：本地最新时间

覆盖：

- `klines` 取 `open_time`
- 其他序列取 `event_time`
- 空表返回 `None`

### 9.4 第三组失败测试：单序列分页修补

覆盖：

- `klines` 多页分页
- `futures/data/*` 多页分页
- 每页数据都能写库
- 重复修补不插重

### 9.5 第四组失败测试：tracked coins 修补

覆盖：

- 只处理 `get_active_coins()` 返回的币种
- 支持遍历全部 5 个序列

### 9.6 第五组失败测试：修补任务入口

覆盖：

- `enabled = false` 时不执行
- `enabled = true` 时执行
- 单个序列异常不导致整个任务崩溃

## 10. 实施步骤

### 阶段 A：配置接入

修改：

- `src/coinx/config.py`
- `application.yml`

输出：

- repair 配置项

### 阶段 B：窗口计算与本地最新时间

新增：

- `src/coinx/collector/binance/repair.py`

实现：

- `floor_to_completed_5m`
- `get_latest_series_timestamp`
- `build_repair_window`

### 阶段 C：单序列修补

实现：

- `repair_single_series`

### 阶段 D：批量修补 tracked coins

实现：

- `repair_tracked_symbols`

### 阶段 E：调度与手动触发

修改：

- `src/coinx/scheduler.py`
- 可选补一个 API，例如：
  - `POST /api/binance-series/repair-tracked`

### 阶段 F：集成验证

验证：

- 无数据时补最近 `7天`
- 有数据时只补尾部
- 重跑不插重

## 11. 验收标准

完成后应满足：

- 只修补 tracked coins
- 只修补 `5m`
- 无数据时默认补最近 `7天`
- 有数据时只补尾部缺口
- 5 类序列全部支持
- 分页抓取有效
- 幂等入库有效
- 具备基础限流
- 具备手动触发能力
- 有单元测试、服务测试、集成测试

## 12. 建议提交节奏

建议拆成 3 个提交：

1. `feat(binance): 新增历史序列修补配置与窗口计算`
2. `feat(binance): 新增 tracked coins 5m 修补服务`
3. `feat(binance): 接入历史序列修补任务与测试`
