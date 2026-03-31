# Binance 历史序列覆盖修补方案（TDD）

## 1. 目标

把现有 Binance 历史序列修补逻辑从“只补最新尾巴”升级为“按覆盖范围自动回补”，确保首页依赖的 `5m` 历史序列能够覆盖以下区间：

- `5m`
- `15m`
- `30m`
- `1h`
- `4h`
- `12h`
- `24h`
- `48h`
- `72h`
- `168h`

本次改造重点是让以下入口都具备“覆盖不足时往前补”的能力：

- 手动修补按钮
- 首页刷新接口
- 首页定时刷新任务

## 2. 当前问题

当前修补窗口的规则是：

- 本地完全没有数据：回补最近 `N` 天
- 本地已经有数据：从 `latest_local_timestamp + 5m` 开始补

这会带来一个明显问题：

1. 某个币最开始可能只用 `limit=300` 采过一次
2. 这样本地只会有大约 25 小时的 `5m` 数据
3. 之后无论手动修补还是定时修补，都只会继续补“最新尾巴”
4. 更早的 `48h / 72h / 168h` 历史永远不会自动补回来

所以现在看到的长周期为空，并不是修补按钮没生效，而是修补逻辑本身不处理“前面的历史覆盖不足”。

## 3. 期望行为

对于首页依赖的 `5m` 序列：

- `klines`
- `open_interest_hist`

修补逻辑应该保证最少覆盖最近 `168h`。

期望规则如下：

1. 如果本地完全没有数据：
   - 按 bootstrap 规则回补，至少满足目标覆盖范围
2. 如果本地最新数据落后：
   - 修补尾巴
3. 如果本地最早数据太晚，无法覆盖 `168h`：
   - 往前回补更早历史
4. 如果头部和尾部都不满足：
   - 从更早的目标起点开始重拉
5. 如果覆盖已经足够且尾巴也最新：
   - 跳过修补

## 4. 范围

### 4.1 本次要做的

- 增加修补覆盖范围配置
- 增加 earliest timestamp 查询能力
- 升级 repair window 计算逻辑
- 让手动修补 / 首页刷新 / 定时刷新都吃到新的覆盖修补能力

### 4.2 本次暂不做的

- 扫描并修复“中间缺几根 5m”的内层空洞
- 非 `5m` period 的覆盖保证
- 全市场全量历史回补

## 5. 设计方案

### 5.1 新增覆盖配置

在配置中新增：

- `app.binance_series.repair.coverage_hours`

默认值：

- `168`

含义：

- 对于首页依赖的 `5m` 历史序列，要求本地至少覆盖最近 168 小时

### 5.2 新增最早时间查询能力

当前 repository 只有“本地最新时间”查询，还需要补充：

- `get_earliest_series_timestamp(series_type, symbol, period='5m', session=None)`

规则：

- `klines` 使用 `open_time`
- 其他序列使用 `event_time`

### 5.3 覆盖感知的修补窗口

修补窗口计算逻辑调整为：

- `target_end_time = floor_to_completed_5m(now_ms)`
- `coverage_start_time = target_end_time - coverage_hours`
- `bootstrap_start_time = target_end_time - bootstrap_days`

然后分情况判断：

1. 本地无数据
   - 从 `min(bootstrap_start_time, coverage_start_time)` 开始补
2. 本地有数据，但最早时间晚于 `coverage_start_time`
   - 说明覆盖不足，需要从更早时间往前补
3. 覆盖足够，但最新时间落后于 `target_end_time`
   - 只补尾巴
4. 覆盖足够且最新时间已追平
   - 跳过

这里允许一定程度的重叠抓取，因为现有 upsert 已经是幂等的。

### 5.5 Futures 历史接口分页约束

对于 `open_interest_hist` 以及其它 futures 历史序列接口，不能假设：

- 给定一个很大的 `startTime ~ endTime`
- 再配合 `limit=500`
- 接口就会从窗口起点开始顺序返回前 500 条

实际修补中需要按“固定时间窗”分页，而不是按“大窗口 + 游标顺推”分页。原因是：

1. futures 历史接口在大窗口下可能优先返回窗口尾部附近的数据；
2. 如果直接依赖返回结果中的最后时间推进游标，会在窗口尾部反复打转；
3. 这样日志看起来像“每次都成功补了 500 条”，但本地最早时间不会前移；
4. 首页依赖的 `48h / 72h / 168h` 目标点就会长期缺失。

因此实现上应改为：

- 每一页先计算固定的 `page_end_time`
- 请求 `startTime=cursor_time, endTime=page_end_time`
- 无论该页是否为空，都按时间窗推进到下一页

这样才能真正把头部历史覆盖补齐，而不是只重复 upsert 窗口尾部的最近数据。

### 5.4 作用入口

新的覆盖修补逻辑应统一作用于：

- `/api/update`
- `/api/binance-series/repair-tracked`
- `scheduled_update()`

这样用户点击手动修补按钮时，就可以真正把首页需要的 `168h` 覆盖补齐。

## 6. TDD 计划

### 6.1 第一组：窗口与时间戳测试

先补失败测试，覆盖：

- earliest timestamp 空表返回 `None`
- `klines` earliest timestamp 使用 `open_time`
- futures 系列 earliest timestamp 使用 `event_time`
- 覆盖不足时 repair window 会往前拉
- 覆盖足够时 repair window 只补尾巴
- 头尾都满足时 repair window 返回无 gap

建议文件：

- `tests/test_binance_series_repair_window.py`

### 6.2 第二组：服务层测试

补失败测试，覆盖：

- `repair_single_series()` 允许从更早时间重抓并幂等 upsert
- `repair_tracked_symbols()` 仍然按 tracked coins 逐个执行
- 手动修补入口实际走的是 coverage-aware repair，而不是旧 tail-only 逻辑

建议文件：

- `tests/test_binance_series_repair_service.py`

### 6.3 第三组：入口验证

由于 API 和 scheduler 已经复用了 repair 逻辑，所以这里只要确认它们继续走 repair 即可，不需要新增全新入口。

重点确认：

- `/api/update`
- `/api/binance-series/repair-tracked`
- `scheduled_update()`

## 7. 实施步骤

### 步骤 A

新增配置：

- `coverage_hours: 168`

涉及文件：

- `application.yml`
- `src/coinx/config.py`

### 步骤 B

新增 repository 能力：

- `get_earliest_series_timestamp()`

涉及文件：

- `src/coinx/repositories/binance_series.py`

### 步骤 C

升级 repair window 计算逻辑：

- `build_repair_window()`

涉及文件：

- `src/coinx/collector/binance/repair.py`

### 步骤 D

保持现有 repair 执行链路不变，但让其消费新的 window 语义。

涉及文件：

- `src/coinx/collector/binance/repair.py`

### 步骤 E

跑定向测试，再跑全量测试。

## 8. 验收标准

完成后应满足：

- 手动修补可以在覆盖不足时自动往前回补
- 首页刷新也能触发这套覆盖修补
- 如果覆盖已经足够，则仍然只补尾巴
- 修补过程保持幂等
- 现有 API 形态不需要改变
- 测试覆盖 earliest timestamp 与 coverage-aware window
