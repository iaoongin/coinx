# 首页切换到历史序列数据方案（TDD）

## 1. 目标

把首页 `/` 当前依赖的快照链路，切换为**直接基于 Binance 历史序列表**构建数据，不再依赖：

- `market_snapshots`
- `load_all_coins_data()`
- `save_all_coins_data()`
- 首页专用的旧缓存文件链路

本次切换采用“纯序列方案”，不做混合兜底。首页展示所需数据统一从以下历史序列表推导：

- `binance_open_interest_hist`
- `binance_klines`

其中：

- 持仓量、持仓价值来自 `binance_open_interest_hist`
- 价格、区间价格变化、24h 变化、净流入来自 `binance_klines`

本次实施采用 **TDD（Test-Driven Development）** 推进：

1. 先补失败测试，明确首页新口径
2. 再写最小实现，让测试通过
3. 最后做必要重构，保持接口稳定

## 2. 当前现状

### 2.1 首页当前数据链路

当前首页接口：

- `GET /api/coins`

现有实现链路：

1. `src/coinx/web/routes/api_data.py`
2. `src/coinx/data_processor.py`
3. `src/coinx/utils.py`
4. `market_snapshots`

也就是说，首页现在读取的是“页面聚合快照”，不是 `binance_series` 结构化历史表。

### 2.2 当前数据库观察结果

本地检查时间：`2026-03-31`

检查结果：

- `market_snapshots` 最新批次时间：`2026-03-31 17:26:53.960`
- 该批次首页快照币种数：`3`
- `binance_open_interest_hist` 最新 `5m` 数据时间：`2026-03-31 17:20:00`
- `binance_klines` 最新 `5m` 数据时间：`2026-03-31 17:20:00`

当前 tracked 币种：

- `AIOTUSDT`
- `CYSUSDT`
- `TAUSDT`

这 3 个 tracked 币种在首页所需的核心历史表中已经具备可用数据，因此切换具备实施条件。

## 3. 改造原则

### 3.1 首页只认历史序列

切换完成后，首页及其刷新行为只认历史序列表，不再读取旧快照表。

### 3.2 保持前端接口结构尽量不变

尽量保持 `GET /api/coins` 返回结构不变，避免大规模修改首页模板和 JS。

目标是只替换后端数据来源，而不是重做首页协议。

### 3.3 基于 `5m` 基础序列推导全部区间

当前 `application.yml` 中 `binance_series.periods` 只配置了：

- `5m`

因此首页的区间指标统一基于 `5m` 历史序列推导，不依赖额外的 `15m / 30m / 1h / 24h / 168h` 独立 period 表。

### 3.4 缺数据时不回退旧链路

若某个币种缺少足够历史数据：

- 该币种字段返回 `N/A` 或 `None`
- 必要时跳过该币种
- 不再回退到旧快照 / 实时接口

### 3.5 先测试后实现

本方案所有关键改造都按以下顺序进行：

1. 先为目标行为补测试
2. 观察测试失败，确认测试命中改造点
3. 编写最小实现使测试通过
4. 进行小步重构
5. 保持每一阶段都可独立验证

## 4. 首页字段映射方案

### 4.1 顶部当前值

首页每个币种当前展示字段映射如下：

- `current_open_interest`
  - 取 `binance_open_interest_hist` 最新 `5m` 记录的 `sum_open_interest`
- `current_open_interest_value`
  - 取最新 `5m` 记录的 `sum_open_interest_value`
- `current_price`
  - 取 `binance_klines` 最新 `5m` 记录的 `close_price`

### 4.2 区间变化列

首页当前配置区间：

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

统一按以下方式计算：

1. 以当前最新 `5m` 时间点作为基准时间
2. 向前回溯对应区间
3. 取该区间目标时间点对应的历史记录
4. 与当前值比较，生成变化结果

#### 持仓量变化

字段：

- `ratio`
- `open_interest`
- `open_interest_formatted`

数据源：

- `binance_open_interest_hist`

#### 持仓价值变化

字段：

- `value_ratio`
- `open_interest_value`
- `open_interest_value_formatted`

数据源：

- `binance_open_interest_hist`

#### 价格变化

字段：

- `price_change`
- `price_change_percent`
- `price_change_formatted`
- `current_price`
- `current_price_formatted`

数据源：

- `binance_klines.close_price`

### 4.3 24h 变化

首页顶层字段：

- `price_change`
- `price_change_percent`

改为基于 `5m` K 线推导：

- 当前价格 = 最新 `close_price`
- 24h 前价格 = 向前 24 小时对应 `5m` K 线的 `close_price`
- 差值 = 当前价格 - 24h 前价格
- 百分比 = 差值 / 24h 前价格

### 4.4 主力净流入

继续沿用当前净流入定义，但改为完全从历史 K 线计算：

- `net_inflow = 2 * taker_buy_quote_volume - quote_volume`

区间聚合方式：

- `5m`：取 1 根 `5m` K 线
- `15m`：聚合最近 3 根 `5m` K 线
- `30m`：聚合最近 6 根 `5m` K 线
- `1h`：聚合最近 12 根 `5m` K 线
- 依此类推

## 5. 时间对齐策略

### 5.1 首页基准时间

对每个币种分别确定一个首页基准时间：

- `latest_oi_time = max(event_time)`
- `latest_kline_time = max(open_time)`
- `current_time = min(latest_oi_time, latest_kline_time)`

这样可以确保同一币种首页当前值来自同一个可对齐时间点。

### 5.2 历史点匹配规则

对于区间目标时间：

- 优先取“时间戳恰好匹配”的序列点
- 若不存在，可接受“向前最近一个不超过目标时间的点”
- 但允许的最大偏差建议不超过 1 根 `5m` K 线

超过偏差阈值时，该区间字段返回 `None`，避免错误展示。

### 5.3 页面更新时间

首页返回的 `cache_update_time` 不再来自旧缓存文件或快照表，而是改为：

- 当前所有 tracked 币种中可用于首页展示的最小 `current_time`

这样页面显示的“最后更新时间”能够真实反映首页当前这一屏数据的有效时间。

## 6. 后端改造范围

### 6.1 新增首页序列聚合层

建议新增模块：

- `src/coinx/repositories/homepage_series.py`

职责：

- 查询 tracked 币种的最新 `5m` 历史序列
- 计算首页当前值
- 计算各时间区间变化
- 计算净流入
- 生成与首页现有 JSON 结构兼容的数据
- 计算首页统一更新时间

建议提供的函数：

- `get_homepage_series_snapshot(symbols=None, session=None)`
- `get_homepage_series_data(symbols=None, session=None)`
- `get_homepage_series_update_time(symbols=None, session=None)`

### 6.2 替换首页 API 数据源

修改：

- `src/coinx/web/routes/api_data.py`

替换点：

- `GET /api/coins`

旧行为：

- 读取 `get_all_coins_data()`
- 读取旧 `cache_update_time`

新行为：

- 读取 `get_homepage_series_snapshot()`
- 单次构建后同时返回 `data` 与 `cache_update_time`

### 6.5 首页查询性能优化

首页切换到历史序列后，`GET /api/coins` 的主要开销来自：

- 查询 `binance_open_interest_hist`
- 查询 `binance_klines`
- ORM 加载不必要的大字段，例如 `raw_json`
- 同一请求内重复构建首页快照

因此实现时需要同步遵守以下优化原则：

1. `GET /api/coins` 只允许构建一次首页快照。
2. 首页 repository 只查询首页真正需要的字段，不加载整行模型。
3. 当 tracked 币种数量较少时，优先使用单币种按索引倒序 `limit` 查询。
4. 当 tracked 币种数量较多时，再切换到批量查询路径。
5. 首页聚合层对外暴露统一快照接口，避免 data 和 update_time 分别重复查库。

当前首页实际使用字段如下：

- `open_interest_hist`
  - `symbol`
  - `event_time`
  - `sum_open_interest`
  - `sum_open_interest_value`
- `klines`
  - `symbol`
  - `open_time`
  - `close_price`
  - `quote_volume`
  - `taker_buy_quote_volume`

不应为首页加载的字段包括：

- `raw_json`
- `created_at`
- `updated_at`
- 其它首页未消费的明细列

### 6.3 替换首页手动刷新逻辑

修改：

- `src/coinx/web/routes/api_data.py`

替换接口：

- `GET /api/update`

旧行为：

- 启动线程执行 `update_all_data()`

新行为：

- 启动线程执行针对首页所需序列的补齐任务
- 仅更新首页依赖的序列表

建议刷新范围：

- `klines`
- `open_interest_hist`

建议复用：

- `repair_tracked_symbols()`

并通过 `series_types` 限定为首页必需的序列，避免刷新过重。

### 6.4 替换首页定时更新逻辑

修改：

- `src/coinx/scheduler.py`

旧行为：

- `scheduled_update()` 定时更新旧快照数据

新行为：

- `scheduled_update()` 改为定时补齐首页依赖的历史序列
- `update_drop_list_data()` 保持原有逻辑不动

这样首页每个 5 分钟周期都能持续消费新的历史序列数据。

## 7. 推荐实施步骤

### 阶段 A：补文档和确定口径

输出物：

- 本文档
- 首页字段的序列映射口径
- 时间对齐口径
- TDD 拆分顺序

### 阶段 B：实现首页序列聚合层

新增：

- `src/coinx/repositories/homepage_series.py`

实现内容：

- 时间区间解析工具
- 最近 `5m` 序列加载
- 历史点查找
- 当前值计算
- 区间变化计算
- 净流入聚合计算
- 首页更新时间计算

进入本阶段前，先补 repository 层失败测试。

### 阶段 C：切换首页 API

修改：

- `src/coinx/web/routes/api_data.py`

实现内容：

- `GET /api/coins` 直接调用首页序列聚合层
- 不再引用旧快照处理链路

进入本阶段前，先补 API 层失败测试。

### 阶段 D：切换首页刷新入口

修改：

- `src/coinx/web/routes/api_data.py`

实现内容：

- `GET /api/update` 改为刷新首页所需历史序列
- 保持返回结构不变，避免前端改动

进入本阶段前，先补刷新入口行为测试。

### 阶段 E：切换定时任务

修改：

- `src/coinx/scheduler.py`

实现内容：

- 用历史序列补齐逻辑替代旧首页快照更新逻辑

进入本阶段前，先补 scheduler 行为测试。

### 阶段 F：清理旧首页依赖

可选清理项：

- `src/coinx/data_processor.py` 中旧首页聚合逻辑
- `src/coinx/utils.py` 中旧首页快照读取逻辑
- `market_snapshots` 的首页使用关系说明

这一阶段可以放在功能稳定后再做，避免一次性改动过大。

## 8. 测试计划

### 8.1 单元测试

建议新增：

- `tests/test_homepage_series_repository.py`

覆盖点：

- 基于 `5m` 序列生成首页当前值
- 基于 `5m` 序列生成 `5m/15m/30m/1h...` 变化列
- 24h 价格变化计算正确
- 净流入聚合正确
- 缺少历史点时返回 `None`
- 首页更新时间计算正确

建议先从以下失败测试开始：

1. 能从 `open_interest_hist` 和 `klines` 组装单个币种首页当前值
2. 能基于 `5m` 序列计算 `15m/1h/24h/168h` 区间变化
3. 能基于 `klines` 正确计算净流入
4. 缺少某个历史点时返回 `None` 而不是抛异常
5. 多个币种时能给出统一 `cache_update_time`

### 8.2 API 测试

建议新增：

- `tests/test_homepage_api.py`

覆盖点：

- `GET /api/coins` 返回成功
- `GET /api/coins` 返回结构兼容前端
- `GET /api/coins` 只构建一次首页快照
- `GET /api/update` 触发的是序列刷新线程而不是旧快照线程

建议先写失败测试覆盖：

1. `/api/coins` 不再调用 `get_all_coins_data`
2. `/api/coins` 改为调用 `get_homepage_series_snapshot`
3. `/api/update` 不再调用 `update_all_data`
4. `/api/update` 改为调用首页所需的序列补齐逻辑

### 8.3 联调验证

联调时重点确认：

- 首页能正常展示 tracked 币种
- “最后更新时间”变为序列时间
- 点击刷新后，数据能在补齐完成后更新
- 缺少某一档区间数据时，页面显示 `N/A` 而不是报错

### 8.4 调度测试

建议补充：

- `tests/test_homepage_scheduler.py`

覆盖点：

- `scheduled_update()` 不再调旧快照更新逻辑
- `scheduled_update()` 改为调首页序列补齐逻辑
- `update_drop_list_data()` 保持原有行为

## 9. TDD 分阶段拆解

### 9.1 第一组失败测试：repository 层

目标：

- 固化首页字段映射规则
- 固化时间对齐规则
- 固化净流入计算规则

输出物：

- `tests/test_homepage_series_repository.py`

通过标准：

- 不依赖 Flask 和线程
- 只验证聚合逻辑本身

### 9.2 第二组失败测试：API 层

目标：

- 固化 `/api/coins` 和 `/api/update` 的新调用路径
- 保持接口返回结构兼容

输出物：

- `tests/test_homepage_api.py`

通过标准：

- 首页接口在不启动真实数据库的情况下可验证行为

### 9.3 第三组失败测试：scheduler 层

目标：

- 固化首页定时任务的切换行为

输出物：

- `tests/test_homepage_scheduler.py`

通过标准：

- 能明确区分“旧快照更新”和“新序列补齐”

### 9.4 第四组：联调验证

目标：

- 验证模板端仍能消费新结构
- 验证刷新和更新时间逻辑正确

输出物：

- 手工联调记录
- 必要时补集成测试

### 9.5 开发节奏

推荐每一轮都遵守：

1. 写一小组失败测试
2. 做最小改动让其通过
3. 跑相关测试
4. 再进入下一小组

## 10. 风险与注意事项

### 9.1 时间语义与旧首页不完全一致

旧首页：

- 当前值更接近实时接口

新首页：

- 当前值基于最新已落库的 `5m` 历史点

这意味着首页展示时间会更“规整”，但会比实时接口慢一个完成周期，这是纯序列方案的自然结果。

### 9.2 `5m` 数据量必须覆盖最长区间

首页最长区间为：

- `168h`

因此至少需要最近 168 小时以上的 `5m` `klines` 和 `open_interest_hist` 数据，首页才能完整展示全部列。

### 9.3 序列补齐成为首页主依赖

切换后，首页能否持续更新，取决于：

- 手动刷新是否正确补齐序列
- 定时任务是否持续补齐序列

如果只切查询、不切更新，首页会变成“只读旧序列”，这是本次实施必须避免的。

### 9.4 旧快照链路不要立即删除

建议先“下线首页使用关系”，不要第一时间彻底删除旧代码和旧表依赖。

原因：

- 便于问题回溯
- 便于对比新旧首页结果
- 便于在切换初期快速定位偏差

## 11. 验收标准

完成后应满足：

- 首页 `GET /api/coins` 不再依赖 `market_snapshots`
- 首页所有当前字段和区间字段均来自历史序列表
- 首页更新时间来自历史序列最新有效时间
- 首页接口单次请求不会重复构建两遍首页快照
- 首页查询不会加载 `raw_json` 等非必要大字段
- 首页手动刷新刷新的是历史序列，不是旧快照
- 首页定时更新更新的是历史序列，不是旧快照
- 首页前端无需大改即可正常显示
- 至少具备首页聚合单测和 API 测试
- TDD 过程留有清晰的测试分层
- 相关测试可以独立跑通

## 12. 下一步执行建议

文档确认后，按下面顺序推进：

1. 先补 `tests/test_homepage_series_repository.py`
2. 新增首页序列聚合层
3. 再补 `tests/test_homepage_api.py`
4. 切换 `GET /api/coins`
5. 切换 `GET /api/update`
6. 再补 `tests/test_homepage_scheduler.py`
7. 切换 `scheduled_update()`
8. 联调首页页面

如果后续需要进一步收口，再安排第二轮清理旧首页快照逻辑。
