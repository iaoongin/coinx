# 币种拉取任务 - 跟踪币种 + 成交额前100

> 状态：已完成

## 1. 目标

定时任务独立修补两类币种的历史序列：

1. **跟踪币种** - 从数据库 `is_tracking=True` 获取
2. **成交额前N** - 从 `get_all_24hr_tickers()` 获取，按 `quoteVolume` 排序

## 2. 任务拆分

| 任务ID | 频率 | 范围 | 开关 |
|--------|------|------|------|
| `repair_tracked_job` | 5分钟 | 跟踪币种 | 无（始终执行） |
| `repair_top_volume_job` | 10分钟 | 成交额前100 | `FETCH_COINS_ENABLED` |

### 防堆积配置

两个任务都配置了 `max_instances=1` 和 `coalesce=True`：
- `max_instances=1` - 最多1个实例，避免并发堆积
- `coalesce=True` - 合并堆积的触发

## 3. 代码结构

### scheduler.py

```python
@scheduler.scheduled_job(
    'interval',
    seconds=REPAIR_TRACKED_INTERVAL,
    id='repair_tracked_job',
    max_instances=1,
    coalesce=True
)
def scheduled_repair_tracked():
    """修补跟踪币种历史序列"""
    tracked_coins = get_active_coins()
    repair_tracked_symbols(
        symbols=tracked_coins,
        series_types=list(HOMEPAGE_REQUIRED_SERIES_TYPES),
    )


if FETCH_COINS_ENABLED:
    @scheduler.scheduled_job(
        'interval',
        seconds=FETCH_COINS_INTERVAL,
        id='repair_top_volume_job',
        max_instances=1,
        coalesce=True
    )
    def scheduled_repair_top_volume():
        """修补成交额前N币种历史序列"""
        all_tickers = get_all_24hr_tickers()
        top_volume_symbols = [...]
        repair_tracked_symbols(
            symbols=top_volume_symbols,
            series_types=list(HOMEPAGE_REQUIRED_SERIES_TYPES),
        )
```

## 4. 修补范围

修补的序列类型：`HOMEPAGE_REQUIRED_SERIES_TYPES`
- `klines`
- `open_interest_hist`

## 5. 配置项

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `REPAIR_TRACKED_INTERVAL` | 300 | 跟踪币种任务间隔（秒） |
| `FETCH_COINS_ENABLED` | true | 是否启用成交额前N修补 |
| `FETCH_COINS_INTERVAL` | 600 | 成交额前N任务间隔（秒） |
| `FETCH_COINS_TOP_VOLUME_COUNT` | 100 | 成交额前N数量 |

## 6. 修改文件清单

| 文件 | 改动 |
|------|------|
| `.env` | 新增4个配置项 |
| `.env.example` | 新增4个配置项 |
| `src/coinx/config.py` | 新增4个配置读取 |
| `src/coinx/scheduler.py` | 拆分两个独立任务 |
| `src/coinx/collector/binance/repair.py` | 删除 `filter_symbols` 过滤 |
| `docs/fetch_coins_plan.md` | 新建计划文档 |