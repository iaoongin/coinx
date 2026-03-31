# CoinX

CoinX 是一个面向 Binance U 本位合约的币种监控项目，提供定时采集、MySQL 落库、Web 页面展示和跟踪币种配置管理。

当前仓库已经可以跑通一套基础闭环：

- 定时抓取合约市场数据
- 展示首页监控面板、币种详情和跌幅榜
- 管理哪些交易对需要跟踪
- 将市场快照写入数据库并保留最近批次

## Features

- 默认每 5 分钟刷新一次市场数据
- 每天 0 点同步一次 Binance 最新交易对列表
- 支持持仓量、持仓价值、24h 涨跌、分周期持仓变化、主力净流入等指标
- 提供跌幅榜页面
- 提供币种配置页面，可切换交易对跟踪状态
- 提供 Flask 页面和 JSON API

## Quick Start

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

或：

```bash
pip install -e .
```

### 2. 初始化数据库

先创建数据库：

```sql
CREATE DATABASE coinx DEFAULT CHARACTER SET utf8mb4;
```

再执行建表脚本：

```bash
mysql -u root -p coinx < sql/schema.sql
```

### 3. 配置环境

项目配置来源：

- `application.yml`
- `application-{env}.yml`
- 环境变量覆盖

优先级从高到低：

- 环境变量
- `application-{env}.yml`
- `application.yml`

常用配置项：

- `DB_HOST`
- `DB_PORT`
- `DB_USER`
- `DB_PASSWORD`
- `DB_NAME`

本地开发可直接修改 [application-dev.yml](/z:/Resource/Code/project/coinx/application-dev.yml)。

### 环境变量说明

| 变量名 | 说明 | 默认值 |
| --- | --- | --- |
| `COINX_ENV` | 选择环境配置文件，例如 `dev` 会加载 `application-dev.yml` | `application.yml` 中的 `profiles.active`，默认是 `dev` |
| `BINANCE_BASE_URL` | Binance API 基础地址，可替换为代理地址或自建转发地址 | `https://proxy.yffjglcms.com/fapi.binance.com` |
| `UPDATE_INTERVAL` | 定时刷新市场数据的间隔，单位为秒 | `300` |
| `TIME_INTERVALS` | 需要计算的时间周期列表，当前更建议放在 YAML 中配置，不建议直接用环境变量字符串覆盖 | `5m,15m,30m,1h,4h,12h,24h,48h,72h,168h` |
| `USE_PROXY` | 是否启用 HTTP/HTTPS 代理，支持 `true/false/1/0/yes/no` | `false` |
| `PROXY_HOST` | 代理主机地址 | `127.0.0.1` |
| `PROXY_PORT` | 代理端口 | `7897` |
| `DB_HOST` | MySQL 主机地址 | `localhost` |
| `DB_PORT` | MySQL 端口 | `3306` |
| `DB_USER` | MySQL 用户名 | `root` |
| `DB_PASSWORD` | MySQL 密码 | 空 |
| `DB_NAME` | 数据库名 | `coinx` |
| `DB_CHARSET` | MySQL 字符集 | `utf8mb4` |
| `WEB_HOST` | Web 服务监听地址 | `0.0.0.0` |
| `WEB_PORT` | Web 服务端口 | `5000` |
| `WEB_DEBUG` | 是否启用 Flask Debug，支持 `true/false/1/0/yes/no` | `false` |

示例：

```bash
set COINX_ENV=dev
set DB_HOST=127.0.0.1
set DB_PORT=3306
set DB_USER=root
set DB_PASSWORD=your_password
set DB_NAME=coinx
set USE_PROXY=false
python scripts/start_app.py run
```

### 4. 启动项目

Windows：

```bat
start.bat
```

或前台启动：

```bash
python scripts/start_app.py run
```

后台启动：

```bash
python scripts/start_app.py start
```

停止：

```bash
python scripts/start_app.py stop
```

默认访问地址：

```text
http://127.0.0.1:5000
```

## Pages

- `/`
  - 首页监控面板
- `/coins-config`
  - 币种配置管理
- `/coin-detail`
  - 单币详情
- `/drop-list`
  - 跌幅榜

## API

- `GET /api/coins`
  - 获取已跟踪币种的展示数据
- `GET /api/update`
  - 手动触发一次刷新
- `GET /api/coin-detail/<symbol>`
  - 获取单币详情
- `GET /api/drop-list`
  - 获取跌幅榜
- `GET /api/coins-config`
  - 获取币种配置
- `POST /api/coins-config/track`
  - 切换币种跟踪状态
- `POST /api/coins-config/update-from-binance`
  - 从 Binance 同步最新交易对列表

## Project Structure

```text
coinx/
├─ src/coinx/
│  ├─ main.py
│  ├─ scheduler.py
│  ├─ config.py
│  ├─ database.py
│  ├─ models.py
│  ├─ utils.py
│  ├─ data_processor.py
│  ├─ coin_manager.py
│  ├─ collector/
│  └─ web/
├─ data/
├─ logs/
├─ sql/schema.sql
├─ scripts/start_app.py
├─ start.bat
├─ application.yml
└─ application-dev.yml
```

## Data Files

`data/` 目录下常见文件：

- `coins_config.json`
  - 历史配置文件，当前主要用于兼容和迁移
- `drop_list_cache.json`
  - 跌幅榜缓存
- `app.pid`
  - 后台运行时的进程号文件

## Status

当前版本已经能完成基础监控闭环，但仍偏工程早期版本，现状包括：

- 首页、配置页、跌幅榜已可用
- 详情页部分展示仍带有占位性质
- 前端模板中仍有较多内联 CSS 和 JS
- 项目已有从 `coins_config.json` 向数据库配置迁移的思路

## Next

- 拆分前端模板中的内联 CSS / JS
- 补齐详情页的真实分周期数据
- 增加测试
- 增加告警能力
- 统一缓存策略
- 校准 `compose.yml` 与当前真实入口

## Binance 历史序列

当前项目已经支持以下 Binance 历史序列的结构化采集、落库、接口触发与页面管理：

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

### 近期变更

- `feat(binance): 新增历史序列采集与管理页`
- `feat(binance): 新增历史序列修补与双模式入口`
- `feat(binance): 支持历史序列覆盖回补`
- `feat(binance): 首页切换到历史序列快照链路`
- `docs(binance): 补充历史序列使用说明`

### 页面

- `/binance-series`
  - Binance 历史序列管理页

### 接口

- `POST /api/binance-series/collect`
  - 采集单个历史序列并写入 MySQL
- `POST /api/binance-series/batch-collect`
  - 按币种、周期、序列类型批量采集历史序列
- `POST /api/binance-series/repair-tracked`
  - 对 tracked 币种执行 `5m` 历史序列覆盖回补与尾部追平

## 首页历史序列说明

- 首页数据现在直接来自 `binance_open_interest_hist` 与 `binance_klines`
- `GET /api/coins` 会一次性构建首页快照，并同时返回：
  - `data`
  - `cache_update_time`
- `GET /api/update`、`POST /api/binance-series/repair-tracked` 与首页定时刷新统一复用 coverage-aware repair
- 首页长周期区间统一基于 `5m` 历史序列推导：
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

### 首页查询优化

为降低首页加载延迟，首页查询做了以下约束：

- 只查询首页真正需要的列，不加载 `raw_json` 等大字段
- 小规模 tracked 币种优先走按币种索引倒序 `limit` 查询
- `GET /api/coins` 不重复构建两次首页快照
- futures 历史接口修补按固定时间窗分页，避免长周期覆盖看似修补成功但实际没有前移

这意味着首页性能瓶颈主要在数据库查询，而不是前端渲染或 Python 计算。

### 脚本示例

```bash
python scripts/fetch_binance_series.py klines --symbol BTCUSDT --period 5m --limit 20
python scripts/fetch_binance_series.py open_interest_hist --symbol BTCUSDT --period 5m --limit 20
python scripts/fetch_binance_series.py top_long_short_position_ratio --symbol BTCUSDT --period 5m --limit 20
```

### 调度配置

`application.yml` 中的相关配置如下：

```yaml
app:
  binance_series:
    limit: 30
    types:
      - top_long_short_position_ratio
      - top_long_short_account_ratio
      - open_interest_hist
      - klines
      - global_long_short_account_ratio
    periods:
      - 5m
    repair:
      enabled: false
      interval: 900
      period: 5m
      bootstrap_days: 7
      coverage_hours: 168
      klines_page_limit: 1000
      futures_page_limit: 500
      sleep_ms: 500
```

手动采集继续按页面或 API 按需触发，调度器定时执行的是 `repair` 链路。

### 相关测试

相关测试包括：

- `tests/test_binance_market_parsers.py`
- `tests/test_binance_series_repository.py`
- `tests/test_binance_series_integration.py`
- `tests/test_binance_series_service.py`
- `tests/test_binance_series_api.py`
- `tests/test_binance_series_page.py`
- `tests/test_binance_series_repair_window.py`
- `tests/test_binance_series_repair_service.py`
