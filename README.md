# CoinX

CoinX 是一个面向 Binance U 本位合约的币种监控项目，提供定时采集、MySQL 落库、Web 页面展示和跟踪币种配置管理。

当前仓库已经可以跑通一套基础闭环：

- 定时抓取合约市场数据
- 展示首页监控面板、行情榜、币种详情和跌幅榜
- 管理哪些交易对需要跟踪
- 将市场快照写入数据库并保留最近批次

## Features

- 默认每 5 分钟刷新一次市场数据
- 行情榜会按定时任务自动刷新数据库快照，页面支持手动强制刷新一次
- 每天 0 点同步一次 Binance 最新交易对列表
- 支持持仓量、持仓价值、24h 涨跌、分周期持仓变化、主力净流入等指标
- 提供行情榜页面
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

如果使用外部或本机 MySQL，先创建数据库：

```sql
CREATE DATABASE coinx DEFAULT CHARACTER SET utf8mb4;
```

再执行建表脚本：

```bash
mysql -u root -p coinx < sql/schema.sql
```

如果使用 Docker Compose 内置 MySQL，可以跳过手动创建数据库和导入表结构；容器会根据 `.env` 中的 `DB_NAME`、`DB_USER`、`DB_PASSWORD` 初始化数据库和应用用户，并在空数据卷首次启动时自动执行 `sql/schema.sql`。

### Docker Compose 可选 MySQL

默认只启动应用服务，适合连接外部 MySQL：

```bash
docker compose up -d
```

需要同时启动内置 MySQL 时，启用 `mysql` profile：

```bash
docker compose --profile mysql up -d
```

`.env.example` 默认面向内置 MySQL，核心配置如下：

```env
DB_HOST=mysql
DB_PORT=3306
DB_USER=coinx
DB_PASSWORD=coinx_password
DB_NAME=coinx
DB_CHARSET=utf8mb4
```

MySQL 容器会引用 `DB_NAME`、`DB_USER`、`DB_PASSWORD` 初始化应用数据库；只需要单独配置 root 密码：

```env
MYSQL_ROOT_PASSWORD=coinx_root_password
```

连接外部 MySQL 时，把 `DB_HOST`、`DB_USER`、`DB_PASSWORD`、`DB_NAME` 改为外部数据库配置即可。

`mysql` profile 启动过之后，后续只运行 `docker compose up -d` 不一定会自动删除之前创建的 MySQL 容器。切回外部 MySQL 时，先调整 `.env` 的 `DB_*` 配置，再显式停止可选 MySQL：

```bash
docker compose --profile mysql down
```

或只停止 MySQL 服务：

```bash
docker compose stop mysql
```

MySQL 数据保存在 Docker 命名卷 `mysql_data` 中。普通停止、重建容器不会删除数据；只有执行带 volume 删除的命令，例如 `docker compose down -v`，才会清理数据库数据。

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
| `OKX_BASE_URL` | OKX API 基础地址，可替换为代理地址或自建转发地址 | `https://proxy.yffjglcms.com/www.okx.com` |
| `OKX_RUBIK_MIN_INTERVAL_MS` | OKX rubik 统计接口最小请求间隔，`taker-volume`/`open-interest-volume` 默认按高敏接口处理 | `500` |
| `BYBIT_BASE_URL` | Bybit API 基础地址，可替换为代理地址或自建转发地址 | `https://proxy.yffjglcms.com/api.bybit.com` |
| `GATE_BASE_URL` | Gate API 基础地址，首期 futures 链路默认走 `fx-api` 的代理口径 | `https://proxy.yffjglcms.com/fx-api.gateio.ws` |
| `GATE_SETTLE` | Gate 合约结算币种，首期默认只接 `usdt` 永续 | `usdt` |
| `GATE_MIN_INTERVAL_MS` | Gate 公共接口最小请求间隔，避免连续 futures 请求触发代理/WAF 拦截 | `60` |
| `GATE_403_RETRY_FALLBACK_SECONDS` | Gate 遇到 `403` 时的冷却秒数 | `8` |
| `UPDATE_INTERVAL` | 定时刷新间隔，单位为秒，当前会同时用于市场数据与行情榜快照刷新 | `300` |
| `TIME_INTERVALS` | 需要计算的时间周期列表，当前更建议放在 YAML 中配置，不建议直接用环境变量字符串覆盖 | `5m,15m,30m,1h,4h,12h,24h,48h,72h,168h` |
| `USE_PROXY` | 是否启用 HTTP/HTTPS 代理，支持 `true/false/1/0/yes/no` | `false` |
| `PROXY_HOST` | 代理主机地址 | `127.0.0.1` |
| `PROXY_PORT` | 代理端口 | `7897` |
| `USE_PROXY_POOL` | 是否启用代理池配置能力 | `false` |
| `PROXY_POOL_URLS` | 通用代理池列表，格式 `id=url;id2=url2` | 空 |
| `PROXY_POOL_STRATEGY` | 代理选择策略，当前支持 `round_robin` / `least_recently_used` | `round_robin` |
| `PROXY_POOL_FAIL_COOLDOWN_SECONDS` | 代理请求失败后的冷却秒数 | `30` |
| `DB_HOST` | MySQL 主机地址；Compose 内置 MySQL 使用 `mysql` | 代码默认 `localhost`，`.env.example` 为 `mysql` |
| `DB_PORT` | MySQL 端口 | `3306` |
| `DB_USER` | MySQL 用户名 | 代码默认 `root`，`.env.example` 为 `coinx` |
| `DB_PASSWORD` | MySQL 密码 | 代码默认空，`.env.example` 为 `coinx_password` |
| `DB_NAME` | 数据库名 | `coinx` |
| `DB_CHARSET` | MySQL 字符集 | `utf8mb4` |
| `MYSQL_ROOT_PASSWORD` | Docker Compose 内置 MySQL 的 root 密码，仅容器初始化使用 | `coinx_root_password` |
| `WEB_HOST` | Web 服务监听地址 | `0.0.0.0` |
| `WEB_PORT` | Web 服务端口 | `5000` |
| `WEB_DEBUG` | 是否启用 Flask Debug，支持 `true/false/1/0/yes/no` | `false` |
| `WEB_USERNAME` | 网页登录用户名 | `admin` |
| `WEB_PASSWORD` | 网页登录密码，未配置时启动时会自动生成并打印到日志 | 随机生成 |
| `WEB_SESSION_SECRET` | 会话签名密钥，未配置时自动生成 | 随机生成 |

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

OKX 代理池示例：

```bash
USE_PROXY_POOL=true
PROXY_POOL_URLS=HK=socks5h://user:pass@proxy.example.com:2261;JP=socks5h://user:pass@proxy.example.com:2261
PROXY_POOL_STRATEGY=round_robin
```

如果代理地址使用 `socks5://` 或 `socks5h://`，部署环境还需要安装 `PySocks`；项目的 [requirements.txt](/Users/xhx-mbp/Code/project/coinx/requirements.txt) 已包含该依赖。

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

首次访问网页会先进入 `/login` 登录页，需要输入 `WEB_USERNAME` 和 `WEB_PASSWORD`。如果没有配置 `WEB_PASSWORD`，系统会在启动日志中打印自动生成的临时密码。

## Pages

- `/`
  - 多周期矩阵，默认首页
- `/legacy-home`
  - 旧首页，历史入口
- `/new-home`
  - 多周期矩阵兼容入口，会重定向到 `/`
- `/market-rank`
  - 行情榜，支持自动刷新和手动刷新快照
- `/coins-config`
  - 币种配置管理
- `/coin-detail`
  - 单币详情

## API

- `GET /api/coins`
  - 获取已跟踪币种的展示数据
- `GET /api/update`
  - 手动触发一次刷新
- `GET /api/market-rank`
  - 获取行情榜排行数据，按最新数据库快照排序返回
- `POST /api/market-rank/refresh`
  - 手动触发行情榜快照刷新，再供 `/api/market-rank` 读取最新结果
- `GET /api/coin-detail/<symbol>`
  - 获取单币详情
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

