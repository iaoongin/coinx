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

- `.env`
- `.env.<profile>`，例如 `.env.local` 或 `.env.prod`
- 进程环境变量覆盖

优先级从高到低：

- 进程环境变量
- `.env.<profile>`
- `.env`

常用配置项：

- `DB_HOST`
- `DB_PORT`
- `DB_USER`
- `DB_PASSWORD`
- `DB_NAME`

本地开发建议修改 `.env.local`；生产环境建议修改 `.env.prod`，并通过密钥管理系统注入密码。

### 环境变量说明

完整变量模板见 [`.env.example`](.env.example)。模板已经按用途分组，日常只需要关注对应 profile 文件：

- 运行和安全：`COINX_ENV`、`WEB_*`、`INSTANCE_NAME`
- 调度和首页：`SCHEDULER_*`、`COLLECTION_SCHEDULER_ONLY`、`UPDATE_INTERVAL`、`TIME_INTERVALS`、`HOMEPAGE_*`
- 后端和数据库：`DB_*`、`MARKET_BACKEND`、`READ_BACKEND`、`MARKET_WRITE_BACKEND`
- ClickHouse：`CLICKHOUSE_*`
- 外部服务：交易所 `*_BASE_URL`、`PROXY_*`、`SR_*`
- 采集和通知：`FETCH_COINS_*`、`REPAIR_*`、`FUNDING_RATE_*`、`NOTIFICATION_*`、`RSS_*`

建议把稳定的公共配置放在 `.env`，把本机和生产差异分别放在 `.env.local`、`.env.prod`。密码、JWT 密钥和通知加密密钥不要提交到 Git。

示例：

```bash
set COINX_ENV=local
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

如果代理地址使用 `socks5://` 或 `socks5h://`，部署环境还需要安装 `PySocks`；项目的 [requirements.txt](requirements.txt) 已包含该依赖。

Profile 配置支持本地和生产环境。优先级为：进程环境变量 > `.env.<profile>` > `.env` > 代码默认值。默认 profile 是 `local`；`.env.local` 和 `.env.prod` 已被 Git 忽略。

复制模板，并按环境填写数据库、密钥和 ClickHouse 配置：

```bash
cp .env.example .env
cp .env.local.example .env.local
cp .env.prod.example .env.prod
```

启动时选择 profile：

```bash
# Windows
start.bat --env local run
start.bat --env prod start

# Linux/macOS
./start.sh --env local run
./start.sh --env prod start

# Or use an environment variable
COINX_ENV=prod python scripts/start_app.py run
```

Docker Compose 需要同时把 profile 文件用于变量插值和容器环境：

```bash
COINX_ENV_FILE=.env.prod docker compose --env-file .env.prod up -d
```

`--env prod` 选择应用 profile；`COINX_ENV_FILE=.env.prod` 选择 Compose 的 `env_file`。生产密码建议通过进程环境变量或密钥管理系统注入。

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
- `/notification-management`
  - 通知渠道与告警规则管理；规则详情按需查看状态、评估记录和投递记录

### RSS monitoring

Open `/rss` to manually add and manage RSS feeds, read fetched articles, and independently toggle fetching and notifications.

Automatic RSS polling uses `RSS_ENABLED` and `RSS_POLL_INTERVAL`, and also requires `SCHEDULER_ENABLED=true`. Notifications reuse the Apprise channels configured in `/notification-management`; for Bark, add a `barks://device-key` channel and set `NOTIFICATIONS_ENABLED=true`. Restart the service after changing environment variables.

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
├─ .env.example
├─ .env.local.example
└─ .env.prod.example
```

## Data Files

### ClickHouse 行情写入迁移

生产统一使用 MARKET_BACKEND=clickhouse 控制行情读写；DB_TYPE 仍保持
mysql，用于控制面、任务记录和告警状态。READ_BACKEND 与
MARKET_WRITE_BACKEND 仅作为双实例灰度、故障回滚时的单方向兼容覆盖。

六张行情表的 ClickHouse 读写切换、`_v2` 原子换表、断点导入和紧急回滚步骤见 [`docs/clickhouse-market-write-migration.md`](docs/clickhouse-market-write-migration.md)。`DB_TYPE` 保持 `mysql`，控制面和告警状态不会迁移到 ClickHouse。

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

