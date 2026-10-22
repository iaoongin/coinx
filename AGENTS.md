# CoinX Agent 开发规则

本文件只维护 Agent 在 CoinX 项目中的跨任务通用规则，不维护具体业务实现细节。当前后续开发以 MySQL 为唯一兼容目标；ClickHouse 和 StarRocks 已弃用，不再为其新增兼容。具体接口、字段、调度、页面、数据库迁移和业务方案，以当前文档和代码为准，避免在 `AGENTS.md` 重复维护。

## 目录

- [工作方式](#工作方式)
- [上下文与文档更新](#上下文与文档更新)
- [文档定位与判定](#文档定位与判定)
- [项目级硬约束](#项目级硬约束)
- [文档目录](#文档目录)
- [本地服务](#本地服务)

## 工作方式

- 涉及代码、页面、数据库或文档修改时，先输出实施方案、影响范围和验证方式，等待用户确认后再修改；只有用户明确要求“直接改”“执行”“提交”等操作时，才进入实施流程。
- 先阅读现有模块和调用链，优先沿用项目已有模式；保留用户已有改动，不做无关重构。
- Web UI 修改必须通过浏览器实际截图验收后再交付；验收需覆盖受影响的视口，并在交付时说明截图视口和结果，不能只依据 DOM、可访问性树或文字描述判定通过。
- 配置、接口地址、任务显示名和可变策略应配置化，避免在业务代码中硬编码。
- 外部接口或数据库结构变化时，先识别受影响的文档、SQL DDL、导入脚本和测试。
- 提交信息遵循 Conventional Commits 格式，类型和作用域使用英文，描述使用简洁中文；提交标题和正文必须以实际 `git diff` 为依据，准确反映真实改动范围，不得混淆交易所、模块或功能；多个独立改动应拆分提交，或在正文中分别列明；提交前先向用户预览，未经明确要求不自动提交。

## 文档定位与判定

- `AGENTS.md`：Agent 的工作规则、项目硬约束和文档索引，不记录具体接口字段或业务流程。
- `README.md`：面向开发者和部署人员的项目入口，记录项目用途、安装、配置、启动方式和功能概览。
- `docs/`：当前业务和技术实现文档，记录交易所接口能力、调度任务、数据库设计和页面方案。
- `docs/superpowers/`：历史计划和设计规格，仅用于追溯，不自动代表当前实现。

处理冲突时遵循：用户当前明确要求优先；当前代码和测试用于确认实际运行行为；标记为当前的文档用于确认业务约定；历史文档只作背景参考。

## 上下文与文档更新

- 每次处理 CoinX 任务前，读取项目根目录 [`README.md`](README.md) 和 [`docs/`](docs/) 下的全部项目文档。
- 阅读后区分当前规范/实施文档与历史方案/计划；README 只提供项目入口和概览，不替代具体设计文档。
- 发现实现变化需要同步文档时，先在回复中同步拟更新的文档、内容和原因，等待用户确认后再修改；不要求用户另行声明“请同步文档”，未经确认仍不主动修改文档。

## 项目级硬约束

- 接口返回列表时默认保存全部返回数据，不自行限制币种数量或使用其他接口二次过滤，除非需求明确要求。
- 采集失败不写入零值或伪造快照，保留缺口并记录任务错误。
- MySQL 的模型、DDL、写入字段白名单和保留策略必须保持同步；ClickHouse 和 StarRocks 仅作为弃用的历史兼容路径，不为其新增功能、表结构、查询或测试兼容。
- 设计文档必须区分“已实施范围”和“后续规划”，不能把未实现内容描述成已上线。
- 所有业务页面的外层容器统一复用 [`dark-theme.css`](src/coinx/web/static/dark-theme.css) 定义的 `--page-max-width`，当前值为 `1536px`；模板中使用 `var(--page-max-width)`，不得为页面外层新增 `1500px`、`1440px` 等独立最大宽度。登录框、弹窗、表单、图表等页面内部组件可以按内容需要设置局部 `max-width`，不属于本约束范围。
- 修改完成后至少运行相关模块测试和编译检查，并在交付时说明结果及环境限制。

## 文档目录

目录中带删除线的文档或条目表示已弃用，仅用于历史追溯、现有代码排查和必要的下线工作，不作为后续新功能或兼容实现依据。

### 项目入口

| 文档 | 用途 |
|---|---|
| [`README.md`](README.md) | 项目安装、配置、启动、页面和总体状态 |

### 当前业务与实现文档

| 文档 | 用途 |
|---|---|
| [`docs/exchange_series_feasibility.md`](docs/exchange_series_feasibility.md) | 交易所序列能力、接口范围和历史限制 |
| [`docs/scheduler_jobs_overview.md`](docs/scheduler_jobs_overview.md) | 调度任务、顺序、周期和任务运行记录 |
| [`docs/fetch_coins_plan.md`](docs/fetch_coins_plan.md) | 币种拉取与市场币种配置 |
| [`docs/binance_smart_money_api.md`](docs/binance_smart_money_api.md) | 币安聪明钱接口和字段 |
| [`docs/binance_futures_smart_money_persistence_plan.md`](docs/binance_futures_smart_money_persistence_plan.md) | 合约聪明钱持久化方案和实施范围 |
| [`docs/合约市场结构评分模型.md`](docs/合约市场结构评分模型.md) | 合约市场结构评分模型 |
| [`docs/交易机会评分模型.md`](docs/交易机会评分模型.md) | 交易机会评分与数据完整性 |
| [`docs/notification_alert_design.md`](docs/notification_alert_design.md) | 告警、评估和通知设计 |
| [`docs/contract_detail_improvement_plan.md`](docs/contract_detail_improvement_plan.md) | 合约详情页改进计划 |
| [`docs/vue3_migration_plan.md`](docs/vue3_migration_plan.md) | Vue 3 页面迁移计划 |

### ~~已弃用后端历史文档~~

ClickHouse 和 StarRocks 文档仅用于迁移历史、现有代码排查和必要的下线工作，不作为后续新功能的兼容目标。

| 文档 | 用途 |
|---|---|
| ~~[`docs/clickhouse-market-write-migration.md`](docs/clickhouse-market-write-migration.md)~~ | 行情写入 ClickHouse 迁移、回滚和幂等 |
| ~~[`docs/clickhouse-read-migration-development-plan.md`](docs/clickhouse-read-migration-development-plan.md)~~ | MySQL/ClickHouse 读链路迁移开发计划 |
| ~~[`docs/clickhouse-api-asof-verification.md`](docs/clickhouse-api-asof-verification.md)~~ | 固定时间回放验证 |
| ~~[`docs/clickhouse-api-readonly-report-2026-08-08.md`](docs/clickhouse-api-readonly-report-2026-08-08.md)~~ | ClickHouse 只读验证报告 |

### 历史方案与规格

以下文档用于追溯设计和实施过程，不自动视为当前实现：

| 目录 | 内容 |
|---|---|
| [`docs/superpowers/plans/`](docs/superpowers/plans/) | 历史实施计划 |
| [`docs/superpowers/specs/`](docs/superpowers/specs/) | 历史设计规格 |

## 本地服务

- 未经用户明确要求，不自动启动、重启或停止 CoinX 服务；发现端口状态异常时只报告检查结果。
- 用户明确要求服务操作时，优先复用原端口，不直接切换到新端口。
