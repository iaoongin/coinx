# CoinX 可配置告警与消息推送概要设计

## 1. 目标与范围

CoinX 已具备行情、资金费率、市场结构评分、历史序列修补和任务运行状态。本设计为这些既有信号增加可配置的事件告警和外部消息推送，让用户无需持续打开页面查看。

首期目标：

- 为所有币种配置资金费率异常告警，并为成交额榜单币配置短周期价格放量异动告警。
- 为系统任务配置失败与恢复告警。
- 以 Apprise 统一接入 Telegram Bot 与通用 Webhook 两种渠道。
- 防止同一事件在阈值附近或周期任务重复执行时重复推送。
- 保存规则、事件状态和发送记录，支持审计、排障与恢复通知。

首期不包含：

- 多租户、用户级权限与多接收人管理。
- 自定义表达式、脚本或可视化规则编排。
- 消息队列、异步 worker、复杂重试编排。
- 自动交易、下单或投资建议。

系统保持当前 Flask + APScheduler + SQLAlchemy 单体结构；告警判定在现有采集或修补任务完成后同步执行。单次发送必须设置短超时，失败不得影响行情数据的落库或修补结果。

## 2. 业务事件

### 2.1 首期事件类型

| 事件类型 | 适用对象 | 数据来源 | 默认条件 | 事件状态 |
| --- | --- | --- | --- | --- |
| `market.funding_rate.threshold` | 所有币种 | `market_funding_rate` | 资金费率绝对值穿越阈值 | `triggered` / `recovered` |
| `market.price_volume.threshold` | 成交额榜单前 N 币种 | 5 分钟 K 线 | 短周期涨跌幅和成交额放大倍数同时达到阈值 | `triggered` / `recovered` |
| `system.job.failure` | 系统任务 | `JOB_METADATA` | 任务失败，或连续失败次数达到阈值 | `triggered` / `recovered` |

命名统一采用 `<domain>.<metric>.<condition>`：`market` 是市场数据事件，`system` 是运行时任务事件；`condition` 使用 `threshold` 表示指标越过规则阈值，使用 `failure` 表示运行失败。`market.funding_rate.threshold` 固定作用于所有币种，`market.price_volume.threshold` 固定作用于成交额榜单前 N 币种；系统规则只允许选择已注册任务。

### 2.2 触发原则

- 仅处理已启用、且对象仍在范围内的规则。
- 同一规则、对象和方向形成唯一事件键，例如 `rule:12|symbol:BTCUSDT|direction:positive`。
- 阈值类事件使用“穿越”而不是“持续满足”触发；已触发期间不重复发出，恢复后才能重新触发。
- 价格放量事件使用已收盘的 5 分钟数据，避免未收盘 K 线造成瞬时误报。
- 每条规则有冷却时间。冷却期内仅更新事件状态和观测值，不发送重复消息。
- 数据缺失时跳过对应市场事件判定，并记录结构化日志。
- 任一通知发送失败只记为发送失败，不回滚采集和数据库事务。

## 3. 总体架构

```text
采集/修补任务或手工刷新
        |
        v
既有数据仓库 + JOB_METADATA
        |
        v
AlertEvaluator（读取启用规则并生成候选事件）
        |
        +--> AlertState（去重、连续命中、恢复、冷却）
        |
        v
NotificationDispatcher
        |
        +--> Telegram Bot
        +--> Webhook
        |
        v
NotificationDelivery（发送记录）
```

建议新增模块：

| 模块 | 职责 |
| --- | --- |
| `coinx/notifications/models.py` | SQLAlchemy 模型或复用 `models.py` 中的新模型定义 |
| `coinx/notifications/rules.py` | 规则参数校验、事件键生成、规则查询 |
| `coinx/notifications/evaluator.py` | 对已有数据快照判定规则，不调用交易所 API |
| `coinx/notifications/dispatcher.py` | 通过 Apprise 路由渠道、渲染内容、控制超时并记录发送结果 |
| `coinx/notifications/channels.py` | Apprise URL 校验、加密存储、解密发送与脱敏展示 |
| `coinx/web/routes/api_notifications.py` | 规则、渠道、发送记录的 API |

告警评估放在以下任务成功完成之后：

- `collect_funding_rates_job`：资金费率规则。
- `repair_market_rolling_job`：滚动修补完成后，对成交额榜单前 N 币种评估价格放量规则。
- 所有已注册任务：任务失败/恢复规则。

榜单币范围复用 `get_market_ticker_symbols(rank_type='quote_volume', limit=FETCH_COINS_TOP_VOLUME_COUNT)`，与当前滚动修补的榜单币范围一致，不新增行情采集任务或数据表。首次实现不在页面请求中触发评估，避免刷新页面导致重复推送；手工刷新 API 仅在实际数据刷新完成后调用同一评估入口。

## 4. 数据模型

### 4.1 通知渠道 `notification_channels`

| 字段 | 说明 |
| --- | --- |
| `id` | 主键 |
| `name` | 渠道显示名称，唯一 |
| `channel_type` | `apprise` |
| `enabled` | 是否启用 |
| `config_encrypted` | 使用服务端密钥加密后的 Apprise URL，API 永不返回该字段 |
| `key_version` | 加密密钥版本，支持后续轮换 |
| `created_at` / `updated_at` | 审计时间 |

页面创建或编辑渠道时提交 Apprise URL，服务端校验后使用 Fernet 加密写入 `config_encrypted`。加密主密钥只存在 `.env`，不得写入数据库、日志、发送记录或 API 响应。加密主密钥丢失会导致既有渠道无法解密，必须在部署备份中妥善保存。

### 4.2 告警规则 `alert_rules`

| 字段 | 说明 |
| --- | --- |
| `id` | 主键 |
| `name` | 规则名称 |
| `event_type` | 2.1 中的事件类型 |
| `scope_type` | `all_market`、`market_rank_top` 或 `system_jobs` |
| `scope_json` | `all_market` 固定为空对象；`market_rank_top` 保存榜单维度与数量；`system_jobs` 保存任务 ID 数组 |
| `params_json` | 与事件类型对应的阈值参数 |
| `cooldown_seconds` | 相同事件键的最短发送间隔 |
| `recovery_enabled` | 是否发送恢复消息 |
| `enabled` | 是否启用 |
| `created_at` / `updated_at` | 审计时间 |

参数白名单：

| `event_type` | `params_json` |
| --- | --- |
| `market.funding_rate.threshold` | `threshold`、`direction`（`positive` / `negative` / `absolute`） |
| `market.price_volume.threshold` | `period`（首期固定 `5m`）、`price_change_threshold`、`volume_ratio_threshold`、`direction` |
| `system.job.failure` | `job_ids`、`consecutive_failures`（默认 1） |

后端必须按 `event_type` 校验参数类型、范围与匹配的 `scope_type`。规则 API 不得传入自由表达式、SQL 片段或渠道密钥；渠道创建/编辑 API 仅允许提交 Apprise URL，且该值不会在后续读取接口中返回。

### 4.3 规则渠道关系 `alert_rule_channels`

规则与渠道是多对多关系，不在 `alert_rules` 中保存 JSON ID 列表。

| 字段 | 说明 |
| --- | --- |
| `rule_id` | 关联告警规则 |
| `channel_id` | 关联通知渠道 |
| `created_at` | 关联创建时间 |

唯一索引为 `(rule_id, channel_id)`。一个规则可选择多个已启用渠道，例如市场规则发 Telegram，系统任务失败同时发 Telegram 与 Webhook。

### 4.4 告警状态 `alert_states`

该表是去重与恢复语义的核心。唯一索引为 `(rule_id, subject_key, dimension_key)`。

| 字段 | 说明 |
| --- | --- |
| `rule_id` | 关联规则 |
| `subject_key` | 例如 `BTCUSDT`、`repair_market_rolling_job` |
| `dimension_key` | 例如 `positive`、`up`、`down` |
| `state` | `normal`、`pending`、`triggered` |
| `consecutive_matches` | 连续命中次数 |
| `last_value_json` | 最近一次判定指标，便于审计 |
| `last_triggered_at` | 最近一次进入异常状态的时间 |
| `last_notified_at` | 最近一次实际发送时间 |
| `last_recovered_at` | 最近一次恢复时间 |

### 4.5 发送记录 `notification_deliveries`

| 字段 | 说明 |
| --- | --- |
| `id` | 主键 |
| `rule_id` / `channel_id` | 关联规则与渠道 |
| `event_key` | 本次事件唯一键 |
| `event_status` | `triggered` 或 `recovered` |
| `payload_json` | 已渲染前的业务载荷，不存密钥 |
| `delivery_status` | `success`、`failed`、`skipped` |
| `response_code` / `error_message` | 投递结果，错误信息需截断与脱敏 |
| `sent_at` | 发送时间 |

首期不创建独立消息队列。Apprise 发送失败仅记录一次；连续失败由系统规则告警或运维日志发现。第二期再基于此表引入有限次数重试。

## 5. 规则判定与生命周期

### 5.1 通用状态机

```text
normal --条件满足且确认次数=1--> triggered
  ^                                  |
  |-----------条件恢复--------------+
                                      |
                         recovery_enabled 时发送恢复消息

normal --条件首次满足且确认次数>1--> pending --满足确认次数--> triggered
  ^                                      |                         |
  |---------------条件不满足------------+----条件恢复-------------+
```

首期全部事件的确认次数为 1。`triggered` 状态下只更新观测值；只有恢复后再次满足条件才会形成下一轮触发。

### 5.2 资金费率

- 使用最新已落库的 `funding_rate`；`predicted_rate` 不参与首期告警判定。
- `absolute` 方向的触发条件为 `abs(rate) >= threshold`；正负方向分别独立维护状态。
- 恢复条件为 `abs(rate) < threshold`。首期不加复杂滞回；若实测抖动频繁，第二期增加恢复阈值比触发阈值低 10% 的配置。

### 5.3 价格与成交额异动

- 使用落库的已收盘 5 分钟 K 线，不使用未收盘 K 线。
- 计算 `price_change = (close - open) / open`，成交额放大倍数复用评分模块的 24 小时均值口径。
- 上涨与下跌使用独立状态；两个条件必须同时满足。
- 规则固定覆盖成交额榜单前 N 币种，范围来自最新 `market_tickers` 快照；榜单变化后失去范围资格的币种不再产生新触发。
- 复用 `repair_market_rolling_job` 已落库的 5 分钟序列，不新增采集任务或数据表。榜单币缺少完整序列时跳过判定，依赖既有修补任务补齐。

### 5.4 任务失败

- 任务规则读取 `get_all_job_runtime_metadata()`，以 `last_status` 和连续失败计数判定。
- 恢复定义为相同任务下一次成功执行。

## 6. 渠道与消息格式

### 6.1 渠道约束

| 渠道 | Apprise URL 配置 | 投递方式 | 超时 | 结果判定 |
| --- | --- | --- | --- | --- |
| Telegram | `tgram://BOT_TOKEN/CHAT_ID`，加密存储在渠道表 | Apprise `notify()` | 5 秒 | Apprise 返回成功 |
| Webhook | Apprise 支持的 JSON Webhook URL，加密存储在渠道表 | Apprise `notify()` | 5 秒 | Apprise 返回成功 |

Webhook 负载固定为：

```json
{
  "event_key": "rule:12|symbol:BTCUSDT|positive",
  "event_type": "market.funding_rate.threshold",
  "status": "triggered",
  "occurred_at": 1780000000000,
  "title": "BTCUSDT 资金费率异常",
  "summary": "资金费率 0.124%，超过 0.100% 阈值",
  "data": {}
}
```

### 6.2 文案原则

- 标题包含对象和事件，不使用仅有“告警”的模糊标题。
- 正文包含观测值、阈值、数据时间、规则名和 CoinX 详情链接。
- 触发与恢复用不同标题，例如“资金费率异常”和“资金费率已恢复”。
- 市场类消息必须带“仅为市场数据提示，不构成投资建议”。
- 禁止在日志、发送记录或前端 API 返回完整 token、URL 查询参数和渠道响应正文。

## 7. 配置与 API

### 7.1 环境变量

```dotenv
# 通知总开关
NOTIFICATIONS_ENABLED=false
# 外部请求超时
NOTIFICATION_TIMEOUT_SECONDS=5

# Fernet URL-safe base64 key，只用于加密通知渠道中的 Apprise URL。
# 生成方式：Fernet.generate_key()；生产环境应放入部署密钥管理系统。
NOTIFICATION_ENCRYPTION_KEY=
```

`NOTIFICATIONS_ENABLED=false` 时不判定、不发送、不写入发送记录。规则可继续在页面维护，启用总开关后才实际生效。`NOTIFICATION_ENCRYPTION_KEY` 缺失或无效时，服务必须拒绝启用通知渠道。

### 7.2 管理 API

| 方法 | 路径 | 作用 |
| --- | --- | --- |
| `GET/POST` | `/api/notification-channels` | 查询渠道或创建渠道；创建请求接收 Apprise URL，响应只返回脱敏配置状态 |
| `PATCH/DELETE` | `/api/notification-channels/<id>` | 编辑渠道、启停或删除渠道；编辑请求可替换 Apprise URL |
| `GET/POST` | `/api/alert-rules` | 查询与新增规则；请求体中的 `channel_ids` 指定已启用渠道 |
| `PATCH/DELETE` | `/api/alert-rules/<id>` | 更新、启停、渠道选择和删除规则 |
| `POST` | `/api/notification-channels/<id>/test` | 向单个渠道发送测试消息，不改变任何规则或告警状态 |
| `POST` | `/api/alert-rules/<id>/evaluate` | 用当前已落库数据立即评估规则，遵循正常状态机、冷却和去重 |
| `GET` | `/api/notification-deliveries` | 按规则、渠道、状态和时间查询发送记录 |
| `GET` | `/api/alert-states` | 查询当前异常、待确认和恢复状态 |

全部 API 沿用既有 Web 登录认证。创建和更新时校验 `market.funding_rate.threshold` 使用 `all_market` 与空对象范围，`market.price_volume.threshold` 使用 `market_rank_top` 与合法的榜单维度/数量，`system.*` 规则只包含已注册任务 ID，并拒绝未配置或未启用的渠道。

渠道 URL 会在创建或更新请求中经过网络传输。服务对外部署时必须启用 HTTPS；本地开发环境只能使用本机访问地址。

## 8. 页面范围

在“任务管理”同级增加“告警管理”页，分成三个无嵌套卡片的区域：

1. 当前异常：显示处于 `pending` / `triggered` 的规则对象、最新指标、首次触发时间和详情链接。
2. 规则：表格展示名称、事件、范围、阈值、渠道、冷却时间与启用开关；使用弹窗创建/编辑，并提供“立即评估”操作。
3. 发送记录：显示状态、发送时间、渠道、摘要和失败原因，不显示密钥。

渠道表格提供“发送测试”操作；规则表格提供“立即评估”操作。首期表单按事件类型展示专属字段，不提供任意 JSON 编辑器。规则默认关闭，用户完成渠道选择和参数检查后显式启用。

## 9. 实施阶段

### 阶段 1：基础设施与运维告警

- 新增五张表和迁移 SQL。
- 在 `requirements.txt` 新增 `apprise` 与 `cryptography`，实现加密渠道配置、发送记录、告警状态机和 Apprise 适配。
- 接入 `system.job.failure` 及其恢复通知。
- 增加规则管理 API，但页面可暂缓。

验收：人为令任务失败后，同一事件只发送一次；任务恢复后只发送一次恢复消息；发送失败不影响任务成功状态。

### 阶段 2：市场告警 MVP

- 接入 `market.funding_rate.threshold` 和 `market.price_volume.threshold` 规则。
- 完成告警管理页面与规则表单。
- 为每类规则增加单元测试、API 契约测试和渠道 mock 测试。

验收：相同周期重复执行不重复发送；阈值恢复后再次穿越才再次发送；未收盘或缺失数据不生成市场事件。

### 阶段 3：稳定性增强

- 在发送记录基础上增加有限重试与失败汇总。
- 加入恢复滞回、每日摘要、渠道级限流和更多渠道。
- 依据实际使用频率评估多交易所分歧告警。

## 10. 测试计划

### 10.1 测试分层

| 层级 | 范围 | 外部依赖 | 通过标准 |
| --- | --- | --- | --- |
| 单元测试 | 加密、参数校验、事件判定、状态机、消息载荷 | 全部 mock | 覆盖所有触发、恢复、跳过和失败分支 |
| 仓储测试 | 五张表、唯一约束、关联关系、发送记录 | 测试数据库 | 事务回滚正确，不产生重复状态或孤立关联 |
| API 契约测试 | 鉴权、CRUD、手动测试、立即评估 | Apprise mock | 不泄露 URL；响应码与 JSON 合同稳定 |
| 页面测试 | 渠道编辑、规则选渠道、状态与记录展示 | 后端 API mock | 密文和 URL 不出现在 DOM；按钮状态正确 |
| 部署冒烟 | 已配置 Telegram/Webhook 的真实测试发送 | 真实渠道，仅人工 | 每个实际渠道收到一条测试消息，生产规则仍保持禁用 |

自动化测试禁止使用真实 Apprise URL、真实 Bot Token 或公网 Webhook。测试配置使用无效占位 URL，发送器统一替换为 spy/mock。

### 10.2 单元与仓储测试

建议新增：

| 测试文件 | 关键场景 |
| --- | --- |
| `tests/test_notification_crypto.py` | Fernet 加解密往返；相同 URL 的密文不同；缺失/错误密钥拒绝启用；旧 `key_version` 可解密；轮换后改写为新版本 |
| `tests/test_notification_channels.py` | Apprise URL 合法性；创建/更新只存密文；删除渠道时规则关联的处理；发送测试成功、超时与失败记录 |
| `tests/test_alert_rules.py` | 三种 `event_type` 与 `scope_type` 匹配；阈值、方向、榜单数量、任务 ID 校验；禁用规则或渠道不参与投递 |
| `tests/test_alert_evaluator.py` | `normal -> triggered -> recovered`；重复执行不重复发送；冷却期只更新状态；同一规则不同币种/方向状态独立；评估异常不向上抛出 |
| `tests/test_funding_rate_alert.py` | `funding_rate` 等于、低于、高于阈值；正/负/绝对值三种方向；`funding_rate is null` 时跳过；恢复后再次穿越可再次触发 |
| `tests/test_price_volume_alert.py` | 仅成交额榜单前 N 参与；涨跌幅和放量条件必须同时成立；上涨/下跌独立；未收盘、序列不足、缺失数据跳过；榜单变更后不再产生新触发 |
| `tests/test_job_failure_alert.py` | 单次与连续任务失败；任务恢复；服务重启后从 `alert_states` 恢复连续失败状态；告警评估失败不改变原任务的成功/失败结果 |

阈值边界必须包含 `>`、`=`、`<` 三组用例；时间相关测试固定注入 `now_ms`，不得依赖真实系统时间或 `sleep`。

### 10.3 API 契约测试

新增 `tests/test_notification_api.py`，至少覆盖：

- 未登录访问全部通知 API 返回 401。
- 创建渠道成功后，`GET /api/notification-channels` 不包含 `config_encrypted`、明文 URL、Token 或 URL 查询参数。
- 失效 Apprise URL、缺少加密主密钥、重复渠道名、删除已被规则引用的渠道均返回明确 4xx。
- 新建/更新规则只能选择启用渠道；`event_type`、`scope_type` 和参数不匹配时返回 400。
- `POST /api/notification-channels/<id>/test` 只生成测试发送记录，不新增或修改 `alert_states`。
- `POST /api/alert-rules/<id>/evaluate` 走正常去重和冷却，不提供绕过状态机的强制发送参数。
- 发送失败时 API 返回可用的业务状态，但采集任务、规则状态和发送记录各自语义正确。

### 10.4 页面测试

在现有 Playwright 套件中新增 `tests/playwright/tests/notification_management.spec.js`：

- 导航栏可进入告警管理页，未登录状态跳转至登录页。
- 创建 Telegram/Webhook 渠道时 URL 输入框只在编辑弹窗出现；保存后列表只展示“已配置”。
- 渠道“发送测试”和规则“立即评估”调用正确 API，并显示成功、失败、加载中状态。
- 规则编辑可多选渠道；禁用渠道不可选；保存后渠道列准确显示。
- 当前异常、发送记录、空数据和 API 失败状态均可读，窄屏下无文本溢出或控件重叠。
- 使用响应 fixture 断言页面 HTML、网络响应和可见文本均不含 `tgram://`、`json://`、Token 或加密密文。

### 10.5 部署验收

部署前按以下顺序人工验证：

1. 设置 `NOTIFICATION_ENCRYPTION_KEY`，启动服务并确认通知总开关默认关闭。
2. 在页面创建 Telegram 与 Webhook 渠道，分别执行“发送测试”，确认收到内容且服务日志没有 URL/Token。
3. 创建三条默认规则但保持禁用，验证规则与渠道关联、发送记录和历史状态为空。
4. 在测试环境启用一条低阈值资金费率规则，手动“立即评估”，验证触发、重复评估去重、恢复和再次触发。
5. 模拟一个核心任务失败并恢复，验证双渠道投递各一次，且任务执行结果没有被通知故障覆盖。
6. 备份数据库后确认只有密文；使用只读 API、页面源代码和日志抽检确认无明文渠道 URL。

建议 CI 顺序：`python -m pytest tests/test_notification_*.py`、相关既有调度测试、再执行通知页面的 Playwright 用例。真实渠道冒烟不纳入 CI。

## 11. 风险与约束

- 市场数据存在延迟和异常值，通知不得以交易指令形式表达。
- `JOB_METADATA` 当前为进程内数据，服务重启会丢失连续失败计数。阶段 1 需要将必要状态持久化到 `alert_states`，并以首次新结果重新建立判断。
- APScheduler 的每项任务需保证告警评估异常被捕获并记录，不能阻塞下一次采集。
- Telegram、Webhook 和代理故障是常态，应通过发送记录、短超时和运维告警处理，不能无限同步重试。
- 首期仅允许管理员配置所有规则；若引入多用户，规则、渠道和发送记录必须增加 `owner_id` 并重新评估权限模型。

## 12. 推荐默认规则

| 名称 | 条件 | 冷却 | 渠道 |
| --- | --- | --- | --- |
| 全市场资金费率异常 | `abs(funding_rate) >= 0.1%` | 30 分钟 | Telegram |
| 成交额榜单币价格放量异动 | 5 分钟涨跌幅 `>= 2%` 且成交额放大 `>= 2x` | 30 分钟 | Telegram |
| 关键任务失败 | 任一核心任务失败 1 次 | 15 分钟 | Telegram + Webhook |

默认规则应随迁移创建但保持禁用，避免升级部署后未经确认就向外发送消息。

## 13. 实现同步（当前版本）

本节优先于前文中与当前实现冲突的页面、投递与 API 描述。

### 13.1 评估与投递

- 通知数据表共六张，除渠道、规则、规则渠道、状态、投递记录外，新增 `alert_evaluation_runs` 保存手动评估的开始/结束时间、检查数、命中数、投递数和错误摘要。
- 资金费率规则一次评估只发送一条汇总通知；币种状态仍逐个维护，用于后续恢复判断与去重。
- 汇总消息列出触发币种；恢复币种使用 `alert_states.last_value_json` 中的上次 `funding_rate` 展示“之前费率 / 当前费率 / 阈值”，不新增字段。
- 资金费率汇总投递的 `event_status` 为 `summary`，不再按每个币种分别写入 `triggered` 或 `recovered` 投递记录。

### 13.2 页面与查询

- 首页仅加载通知渠道和规则摘要；不展示全局状态、全局评估记录或全局投递列表。
- 规则详情按需加载三个分页页签：状态、评估记录、投递记录。各接口使用 `limit` 和 `offset`，默认每页 50 条。
- “立即评估”只执行所选规则；`all_market` 仅表示该规则会扫描全部币种。

### 13.3 补充 API

| 方法 | 路径 | 作用 |
| --- | --- | --- |
| `GET` | `/api/alert-evaluation-runs` | 最近评估记录 |
| `GET` | `/api/alert-evaluation-runs/<id>/logs` | 单次评估生命周期和投递日志 |
| `GET` | `/api/alert-rules/<id>/states?limit=&offset=` | 单规则状态分页 |
| `GET` | `/api/alert-rules/<id>/evaluation-runs?limit=&offset=` | 单规则评估记录分页 |
| `GET` | `/api/alert-rules/<id>/deliveries?limit=&offset=` | 单规则投递记录分页 |

### 13.4 测试要求

资金费率规则必须验证一次评估最多生成一条汇总投递；恢复内容必须包含之前费率和当前费率。页面测试必须验证规则详情的按需请求和分页，不再依赖首页全局状态或投递列表。
