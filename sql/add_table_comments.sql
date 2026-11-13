-- 为已存在的 CoinX MySQL 表补充表级注释。
-- 执行前请确认目标数据库中已存在对应表；本脚本不修改字段、索引或数据。

ALTER TABLE coins COMMENT = '币种配置表';
ALTER TABLE market_snapshots COMMENT = '市场数据快照表';
ALTER TABLE market_futures_smart_money_signal COMMENT = '聪明钱快照';
ALTER TABLE market_tickers COMMENT = '行情快照原始数据表';
ALTER TABLE market_open_interest_hist COMMENT = '持仓量历史';
ALTER TABLE market_klines COMMENT = '多交易所K线历史数据表';
ALTER TABLE market_taker_buy_sell_vol COMMENT = '主动买卖历史';
ALTER TABLE market_funding_rate COMMENT = '资金费率历史';
ALTER TABLE notification_channels COMMENT = '通知渠道';
ALTER TABLE alert_rules COMMENT = '告警规则';
ALTER TABLE alert_rule_channels COMMENT = '规则与渠道关联';
ALTER TABLE alert_states COMMENT = '告警状态';
ALTER TABLE notification_deliveries COMMENT = '通知发送记录';
ALTER TABLE alert_evaluation_runs COMMENT = '告警规则评估记录';
ALTER TABLE alert_evaluation_metrics COMMENT = '告警评估指标';
ALTER TABLE rss_subscriptions COMMENT = 'RSS订阅源配置';
ALTER TABLE rss_articles COMMENT = 'RSS文章记录';
ALTER TABLE scheduled_job_runs COMMENT = '定时任务执行记录';

-- 该表由 APScheduler 自动创建，不属于 CoinX 的 schema.sql。
ALTER TABLE apscheduler_jobs COMMENT = 'APScheduler 调度任务存储';
