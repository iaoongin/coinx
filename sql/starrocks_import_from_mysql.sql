-- StarRocks 从 MySQL 全量导入脚本（通过 Catalog）
-- 用法：mysql -h <sr_host> -P 9030 -u <user> -p < starrocks_import_from_mysql.sql
-- 或者粘贴到 StarRocks 的 SQL 编辑器逐段执行
--
-- 前置条件：
--   1. StarRocks 已创建 MySQL catalog（本文末尾有示例）
--   2. StarRocks 表已按 schema_starrocks.sql 建好
-- 注意：Coins 表和 market_snapshots/market_tickers 数据量小，可全量覆盖；
--       大表建议用 INSERT OVERWRITE 或拆成批次导入，避免长事务

-- ============================================================================
-- 第一步：创建 MySQL Catalog（如已存在可跳过）
-- ============================================================================
-- CREATE EXTERNAL CATALOG mysql_catalog
-- PROPERTIES (
--     "type" = "mysql",
--     "host" = "127.0.0.1",
--     "port" = "3306",
--     "user" = "root",
--     "password" = "your_password",
--     "database" = "coinx"
-- );

-- ============================================================================
-- 第二步：建表（见 schema_starrocks.sql，已单独执行可跳过）
-- ============================================================================
-- source schema_starrocks.sql;

-- ============================================================================
-- 第三步：导入数据
-- ============================================================================

-- 3.1 币种配置表（全量覆盖）
INSERT INTO default_catalog.coinx.coins (symbol, is_tracking, base_asset, quote_asset, margin_asset,
                   price_precision, quantity_precision, base_asset_precision,
                   quote_precision, status, onboard_date, delivery_date,
                   contract_type, underlying_type, liquidation_fee,
                   maint_margin_percent, required_margin_percent,
                   created_at, updated_at)
SELECT symbol, is_tracking, base_asset, quote_asset, margin_asset,
       price_precision, quantity_precision, base_asset_precision,
       quote_precision, status, onboard_date, delivery_date,
       contract_type, underlying_type, liquidation_fee,
       maint_margin_percent, required_margin_percent,
       created_at, updated_at
FROM mysql_catalog.coinx.coins;

-- 3.2 持仓量历史（大表，按 exchange,symbol 分批执行更安全）
INSERT INTO default_catalog.coinx.market_open_interest_hist (exchange, symbol, period, event_time,
                                       sum_open_interest, sum_open_interest_value,
                                       created_at, updated_at)
SELECT exchange, symbol, period, event_time,
       sum_open_interest, sum_open_interest_value,
       created_at, updated_at
FROM mysql_catalog.coinx.market_open_interest_hist;
-- 分批写法示例（如需）：
-- INSERT INTO default_catalog.coinx.market_open_interest_hist ...
-- SELECT ... FROM mysql_catalog.coinx.market_open_interest_hist
-- WHERE exchange = 'binance' AND symbol = 'BTCUSDT';

-- 3.3 K线历史（大表，建议分批）
INSERT INTO default_catalog.coinx.market_klines (exchange, symbol, period, open_time, close_time,
                           open_price, high_price, low_price, close_price,
                           volume, quote_volume, trade_count,
                           taker_buy_base_volume, taker_buy_quote_volume,
                           created_at, updated_at)
SELECT exchange, symbol, period, open_time, close_time,
       open_price, high_price, low_price, close_price,
       volume, quote_volume, trade_count,
       taker_buy_base_volume, taker_buy_quote_volume,
       created_at, updated_at
FROM mysql_catalog.coinx.market_klines;

-- 3.4 Taker 买卖量历史（大表，建议分批）
INSERT INTO default_catalog.coinx.market_taker_buy_sell_vol (exchange, symbol, period, event_time,
                                       buy_sell_ratio, buy_vol, sell_vol,
                                       created_at, updated_at)
SELECT exchange, symbol, period, event_time,
       buy_sell_ratio, buy_vol, sell_vol,
       created_at, updated_at
FROM mysql_catalog.coinx.market_taker_buy_sell_vol;

-- 3.5 资金费率历史
INSERT INTO default_catalog.coinx.market_funding_rate (symbol, period, event_time,
                                 funding_rate, predicted_rate,
                                 next_funding_time, mark_price, exchange,
                                 created_at)
SELECT symbol, period, event_time,
       funding_rate, predicted_rate,
       next_funding_time, mark_price, exchange,
       created_at
FROM mysql_catalog.coinx.market_funding_rate;

-- 3.6 市场快照表（小表，全量覆盖）
INSERT INTO default_catalog.coinx.market_snapshots (snapshot_time, symbol, batch_id,
                              price, open_interest, open_interest_value,
                              created_at)
SELECT snapshot_time, symbol, batch_id,
       price, open_interest, open_interest_value,
       created_at
FROM mysql_catalog.coinx.market_snapshots;

-- 3.7 行情 tickers（小表，全量覆盖）
INSERT INTO default_catalog.coinx.market_tickers (close_time, symbol, price_change,
                            price_change_percent, weighted_avg_price,
                            last_price, last_qty, open_price,
                            high_price, low_price, volume, quote_volume,
                            open_time, first_id, last_id, count,
                            created_at)
SELECT close_time, symbol, price_change,
       price_change_percent, weighted_avg_price,
       last_price, last_qty, open_price,
       high_price, low_price, volume, quote_volume,
       open_time, first_id, last_id, count,
       created_at
FROM mysql_catalog.coinx.market_tickers;

-- ============================================================================
-- 验证：源 vs 目标 行数对比
-- ============================================================================
SELECT 'coins' AS tbl,
       (SELECT COUNT(*) FROM mysql_catalog.coinx.coins)  AS mysql_cnt,
       (SELECT COUNT(*) FROM default_catalog.coinx.coins) AS sr_cnt,
       (SELECT COUNT(*) FROM mysql_catalog.coinx.coins) -
       (SELECT COUNT(*) FROM default_catalog.coinx.coins) AS diff
UNION ALL
SELECT 'market_open_interest_hist',
       (SELECT COUNT(*) FROM mysql_catalog.coinx.market_open_interest_hist),
       (SELECT COUNT(*) FROM default_catalog.coinx.market_open_interest_hist),
       (SELECT COUNT(*) FROM mysql_catalog.coinx.market_open_interest_hist) -
       (SELECT COUNT(*) FROM default_catalog.coinx.market_open_interest_hist)
UNION ALL
SELECT 'market_klines',
       (SELECT COUNT(*) FROM mysql_catalog.coinx.market_klines),
       (SELECT COUNT(*) FROM default_catalog.coinx.market_klines),
       (SELECT COUNT(*) FROM mysql_catalog.coinx.market_klines) -
       (SELECT COUNT(*) FROM default_catalog.coinx.market_klines)
UNION ALL
SELECT 'market_taker_buy_sell_vol',
       (SELECT COUNT(*) FROM mysql_catalog.coinx.market_taker_buy_sell_vol),
       (SELECT COUNT(*) FROM default_catalog.coinx.market_taker_buy_sell_vol),
       (SELECT COUNT(*) FROM mysql_catalog.coinx.market_taker_buy_sell_vol) -
       (SELECT COUNT(*) FROM default_catalog.coinx.market_taker_buy_sell_vol)
UNION ALL
SELECT 'market_funding_rate',
       (SELECT COUNT(*) FROM mysql_catalog.coinx.market_funding_rate),
       (SELECT COUNT(*) FROM default_catalog.coinx.market_funding_rate),
       (SELECT COUNT(*) FROM mysql_catalog.coinx.market_funding_rate) -
       (SELECT COUNT(*) FROM default_catalog.coinx.market_funding_rate)
UNION ALL
SELECT 'market_snapshots',
       (SELECT COUNT(*) FROM mysql_catalog.coinx.market_snapshots),
       (SELECT COUNT(*) FROM default_catalog.coinx.market_snapshots),
       (SELECT COUNT(*) FROM mysql_catalog.coinx.market_snapshots) -
       (SELECT COUNT(*) FROM default_catalog.coinx.market_snapshots)
UNION ALL
SELECT 'market_tickers',
       (SELECT COUNT(*) FROM mysql_catalog.coinx.market_tickers),
       (SELECT COUNT(*) FROM default_catalog.coinx.market_tickers),
       (SELECT COUNT(*) FROM mysql_catalog.coinx.market_tickers) -
       (SELECT COUNT(*) FROM default_catalog.coinx.market_tickers)
ORDER BY tbl;
