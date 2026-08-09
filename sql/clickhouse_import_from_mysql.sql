-- MySQL 到 ClickHouse 的全量行情数据导入。
-- 使用前：
--   1. 已执行 schema_clickhouse.sql。
--   2. 将下方 MySQL 连接参数替换为实际值；地址以 ClickHouse 服务端视角填写。
--   3. 导入期间暂停采集任务，或按时间范围分批导入并在切换前补一次增量。
--   4. 通过 clickhouse-client --multiquery < sql/clickhouse_import_from_mysql.sql 执行。

CREATE DATABASE IF NOT EXISTS mysql_source
ENGINE = MySQL(
    'mysql:3306',       -- MySQL 地址和端口；Docker 同网络时通常为 mysql:3306
    'coinx',            -- MySQL 数据库名
    'coinx',            -- MySQL 用户名
    'coinx_password'    -- MySQL 密码
);

-- 将 MySQL 的 DATETIME 按北京时间解释并写入 DateTime64(3, 'Asia/Shanghai')。
-- 本项目的 MySQL DATETIME 由应用侧 datetime.now() 生成，约定为北京时间。

INSERT INTO coinx.market_klines
(
    exchange, symbol, period, open_time, close_time,
    open_price, high_price, low_price, close_price,
    volume, quote_volume, trade_count,
    taker_buy_base_volume, taker_buy_quote_volume,
    created_at, updated_at
)
SELECT
    exchange, symbol, period, open_time, close_time,
    open_price, high_price, low_price, close_price,
    volume, quote_volume, trade_count,
    taker_buy_base_volume, taker_buy_quote_volume,
    toDateTime64(created_at, 3, 'Asia/Shanghai'),
    toDateTime64(updated_at, 3, 'Asia/Shanghai')
FROM mysql_source.market_klines;

INSERT INTO coinx.market_open_interest_hist
(
    exchange, symbol, period, event_time,
    sum_open_interest, sum_open_interest_value,
    created_at, updated_at
)
SELECT
    exchange, symbol, period, event_time,
    sum_open_interest, sum_open_interest_value,
    toDateTime64(created_at, 3, 'Asia/Shanghai'),
    toDateTime64(updated_at, 3, 'Asia/Shanghai')
FROM mysql_source.market_open_interest_hist;

INSERT INTO coinx.market_taker_buy_sell_vol
(
    exchange, symbol, period, event_time,
    buy_sell_ratio, buy_vol, sell_vol,
    created_at, updated_at
)
SELECT
    exchange, symbol, period, event_time,
    buy_sell_ratio, buy_vol, sell_vol,
    toDateTime64(created_at, 3, 'Asia/Shanghai'),
    toDateTime64(updated_at, 3, 'Asia/Shanghai')
FROM mysql_source.market_taker_buy_sell_vol;

INSERT INTO coinx.market_funding_rate
(
    exchange, symbol, period, event_time,
    funding_rate, predicted_rate, next_funding_time, mark_price,
    created_at, updated_at
)
SELECT
    exchange, symbol, period, event_time,
    funding_rate, predicted_rate, next_funding_time, mark_price,
    toDateTime64(created_at, 3, 'Asia/Shanghai'),
    toDateTime64(created_at, 3, 'Asia/Shanghai')
FROM mysql_source.market_funding_rate;

INSERT INTO coinx.market_snapshots
(
    snapshot_time, symbol, batch_id,
    price, open_interest, open_interest_value, data_json,
    created_at
)
SELECT
    snapshot_time, symbol, batch_id,
    price, open_interest, open_interest_value, ifNull(toString(data_json), '{}'),
    toDateTime64(created_at, 3, 'Asia/Shanghai')
FROM mysql_source.market_snapshots;

INSERT INTO coinx.market_tickers
(
    close_time, symbol,
    price_change, price_change_percent, weighted_avg_price,
    last_price, last_qty, open_price, high_price, low_price,
    volume, quote_volume, open_time, first_id, last_id, count,
    created_at, updated_at
)
SELECT
    close_time, symbol,
    price_change, price_change_percent, weighted_avg_price,
    last_price, last_qty, open_price, high_price, low_price,
    volume, quote_volume, open_time, first_id, last_id, count,
    toDateTime64(created_at, 3, 'Asia/Shanghai'),
    toDateTime64(created_at, 3, 'Asia/Shanghai')
FROM mysql_source.market_tickers;

-- 导入完成后校验行数。ReplacingMergeTree 表可使用 FINAL 校验去重后的逻辑行数。
SELECT 'market_klines' AS table_name,
       (SELECT count() FROM mysql_source.market_klines) AS mysql_count,
       (SELECT count() FROM coinx.market_klines FINAL) AS clickhouse_count;

SELECT 'market_open_interest_hist' AS table_name,
       (SELECT count() FROM mysql_source.market_open_interest_hist) AS mysql_count,
       (SELECT count() FROM coinx.market_open_interest_hist FINAL) AS clickhouse_count;

SELECT 'market_taker_buy_sell_vol' AS table_name,
       (SELECT count() FROM mysql_source.market_taker_buy_sell_vol) AS mysql_count,
       (SELECT count() FROM coinx.market_taker_buy_sell_vol FINAL) AS clickhouse_count;

SELECT 'market_funding_rate' AS table_name,
       (SELECT count() FROM mysql_source.market_funding_rate) AS mysql_count,
       (SELECT count() FROM coinx.market_funding_rate FINAL) AS clickhouse_count;

SELECT 'market_snapshots' AS table_name,
       (SELECT count() FROM mysql_source.market_snapshots) AS mysql_count,
       (SELECT count() FROM coinx.market_snapshots) AS clickhouse_count;

SELECT 'market_tickers' AS table_name,
       (SELECT count() FROM mysql_source.market_tickers) AS mysql_count,
       (SELECT count() FROM coinx.market_tickers) AS clickhouse_count;
