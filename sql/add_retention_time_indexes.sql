-- Apply to an existing MySQL database after checking disk space and workload.
-- Inspect existing indexes first; run missing statements one at a time.
ALTER TABLE market_klines ADD INDEX idx_mk_close_time (close_time), ALGORITHM=INPLACE, LOCK=NONE;
ALTER TABLE market_open_interest_hist ADD INDEX idx_moih_event_time (event_time), ALGORITHM=INPLACE, LOCK=NONE;
ALTER TABLE market_taker_buy_sell_vol ADD INDEX idx_mtbsv_event_time (event_time), ALGORITHM=INPLACE, LOCK=NONE;
ALTER TABLE market_funding_rate ADD INDEX idx_funding_event_time (event_time), ALGORITHM=INPLACE, LOCK=NONE;
