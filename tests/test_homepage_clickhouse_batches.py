import pytest

from coinx.repositories import homepage_series as homepage


@pytest.mark.parametrize('batch_size,effective_size', [(2, 2), (32, 32), (0, 32), (-1, 1)])
@pytest.mark.parametrize('supports_taker', [True, False])
def test_homepage_batches_preserve_rows_anchors_and_net_inflow(
    monkeypatch, batch_size, effective_size, supports_taker,
):
    symbols = [f'COIN{index}USDT' for index in range(35)]
    upper = 1_700_000_000_000
    latest_taker = upper - homepage.FIVE_MINUTES_MS
    shared_anchors = {symbol: latest_taker for symbol in symbols}
    shared_anchors[symbols[-1]] -= 2 * homepage.FIVE_MINUTES_MS
    lower = min(shared_anchors.values()) - homepage.MAX_TIME_INTERVAL_MS
    data = {
        'market_open_interest_hist': [],
        'market_klines': [],
        'market_taker_buy_sell_vol': [],
    }
    for index, symbol in enumerate(symbols):
        for timestamp in (lower, latest_taker, upper):
            data['market_open_interest_hist'].append({
                'symbol': symbol, 'event_time': timestamp,
                'sum_open_interest': index + 1, 'sum_open_interest_value': (index + 1) * 100,
            })
            data['market_klines'].append({
                'symbol': symbol, 'open_time': timestamp, 'close_price': index + 100,
            })
            if timestamp != upper:
                data['market_taker_buy_sell_vol'].append({
                    'symbol': symbol, 'event_time': timestamp, 'buy_vol': index + 2, 'sell_vol': 1,
                })

    class Repository:
        def __init__(self):
            self.calls = []

        def market_rows(self, table, columns, **kwargs):
            self.calls.append((table, kwargs))
            return [dict(row) for row in data[table] if row['symbol'] in kwargs['symbols']]

    repository = Repository()
    monkeypatch.setattr(homepage, 'get_clickhouse_repository', lambda: repository)
    monkeypatch.setattr(homepage, '_has_unreliable_taker_source', lambda exchange: not supports_taker)

    def load():
        return homepage._load_homepage_exchange_maps_clickhouse(
            'binance', symbols + [symbols[0]], upper_bound=upper,
            shared_anchor_by_symbol=shared_anchors,
            candidate_latest_override={symbol: upper for symbol in symbols},
        )

    monkeypatch.setattr(homepage, 'CLICKHOUSE_AGGREGATION_SYMBOL_BATCH_SIZE', 100)
    unbatched = load()
    repository.calls.clear()
    monkeypatch.setattr(homepage, 'CLICKHOUSE_AGGREGATION_SYMBOL_BATCH_SIZE', batch_size)
    batched = load()

    assert batched == unbatched
    expected_batches = [symbols[start:start + effective_size] for start in range(0, len(symbols), effective_size)]
    for table in data:
        calls = [kwargs for name, kwargs in repository.calls if name == table]
        if table == 'market_taker_buy_sell_vol' and not supports_taker:
            assert calls == []
            continue
        assert [call['symbols'] for call in calls] == expected_batches
        for call in calls:
            assert call['exchange'] == 'binance'
            assert call['period'] == '5m'
            assert call['lower_bound'] == lower
            assert call['upper_bound'] == upper
            assert call['final'] is True
            assert call['deduplicate'] is False

    oi, klines, net, latest = batched
    for index, symbol in enumerate(symbols):
        assert lower in oi[symbol] and lower in klines[symbol]
        assert latest[symbol] == (latest_taker if supports_taker else upper)
        if supports_taker:
            assert net[symbol]['net_inflow']['5m'] == index + 1
            assert net[symbol]['net_inflow_value']['5m'] == (index + 1) * (index + 100)
            assert net[symbol]['health']['5m'] == 100.0
            assert len(net[symbol]['_raw_taker_rows']) == 2


def test_homepage_detail_reads_skip_symbols_without_candidate_anchor(monkeypatch):
    symbols = ['ACTIVE1', 'ACTIVE2', 'STALE1', 'STALE2']
    candidates = {'ACTIVE1': 1_700_000_000_000, 'ACTIVE2': 1_700_000_000_000}
    upper = 1_700_000_000_000
    data = {
        'market_open_interest_hist': [
            {'symbol': symbol, 'event_time': upper, 'sum_open_interest': 1, 'sum_open_interest_value': 10, 'updated_at': '2026-01-01 00:00:00'}
            for symbol in candidates
        ],
        'market_klines': [
            {'symbol': symbol, 'open_time': upper, 'close_price': 100, 'updated_at': '2026-01-01 00:00:00'}
            for symbol in candidates
        ],
        'market_taker_buy_sell_vol': [
            {'symbol': symbol, 'event_time': upper, 'buy_vol': 2, 'sell_vol': 1, 'updated_at': '2026-01-01 00:00:00'}
            for symbol in candidates
        ],
    }
    data['market_klines'].append({
        'symbol': 'ACTIVE1', 'open_time': upper, 'close_price': 101,
        'updated_at': '2026-01-02 00:00:00',
    })

    class Repository:
        def __init__(self):
            self.calls = []

        def market_rows(self, table, columns, **kwargs):
            self.calls.append((table, kwargs['symbols']))
            return [dict(row) for row in data[table] if row['symbol'] in kwargs['symbols']]

    repository = Repository()
    monkeypatch.setattr(homepage, 'get_clickhouse_repository', lambda: repository)
    monkeypatch.setattr(homepage, '_has_unreliable_taker_source', lambda exchange: False)
    monkeypatch.setattr(homepage, 'CLICKHOUSE_AGGREGATION_SYMBOL_BATCH_SIZE', 8)

    oi, klines, net, latest = homepage._load_homepage_exchange_maps_clickhouse(
        'binance', symbols, upper_bound=upper,
        candidate_latest_override=candidates,
    )

    assert all(batch == list(candidates) for _table, batch in repository.calls)
    assert set(oi) == set(klines) == set(net) == set(symbols)
    assert oi['STALE1'] == {}
    assert klines['ACTIVE1'][upper].close_price == 101
    assert klines['STALE2'] == {}
    assert latest == candidates

