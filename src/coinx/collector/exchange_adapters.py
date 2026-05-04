from dataclasses import dataclass


HOMEPAGE_SERIES_TYPES = ('klines', 'open_interest_hist', 'taker_buy_sell_vol')
DEFAULT_TAKER_PERIOD_BY_INTERVAL = {
    '5m': '5m',
    '15m': '5m',
    '30m': '5m',
    '1h': '5m',
    '4h': '5m',
    '12h': '5m',
    '24h': '5m',
    '48h': '5m',
    '72h': '5m',
    '168h': '5m',
}
OKX_TAKER_PERIOD_BY_INTERVAL = {
    **DEFAULT_TAKER_PERIOD_BY_INTERVAL,
    '48h': '1H',
    '72h': '1H',
    '168h': '1H',
}


@dataclass(frozen=True)
class ExchangeSeriesAdapter:
    exchange_id: str
    supported_series_types: tuple
    fetch_series_payload: object
    parse_series_payload: object
    precise_window_series_types: tuple = ()
    is_symbol_supported: object = None
    supported_symbols_fetcher: object = None
    page_limits: dict = None
    series_periods: dict = None
    taker_period_by_interval: dict = None

    def supports_time_window(self, series_type):
        return series_type in self.precise_window_series_types

    def supports_symbol(self, symbol, series_type=None, session=None):
        if self.is_symbol_supported is None:
            return True
        return self.is_symbol_supported(symbol, series_type=series_type, session=session)

    def symbol_support_state(self, symbol, series_type=None, session=None):
        if self.supported_symbols_fetcher is None:
            return {
                'state': 'supported',
                'supported': True,
                'known': True,
            }

        try:
            supported_symbols = self.supported_symbols_fetcher(session=session)
        except Exception as exc:
            return {
                'state': 'unknown',
                'supported': None,
                'known': False,
                'reason': 'supported_symbol_lookup_failed',
                'details': {'error': str(exc)},
            }

        is_supported = symbol in supported_symbols
        return {
            'state': 'supported' if is_supported else 'unsupported',
            'supported': is_supported,
            'known': True,
        }

    def page_limit(self, series_type):
        if not self.page_limits:
            return None
        return self.page_limits.get(series_type)

    def periods_for_series(self, series_type):
        if not self.series_periods:
            return ('5m',)
        return tuple(self.series_periods.get(series_type, ('5m',)))

    def taker_period_for_interval(self, interval):
        if not self.taker_period_by_interval:
            return None
        return self.taker_period_by_interval.get(interval)


def _build_binance_adapter():
    from coinx.collector.binance import series as binance_series

    return ExchangeSeriesAdapter(
        exchange_id='binance',
        supported_series_types=HOMEPAGE_SERIES_TYPES,
        fetch_series_payload=binance_series.fetch_series_payload,
        parse_series_payload=binance_series.parse_series_payload,
        precise_window_series_types=HOMEPAGE_SERIES_TYPES,
        taker_period_by_interval=DEFAULT_TAKER_PERIOD_BY_INTERVAL,
    )


def _build_okx_adapter():
    from coinx.collector.okx import series as okx_series

    return ExchangeSeriesAdapter(
        exchange_id='okx',
        supported_series_types=tuple(okx_series.SUPPORTED_SERIES_TYPES),
        fetch_series_payload=okx_series.fetch_series_payload,
        parse_series_payload=okx_series.parse_series_payload,
        precise_window_series_types=tuple(okx_series.SUPPORTED_SERIES_TYPES),
        is_symbol_supported=okx_series.is_symbol_supported,
        supported_symbols_fetcher=okx_series.get_supported_symbols,
        series_periods={
            'taker_buy_sell_vol': ('5m', '1H'),
        },
        taker_period_by_interval=OKX_TAKER_PERIOD_BY_INTERVAL,
    )


def _build_bybit_adapter():
    from coinx.collector.bybit import series as bybit_series

    return ExchangeSeriesAdapter(
        exchange_id='bybit',
        supported_series_types=tuple(bybit_series.SUPPORTED_SERIES_TYPES),
        fetch_series_payload=bybit_series.fetch_series_payload,
        parse_series_payload=bybit_series.parse_series_payload,
        precise_window_series_types=tuple(bybit_series.SUPPORTED_SERIES_TYPES),
        is_symbol_supported=bybit_series.is_symbol_supported,
        supported_symbols_fetcher=bybit_series.get_supported_symbols,
        page_limits={
            'klines': 1000,
            'open_interest_hist': 200,
        },
    )


def get_exchange_adapter(exchange_id):
    registry = {
        'binance': _build_binance_adapter,
        'okx': _build_okx_adapter,
        'bybit': _build_bybit_adapter,
    }
    key = (exchange_id or '').strip().lower()
    try:
        return registry[key]()
    except KeyError as exc:
        raise ValueError(f'unsupported exchange: {exchange_id}') from exc


def get_exchange_adapters(exchange_ids):
    adapters = []
    for exchange_id in exchange_ids:
        if exchange_id and exchange_id.strip():
            adapters.append(get_exchange_adapter(exchange_id))
    return adapters


def get_supported_exchange_ids():
    return list({'binance': None, 'okx': None, 'bybit': None}.keys())
