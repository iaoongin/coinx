from dataclasses import dataclass


HOMEPAGE_SERIES_TYPES = ('klines', 'open_interest_hist', 'taker_buy_sell_vol')


@dataclass(frozen=True)
class ExchangeSeriesAdapter:
    exchange_id: str
    supported_series_types: tuple
    fetch_series_payload: object
    parse_series_payload: object
    precise_window_series_types: tuple = ()
    is_symbol_supported: object = None

    def supports_time_window(self, series_type):
        return series_type in self.precise_window_series_types

    def supports_symbol(self, symbol, series_type=None, session=None):
        if self.is_symbol_supported is None:
            return True
        return self.is_symbol_supported(symbol, series_type=series_type, session=session)


def _build_binance_adapter():
    from coinx.collector.binance import series as binance_series

    return ExchangeSeriesAdapter(
        exchange_id='binance',
        supported_series_types=HOMEPAGE_SERIES_TYPES,
        fetch_series_payload=binance_series.fetch_series_payload,
        parse_series_payload=binance_series.parse_series_payload,
        precise_window_series_types=HOMEPAGE_SERIES_TYPES,
    )


def _build_okx_adapter():
    from coinx.collector.okx import series as okx_series

    return ExchangeSeriesAdapter(
        exchange_id='okx',
        supported_series_types=tuple(okx_series.SUPPORTED_SERIES_TYPES),
        fetch_series_payload=okx_series.fetch_series_payload,
        parse_series_payload=okx_series.parse_series_payload,
        precise_window_series_types=('klines',),
        is_symbol_supported=okx_series.is_symbol_supported,
    )


def get_exchange_adapter(exchange_id):
    registry = {
        'binance': _build_binance_adapter,
        'okx': _build_okx_adapter,
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
