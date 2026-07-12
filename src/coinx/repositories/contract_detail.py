from coinx.repositories.funding_rate import load_latest_funding_rates
from coinx.repositories.homepage_series import get_homepage_series_snapshot
from coinx.repositories.market_structure_score import get_market_structure_score_snapshot
from coinx.utils import logger


INTERVAL_ORDER = ('5m', '15m', '30m', '1h', '4h', '12h', '24h', '48h', '72h', '168h')


def _first_symbol(snapshot, symbol):
    for item in (snapshot or {}).get('data') or []:
        if str(item.get('symbol', '')).upper() == symbol:
            return item
    return None


def _load_optional(loader, symbols, label):
    try:
        return loader(symbols)
    except Exception as exc:
        logger.warning('加载合约详情%s失败: symbols=%s error=%s', label, symbols, exc)
        return None


def _build_intervals(homepage):
    changes = homepage.get('changes') or {}
    net_inflow = homepage.get('net_inflow') or {}
    net_inflow_value = homepage.get('net_inflow_value') or {}
    net_inflow_value_formatted = homepage.get('net_inflow_value_formatted') or {}
    rows = []
    for interval in INTERVAL_ORDER:
        change = changes.get(interval) or {}
        rows.append({
            'interval': interval,
            'price_change_percent': change.get('price_change_percent'),
            'open_interest_change_percent': change.get('ratio'),
            'open_interest_value_change_percent': change.get('value_ratio'),
            'net_inflow': net_inflow.get(interval),
            'net_inflow_value': net_inflow_value.get(interval),
            'net_inflow_value_formatted': net_inflow_value_formatted.get(interval),
        })
    return rows


def get_contract_detail(
    symbol,
    homepage_loader=get_homepage_series_snapshot,
    score_loader=get_market_structure_score_snapshot,
    funding_loader=load_latest_funding_rates,
):
    """Build a contract detail view from stored snapshots only."""
    normalized_symbol = symbol.upper()
    homepage_snapshot = homepage_loader([normalized_symbol])
    homepage = _first_symbol(homepage_snapshot, normalized_symbol)
    if homepage is None:
        return None

    score_snapshot = _load_optional(score_loader, [normalized_symbol], '结构评分')
    structure_score = _first_symbol(score_snapshot, normalized_symbol) if score_snapshot else None
    funding_map = _load_optional(funding_loader, [normalized_symbol], '资金费率') or {}
    funding = funding_map.get(normalized_symbol) or {}

    funding_rate = funding.get('funding_rate', homepage.get('funding_rate'))
    predicted_rate = funding.get('predicted_rate', homepage.get('predicted_funding_rate'))
    next_funding_time = funding.get('next_funding_time', homepage.get('next_funding_time'))
    timestamps = [
        (homepage_snapshot or {}).get('cache_update_time'),
        funding.get('event_time'),
        (score_snapshot or {}).get('cache_update_time') if score_snapshot else None,
    ]
    timestamps = [value for value in timestamps if value is not None]

    return {
        'symbol': normalized_symbol,
        'as_of': min(timestamps) if timestamps else None,
        'data_status': homepage.get('status') or 'partial',
        'included_exchanges': homepage.get('included_exchanges') or [],
        'missing_exchanges': homepage.get('missing_exchanges') or [],
        'summary': {
            'latest_price': homepage.get('current_price'),
            'latest_price_formatted': homepage.get('current_price_formatted'),
            'price_change_24h_percent': homepage.get('price_change_percent'),
            'quote_volume_24h': homepage.get('quote_volume_24h'),
            'open_interest': homepage.get('current_open_interest'),
            'open_interest_formatted': homepage.get('current_open_interest_formatted'),
            'open_interest_value': homepage.get('current_open_interest_value'),
            'open_interest_value_formatted': homepage.get('current_open_interest_value_formatted'),
            'funding_rate': funding_rate,
            'predicted_funding_rate': predicted_rate,
            'next_funding_time': next_funding_time,
            'mark_price': funding.get('mark_price'),
        },
        'intervals': _build_intervals(homepage),
        'exchange_distribution': homepage.get('exchange_open_interest') or [],
        'structure_score': structure_score,
    }
