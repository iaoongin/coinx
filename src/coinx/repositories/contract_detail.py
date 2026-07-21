from coinx.repositories.funding_rate import load_latest_funding_rates
from coinx.repositories.homepage_series import get_homepage_series_snapshot
from coinx.repositories.market_structure_score import get_market_structure_score_snapshot
from coinx.database import get_session
from coinx.models import MarketFundingRate, MarketKline, MarketOpenInterestHist, MarketTakerBuySellVol
from coinx.utils import logger


INTERVAL_ORDER = ('5m', '15m', '30m', '1h', '4h', '12h', '24h', '48h', '72h', '168h')
RANGE_HOURS = {'1h': 1, '4h': 4, '24h': 24, '72h': 72, '7d': 168}


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


def _difference(current, previous):
    if current is None or previous is None:
        return None
    return float(current) - float(previous)


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
            'price_change': change.get('price_change'),
            'price_change_percent': change.get('price_change_percent'),
            'open_interest_change': _difference(homepage.get('current_open_interest'), change.get('open_interest')),
            'open_interest_change_percent': change.get('ratio'),
            'open_interest_value_change': _difference(homepage.get('current_open_interest_value'), change.get('open_interest_value')),
            'open_interest_value_change_percent': change.get('value_ratio'),
            'net_inflow': net_inflow.get(interval),
            'net_inflow_value': net_inflow_value.get(interval),
            'net_inflow_value_formatted': net_inflow_value_formatted.get(interval),
        })
    return rows


def get_contract_detail(
    symbol,
    homepage_loader=get_homepage_series_snapshot,
    funding_loader=load_latest_funding_rates,
):
    """Build a contract detail view from stored snapshots only."""
    normalized_symbol = symbol.upper()
    homepage_snapshot = homepage_loader([normalized_symbol])
    homepage = _first_symbol(homepage_snapshot, normalized_symbol)
    if homepage is None:
        return None

    funding_map = _load_optional(funding_loader, [normalized_symbol], '资金费率') or {}
    funding = funding_map.get(normalized_symbol) or {}

    funding_rate = funding.get('funding_rate', homepage.get('funding_rate'))
    predicted_rate = funding.get('predicted_rate', homepage.get('predicted_funding_rate'))
    next_funding_time = funding.get('next_funding_time', homepage.get('next_funding_time'))
    timestamps = [
        (homepage_snapshot or {}).get('cache_update_time'),
        funding.get('event_time'),
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
    }


def get_contract_structure_score(symbol, score_loader=get_market_structure_score_snapshot):
    normalized_symbol = symbol.upper()
    snapshot = score_loader([normalized_symbol])
    return {
        'symbol': normalized_symbol,
        'as_of': (snapshot or {}).get('cache_update_time'),
        'structure_score': _first_symbol(snapshot, normalized_symbol),
    }


def _float(value):
    return float(value) if value is not None else None


def load_contract_chart_series(symbol, range_key='24h', session=None, max_points=300):
    """Load stored 5m series and aggregate exchanges on one timeline."""
    hours = RANGE_HOURS[range_key]
    own_session = session is None
    db = session or get_session()
    try:
        latest_times = [
            db.query(model_time).filter(model_symbol == symbol, model_period == '5m').order_by(model_time.desc()).limit(1).scalar()
            for model_time, model_symbol, model_period in (
                (MarketKline.open_time, MarketKline.symbol, MarketKline.period),
                (MarketOpenInterestHist.event_time, MarketOpenInterestHist.symbol, MarketOpenInterestHist.period),
                (MarketTakerBuySellVol.event_time, MarketTakerBuySellVol.symbol, MarketTakerBuySellVol.period),
            )
        ]
        anchor = max((value for value in latest_times if value is not None), default=None)
        if anchor is None:
            return {'range': range_key, 'anchor_time': None, 'market': [], 'flow': [], 'funding_rate': []}
        cutoff = anchor - hours * 60 * 60 * 1000

        klines = db.query(MarketKline).filter(MarketKline.symbol == symbol, MarketKline.period == '5m', MarketKline.open_time >= cutoff, MarketKline.open_time <= anchor).order_by(MarketKline.open_time).all()
        oi_rows = db.query(MarketOpenInterestHist).filter(MarketOpenInterestHist.symbol == symbol, MarketOpenInterestHist.period == '5m', MarketOpenInterestHist.event_time >= cutoff, MarketOpenInterestHist.event_time <= anchor).order_by(MarketOpenInterestHist.event_time).all()
        flow_rows = db.query(MarketTakerBuySellVol).filter(MarketTakerBuySellVol.symbol == symbol, MarketTakerBuySellVol.period == '5m', MarketTakerBuySellVol.event_time >= cutoff, MarketTakerBuySellVol.event_time <= anchor).order_by(MarketTakerBuySellVol.event_time).all()
        funding_rows = db.query(MarketFundingRate).filter(MarketFundingRate.symbol == symbol, MarketFundingRate.period == '5m', MarketFundingRate.event_time >= cutoff, MarketFundingRate.event_time <= anchor).order_by(MarketFundingRate.event_time).all()

        prices = {}
        volumes = {}
        for row in klines:
            current = prices.get(row.open_time)
            if current is None or row.exchange == 'binance':
                prices[row.open_time] = {'value': _float(row.close_price), 'exchange': row.exchange}
            if row.volume is not None:
                volumes[row.open_time] = volumes.get(row.open_time, 0.0) + float(row.volume)
        oi = {}
        for row in oi_rows:
            item = oi.setdefault(row.event_time, [0.0, False, 0.0, False])
            if row.sum_open_interest_value is not None:
                item[0] += float(row.sum_open_interest_value); item[1] = True
            if row.sum_open_interest is not None:
                item[2] += float(row.sum_open_interest); item[3] = True
        flow = {}
        for row in flow_rows:
            item = flow.setdefault(row.event_time, [0.0, 0.0])
            item[0] += float(row.buy_vol or 0); item[1] += float(row.sell_vol or 0)

        market_times = sorted(set(prices) | set(volumes) | set(oi))
        market = []
        for t in market_times:
            price = (prices.get(t) or {}).get('value')
            oi_item = oi.get(t, [None, False, None, False])
            open_interest_value = oi_item[0] if oi_item[1] else None
            market.append({
                'time': t,
                'price': price,
                'volume': volumes.get(t),
                'open_interest_value': open_interest_value,
                'open_interest': oi_item[2] if oi_item[3] else None,
            })
        flow_data = [{'time': t, 'buy_volume': values[0], 'sell_volume': values[1], 'net_inflow': values[0] - values[1]} for t, values in sorted(flow.items())]
        funding = [{'time': int(row.event_time), 'funding_rate': _float(row.funding_rate), 'predicted_rate': _float(row.predicted_rate)} for row in funding_rows]

        step = max(1, (max(len(market), len(flow_data), len(funding)) + max_points - 1) // max_points)
        return {'range': range_key, 'anchor_time': anchor, 'market': market[::step], 'flow': flow_data[::step], 'funding_rate': funding[::step]}
    finally:
        if own_session:
            db.close()
