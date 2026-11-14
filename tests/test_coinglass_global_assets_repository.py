from coinx import config
from coinx.models import AlertRule, AlertState, MarketCoinglassGlobalAsset
from coinx.notifications import evaluate_coinglass_global_asset_rules
from coinx.repositories.coinglass_global_assets import (
    load_global_asset_history,
    load_global_assets_page,
    save_global_assets,
)


def _row(symbol, snapshot_time, **values):
    return {
        'symbol': symbol,
        'snapshot_time': snapshot_time,
        'oi_market_cap_ratio': values.get('oi_market_cap_ratio', None),
        'h1_oi_change': values.get('h1_oi_change', None),
        'h4_oi_change': values.get('h4_oi_change', None),
        'market_cap': values.get('market_cap', 1000),
        'open_interest': values.get('open_interest', 100),
        'price': values.get('price', 10),
        'h24_liquidation': values.get('h24_liquidation', 5),
        'h24_price_change': values.get('h24_price_change', 1),
        'h24_oi_change': values.get('h24_oi_change', 2),
    }


def test_save_and_load_global_asset_snapshot_page(db_session):
    save_global_assets([
        _row('BTC', 2000, market_cap=2000),
        _row('ETH', 2000, market_cap=1000),
        _row('BTC', 1000, market_cap=1500),
    ], session=db_session)

    result = load_global_assets_page(
        page=1,
        page_size=1,
        sort_by='market_cap',
        snapshot_time=2000,
        session=db_session,
    )

    assert result['total_count'] == 2
    assert result['data'][0]['symbol'] == 'BTC'
    assert result['stats']['market_cap'] == 3000
    assert result['snapshots'] == [2000, 1000]


def test_global_asset_history_is_latest_first(db_session):
    save_global_assets([_row('BTC', 1000), _row('BTC', 2000)], session=db_session)

    history = load_global_asset_history('btc', session=db_session)

    assert [item['snapshot_time'] for item in history] == [2000, 1000]


def test_global_asset_page_applies_strict_default_filter_values(db_session):
    save_global_assets([
        _row(
            'MATCH', 2000, market_cap=3_000_001, oi_market_cap_ratio=0.6001,
            h1_oi_change=5.1, h4_oi_change=10.1, h24_oi_change=20.1,
        ),
        _row(
            'BOUNDARY', 2000, market_cap=3_000_000, oi_market_cap_ratio=0.6,
            h1_oi_change=5, h4_oi_change=10, h24_oi_change=20,
        ),
    ], session=db_session)

    result = load_global_assets_page(
        snapshot_time=2000,
        sort_by='oi_market_cap_ratio',
        page_size=10,
        min_market_cap=3_000_000,
        min_oi_market_cap_ratio=0.6,
        min_h1_oi_change=5,
        min_h4_oi_change=10,
        min_h24_oi_change=20,
        session=db_session,
    )

    assert result['total_count'] == 1
    assert result['data'][0]['symbol'] == 'MATCH'


def test_global_asset_alert_reuses_alert_state_machine(db_session, monkeypatch):
    monkeypatch.setattr(config, 'NOTIFICATIONS_ENABLED', True)
    db_session.add(AlertRule(
        name='liquidation threshold',
        event_type='market.coinglass_global_asset.threshold',
        scope_type='global_assets',
        scope_json={'symbols': ['BTC']},
        params_json={
            'metric': 'h24_liquidation',
            'threshold': 100,
            'direction': 'positive',
            'recovery_confirmations': 1,
        },
        enabled=True,
    ))
    db_session.add(MarketCoinglassGlobalAsset(
        symbol='BTC', snapshot_time=2000, h24_liquidation=150,
    ))
    db_session.commit()
    rule = db_session.query(AlertRule).one()

    result = evaluate_coinglass_global_asset_rules(session=db_session, rule_id=rule.id)

    assert result['status'] == 'success'
    assert result['checked'] == 1
    assert result['matched'] == 1
    assert db_session.query(MarketCoinglassGlobalAsset).count() == 1
    assert db_session.query(AlertState).filter_by(rule_id=rule.id).one().state == 'triggered'
