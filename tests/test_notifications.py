from cryptography.fernet import Fernet

from flask import Flask
from sqlalchemy import event
import werkzeug

from coinx import config, notifications
from coinx.models import (
    AlertEvaluationMetric,
    AlertEvaluationRun,
    AlertRule,
    AlertRuleChannel,
    AlertState,
    MarketFundingRate,
    MarketKline,
    MarketTickers,
    NotificationChannel,
    NotificationDelivery,
)
from coinx.web.routes.api_notifications import api_notifications_bp
from coinx.web.routes.pages import pages_bp


def configure_notifications(monkeypatch):
    monkeypatch.setattr(config, 'NOTIFICATIONS_ENABLED', True)
    monkeypatch.setattr(config, 'NOTIFICATION_ENCRYPTION_KEY', Fernet.generate_key().decode())
    monkeypatch.setattr(config, 'NOTIFICATION_ENCRYPTION_KEY_VERSION', 'v1')
    monkeypatch.setattr(notifications, 'send_apprise', lambda *args, **kwargs: True)


def create_channel(db):
    channel = NotificationChannel(
        name='telegram',
        channel_type='apprise',
        config_encrypted=notifications.encrypt_apprise_url('json://example.test/coinx'),
        key_version='v1',
        enabled=True,
    )
    db.add(channel)
    db.flush()
    return channel


def create_rule(db, channel, event_type, scope_type, params, scope=None):
    rule = AlertRule(
        name=f'rule-{event_type}',
        event_type=event_type,
        scope_type=scope_type,
        scope_json=scope or {},
        params_json=params,
        cooldown_seconds=0,
        recovery_enabled=True,
        enabled=True,
    )
    db.add(rule)
    db.flush()
    db.add(AlertRuleChannel(rule_id=rule.id, channel_id=channel.id))
    db.commit()
    return rule


def test_channel_url_is_encrypted_and_not_deterministic(monkeypatch):
    configure_notifications(monkeypatch)
    first = notifications.encrypt_apprise_url('tgram://token/chat')
    second = notifications.encrypt_apprise_url('tgram://token/chat')

    assert first != second
    channel = type('Channel', (), {'config_encrypted': first})()
    assert notifications.decrypt_apprise_url(channel) == 'tgram://token/chat'


def test_notification_time_is_fixed_to_china_standard_time():
    assert notifications.format_notification_time(0) == '1970-01-01 08:00:00'


def test_funding_rate_alert_triggers_once_then_recovers(db_session, monkeypatch):
    configure_notifications(monkeypatch)
    sent_bodies = []
    monkeypatch.setattr(notifications, 'send_apprise', lambda _url, _title, body: sent_bodies.append(body) or True)
    channel = create_channel(db_session)
    create_rule(
        db_session, channel, notifications.EVENT_FUNDING_RATE, 'all_market',
        {'threshold': 0.001, 'direction': 'absolute', 'recovery_confirmations': 1},
    )
    rate = MarketFundingRate(symbol='BTCUSDT', period='5m', event_time=100, funding_rate=0.0012, exchange='binance')
    db_session.add(rate)
    db_session.commit()

    assert notifications.evaluate_funding_rate_rules(session=db_session)['sent'] == 1
    assert notifications.evaluate_funding_rate_rules(session=db_session)['sent'] == 0

    rate.funding_rate = 0.0002
    db_session.commit()
    assert notifications.evaluate_funding_rate_rules(session=db_session)['sent'] == 1
    deliveries = db_session.query(NotificationDelivery).order_by(NotificationDelivery.id.asc()).all()
    assert [delivery.event_status for delivery in deliveries] == ['summary', 'summary']
    assert all(delivery.delivery_status == 'success' for delivery in deliveries)
    assert all(delivery.payload_json['message']['title'] for delivery in deliveries)
    assert all('检查对象：1｜触发异常：1｜恢复正常：0' in body or '检查对象：1｜触发异常：0｜恢复正常：1' in body for body in sent_bodies)
    assert all('时间：' in body for body in sent_bodies)


def test_funding_rate_recovery_requires_configured_confirmations(db_session, monkeypatch):
    configure_notifications(monkeypatch)
    channel = create_channel(db_session)
    create_rule(
        db_session, channel, notifications.EVENT_FUNDING_RATE, 'all_market',
        {'threshold': 0.001, 'direction': 'absolute', 'recovery_confirmations': 3},
    )
    rate = MarketFundingRate(symbol='BTCUSDT', period='5m', event_time=100, funding_rate=0.0012, exchange='binance')
    db_session.add(rate)
    db_session.commit()

    assert notifications.evaluate_funding_rate_rules(session=db_session)['sent'] == 1
    rate.funding_rate = 0.0002
    db_session.commit()
    assert notifications.evaluate_funding_rate_rules(session=db_session)['sent'] == 0
    assert notifications.evaluate_funding_rate_rules(session=db_session)['sent'] == 0
    assert notifications.evaluate_funding_rate_rules(session=db_session)['sent'] == 1

    deliveries = db_session.query(NotificationDelivery).order_by(NotificationDelivery.id.asc()).all()
    assert [delivery.event_status for delivery in deliveries] == ['summary', 'summary']


def test_price_volume_only_evaluates_current_quote_volume_rank(db_session, monkeypatch):
    configure_notifications(monkeypatch)
    channel = create_channel(db_session)
    create_rule(
        db_session, channel, notifications.EVENT_PRICE_VOLUME, 'market_rank_top',
        {'period': '5m', 'price_change_threshold': 0.02, 'volume_ratio_threshold': 2, 'direction': 'up'},
        {'rank_type': 'quote_volume', 'limit': 1},
    )
    db_session.add_all([
        MarketTickers(symbol='BTCUSDT', quote_volume=1000, close_time=100),
        MarketTickers(symbol='ETHUSDT', quote_volume=100, close_time=100),
    ])
    for index in range(1, 4):
        db_session.add(MarketKline(
            exchange='binance', symbol='BTCUSDT', period='5m', open_time=index,
            close_time=index + 1, open_price=100, high_price=105, low_price=99,
            close_price=103 if index == 3 else 100, quote_volume=300 if index == 3 else 100,
        ))
        db_session.add(MarketKline(
            exchange='binance', symbol='ETHUSDT', period='5m', open_time=index,
            close_time=index + 1, open_price=100, high_price=105, low_price=99,
            close_price=110, quote_volume=1000,
        ))
    db_session.commit()

    result = notifications.evaluate_price_volume_rules(session=db_session)

    assert result['sent'] == 1
    delivery = db_session.query(NotificationDelivery).one()
    assert delivery.event_status == 'summary'
    assert 'BTCUSDT' in delivery.payload_json['message']['body']


def test_price_volume_batches_kline_loading_and_state_initialization(db_session, monkeypatch):
    configure_notifications(monkeypatch)
    channel = create_channel(db_session)
    rule = create_rule(
        db_session, channel, notifications.EVENT_PRICE_VOLUME, 'market_rank_top',
        {'period': '5m', 'price_change_threshold': 0.02, 'volume_ratio_threshold': 2, 'direction': 'absolute'},
        {'rank_type': 'quote_volume', 'limit': 3},
    )
    symbols = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT']
    for rank, symbol in enumerate(symbols, start=1):
        db_session.add(MarketTickers(symbol=symbol, quote_volume=1000 - rank, close_time=100))
        for index in range(1, 4):
            db_session.add(MarketKline(
                exchange='binance', symbol=symbol, period='5m', open_time=index,
                close_time=index + 1, open_price=100, high_price=105, low_price=99,
                close_price=100, quote_volume=100,
            ))
    db_session.commit()

    kline_selects = []

    def capture_kline_select(_conn, _cursor, statement, _params, _context, _executemany):
        if 'FROM market_klines' in statement:
            kline_selects.append(statement)

    event.listen(db_session.bind, 'before_cursor_execute', capture_kline_select)
    try:
        result = notifications.evaluate_price_volume_rules(session=db_session, rule_id=rule.id)
    finally:
        event.remove(db_session.bind, 'before_cursor_execute', capture_kline_select)

    assert result['status'] == 'success'
    assert len(kline_selects) == 1
    assert result['metrics']['symbols'] == 3
    assert result['metrics']['kline_rows'] == 9
    assert db_session.query(AlertState).filter_by(rule_id=rule.id).count() == 3


def test_observation_skips_cas_when_state_does_not_advance(db_session, monkeypatch):
    configure_notifications(monkeypatch)
    channel = create_channel(db_session)
    rule = create_rule(
        db_session, channel, notifications.EVENT_FUNDING_RATE, 'all_market',
        {'threshold': 0.001, 'direction': 'absolute'},
    )
    state = notifications._load_rule_states(db_session, rule.id, 'absolute', ['BTCUSDT'])['BTCUSDT']
    updates = []

    def capture_state_update(_conn, _cursor, statement, _params, _context, _executemany):
        if statement.lstrip().upper().startswith('UPDATE ALERT_STATES'):
            updates.append(statement)

    event.listen(db_session.bind, 'before_cursor_execute', capture_state_update)
    try:
        result = notifications._observe(
            db_session, rule, 'BTCUSDT', 'absolute', False, {'funding_rate': 0.0},
            'unused', 'unused', state=state, aggregate=True,
        )
    finally:
        event.remove(db_session.bind, 'before_cursor_execute', capture_state_update)

    assert result == {'event_status': None, 'sent': 0}
    assert updates == []

    notifications._observe(
        db_session, rule, 'BTCUSDT', 'absolute', True, {'funding_rate': 0.01},
        'unused', 'unused', state=state, aggregate=True,
    )
    db_session.expire_all()
    triggered_state = db_session.query(AlertState).filter_by(rule_id=rule.id, subject_key='BTCUSDT').one()
    updates = []
    event.listen(db_session.bind, 'before_cursor_execute', capture_state_update)
    try:
        result = notifications._observe(
            db_session, rule, 'BTCUSDT', 'absolute', True, {'funding_rate': 0.02},
            'unused', 'unused', state=triggered_state, aggregate=True,
        )
    finally:
        event.remove(db_session.bind, 'before_cursor_execute', capture_state_update)

    assert result == {'event_status': None, 'sent': 0}
    assert updates == []


def test_job_failure_requires_configured_consecutive_failures(db_session, monkeypatch):
    configure_notifications(monkeypatch)
    channel = create_channel(db_session)
    create_rule(
        db_session, channel, notifications.EVENT_JOB_FAILURE, 'system_jobs',
        {'job_ids': ['repair_market_rolling_job', 'collect_funding_rates_job'], 'consecutive_failures': 2},
    )
    metadata = {
        'repair_market_rolling_job': {'last_status': 'error', 'last_error': 'network'},
        'collect_funding_rates_job': {'last_status': 'error', 'last_error': 'timeout'},
    }

    assert notifications.evaluate_job_failure_rules(metadata, session=db_session)['sent'] == 0
    assert notifications.evaluate_job_failure_rules(metadata, session=db_session)['sent'] == 1
    assert notifications.evaluate_job_failure_rules({
        'repair_market_rolling_job': {'last_status': 'success'},
        'collect_funding_rates_job': {'last_status': 'success'},
    }, session=db_session)['sent'] == 1
    deliveries = db_session.query(NotificationDelivery).order_by(NotificationDelivery.id).all()
    assert [item.event_status for item in deliveries] == ['summary', 'summary']
    assert 'repair_market_rolling_job' in deliveries[0].payload_json['message']['body']
    assert 'collect_funding_rates_job' in deliveries[0].payload_json['message']['body']


def test_channel_api_never_returns_url_and_rule_can_select_channel(db_session, monkeypatch):
    configure_notifications(monkeypatch)
    monkeypatch.setattr('coinx.web.routes.api_notifications.get_session', lambda: db_session)
    if not hasattr(werkzeug, '__version__'):
        werkzeug.__version__ = '3'
    app = Flask(__name__)
    app.register_blueprint(api_notifications_bp)
    client = app.test_client()

    channel_response = client.post('/api/notification-channels', json={
        'name': 'ops', 'url': 'json://example.test/coinx', 'enabled': True,
    })
    assert channel_response.status_code == 201
    assert 'example.test' not in channel_response.get_data(as_text=True)
    channel = channel_response.get_json()['data']
    assert channel['configured'] is True
    assert channel['apprise_type'] == 'JSON Webhook'

    update_response = client.patch(f"/api/notification-channels/{channel['id']}", json={
        'name': 'ops', 'url': 'json://example.test/updated', 'enabled': True,
    })
    assert update_response.status_code == 200
    assert 'example.test' not in update_response.get_data(as_text=True)
    stored_channel = db_session.get(NotificationChannel, channel['id'])
    assert notifications.decrypt_apprise_url(stored_channel) == 'json://example.test/updated'

    rule_response = client.post('/api/alert-rules', json={
        'name': 'all funding',
        'event_type': notifications.EVENT_FUNDING_RATE,
        'scope_type': 'all_market',
        'scope': {},
        'params': {'threshold': 0.001, 'direction': 'absolute'},
        'channel_ids': [channel['id']],
        'enabled': False,
    })
    assert rule_response.status_code == 201
    rule = rule_response.get_json()['data']
    assert rule['channel_ids'] == [channel['id']]
    assert rule['params']['recovery_confirmations'] == 3
    assert db_session.query(AlertRuleChannel).filter_by(rule_id=rule['id'], channel_id=channel['id']).one()

    db_session.add(NotificationDelivery(
        rule_id=rule['id'], channel_id=channel['id'], event_key='rule:test',
        event_status='summary', delivery_status='success', sent_at=123,
        payload_json={'message': {'title': '资金费率异常', 'body': 'BTCUSDT 资金费率 1.00%\n\n时间：2026-07-23 18:00:00'}},
    ))
    db_session.commit()
    deliveries_response = client.get(f"/api/alert-rules/{rule['id']}/deliveries")
    delivery = deliveries_response.get_json()['data']['items'][0]
    assert delivery['message']['title'] == '资金费率异常'
    assert 'BTCUSDT' in delivery['message']['body']


def test_channel_test_api_returns_delivery_error(db_session, monkeypatch):
    configure_notifications(monkeypatch)
    monkeypatch.setattr('coinx.web.routes.api_notifications.get_session', lambda: db_session)
    channel = create_channel(db_session)

    def fail_delivery(*_args, **_kwargs):
        raise RuntimeError('Telegram request timed out')

    monkeypatch.setattr(notifications, 'send_apprise', fail_delivery)
    app = Flask(__name__)
    app.register_blueprint(api_notifications_bp)

    response = app.test_client().post(f'/api/notification-channels/{channel.id}/test')

    assert response.status_code == 502
    payload = response.get_json()
    assert payload['message'] == '测试发送失败：Telegram request timed out'
    assert payload['data']['error_message'] == 'Telegram request timed out'


def test_rule_parameter_change_resets_states_without_recovery(db_session, monkeypatch):
    configure_notifications(monkeypatch)
    monkeypatch.setattr('coinx.web.routes.api_notifications.get_session', lambda: db_session)
    if not hasattr(werkzeug, '__version__'):
        werkzeug.__version__ = '3'
    channel = create_channel(db_session)
    rule = create_rule(
        db_session, channel, notifications.EVENT_PRICE_VOLUME, 'market_rank_top',
        {'period': '5m', 'price_change_threshold': 0.01, 'volume_ratio_threshold': 2, 'direction': 'absolute'},
        {'rank_type': 'quote_volume', 'limit': 100},
    )
    db_session.add(AlertState(
        rule_id=rule.id, subject_key='BTCUSDT', dimension_key='absolute',
        state='triggered', consecutive_matches=1,
    ))
    db_session.commit()
    app = Flask(__name__)
    app.register_blueprint(api_notifications_bp)

    response = app.test_client().patch(f'/api/alert-rules/{rule.id}', json={
        'params': {'period': '5m', 'price_change_threshold': 0.05, 'volume_ratio_threshold': 2, 'direction': 'absolute'},
    })

    assert response.status_code == 200
    assert response.get_json()['meta']['state_reset'] is True
    assert db_session.query(AlertState).filter_by(rule_id=rule.id).count() == 0
    assert db_session.query(NotificationDelivery).filter_by(rule_id=rule.id).count() == 0


def test_manual_evaluation_creates_a_visible_run_record(db_session, monkeypatch):
    configure_notifications(monkeypatch)
    monkeypatch.setattr('coinx.web.routes.api_notifications.get_session', lambda: db_session)
    if not hasattr(werkzeug, '__version__'):
        werkzeug.__version__ = '3'
    channel = create_channel(db_session)
    rule = create_rule(
        db_session, channel, notifications.EVENT_FUNDING_RATE, 'all_market',
        {'threshold': 0.001, 'direction': 'absolute'},
    )
    rule_id = rule.id
    app = Flask(__name__)
    app.register_blueprint(api_notifications_bp)
    client = app.test_client()

    response = client.post(f'/api/alert-rules/{rule_id}/evaluate')

    assert response.status_code == 200
    assert response.get_json()['data']['checked'] == 0
    runs_response = client.get('/api/alert-evaluation-runs')
    run = runs_response.get_json()['data'][0]
    assert run['rule_id'] == rule_id
    assert run['status'] == 'success'
    assert run['checked_count'] == 0
    assert run['completed_at'] is not None
    assert run['duration_ms'] is not None
    logs_response = client.get(f"/api/alert-evaluation-runs/{run['id']}/logs")
    logs = logs_response.get_json()['data']['logs']
    assert logs == sorted(logs, key=lambda item: item['timestamp'], reverse=True)
    assert 'evaluation completed' in logs[0]['message']
    assert any(item['message'].startswith('manual evaluation started') for item in logs)
    assert any(item['message'].startswith('evaluation timing: total=') for item in logs)
    metric = db_session.query(AlertEvaluationMetric).filter_by(run_id=run['id']).one()
    assert metric.metrics_json['duration_ms'] >= 0

    rule_runs_response = client.get(f'/api/alert-rules/{rule_id}/evaluation-runs')
    assert rule_runs_response.get_json()['data']['items'][0]['rule_id'] == rule_id


def test_scheduled_evaluation_creates_a_visible_run_record(db_session, monkeypatch):
    configure_notifications(monkeypatch)
    monkeypatch.setattr(notifications, 'get_session', lambda: db_session)
    channel = create_channel(db_session)
    rule = create_rule(
        db_session, channel, notifications.EVENT_FUNDING_RATE, 'all_market',
        {'threshold': 0.001, 'direction': 'absolute'},
    )
    rule_id = rule.id

    result = notifications.evaluate_scheduled_rules(notifications.EVENT_FUNDING_RATE)

    assert result['status'] == 'success'
    run = db_session.query(AlertEvaluationRun).filter_by(rule_id=rule_id).one()
    assert run.trigger_source == 'scheduled'
    assert run.status == 'success'
    assert run.completed_at is not None
    metric = db_session.query(AlertEvaluationMetric).filter_by(run_id=run.id).one()
    assert metric.metrics_json['duration_ms'] >= 0


def test_stale_scheduled_evaluation_is_finalized(db_session, monkeypatch):
    monkeypatch.setattr('coinx.web.routes.api_notifications.get_session', lambda: db_session)
    channel = create_channel(db_session)
    rule = create_rule(
        db_session, channel, notifications.EVENT_FUNDING_RATE, 'all_market',
        {'threshold': 0.001, 'direction': 'absolute'},
    )
    run = AlertEvaluationRun(
        rule_id=rule.id,
        trigger_source='scheduled',
        status='running',
        started_at=notifications.now_ms() - 5 * 60 * 1000 - 1,
    )
    db_session.add(run)
    db_session.commit()
    if not hasattr(werkzeug, '__version__'):
        werkzeug.__version__ = '3'
    app = Flask(__name__)
    app.register_blueprint(api_notifications_bp)

    response = app.test_client().get(f'/api/alert-rules/{rule.id}/evaluation-runs')

    assert response.status_code == 200
    item = response.get_json()['data']['items'][0]
    assert item['status'] == 'error'
    assert '5-minute timeout' in item['error_message']
    assert item['completed_at'] is not None


def test_rule_state_details_default_to_all_states(db_session, monkeypatch):
    monkeypatch.setattr('coinx.web.routes.api_notifications.get_session', lambda: db_session)
    channel = create_channel(db_session)
    rule = create_rule(
        db_session, channel, notifications.EVENT_FUNDING_RATE, 'all_market',
        {'threshold': 0.001, 'direction': 'absolute'},
    )
    db_session.add_all([
        AlertState(rule_id=rule.id, subject_key='BTCUSDT', dimension_key='absolute', state='normal'),
        AlertState(rule_id=rule.id, subject_key='ETHUSDT', dimension_key='absolute', state='triggered'),
    ])
    db_session.commit()
    app = Flask(__name__)
    app.register_blueprint(api_notifications_bp)
    client = app.test_client()

    all_states = client.get(f'/api/alert-rules/{rule.id}/states?limit=5').get_json()['data']
    triggered_states = client.get(f'/api/alert-rules/{rule.id}/states?limit=5&status=triggered').get_json()['data']

    assert all_states['total'] == 2
    assert {item['state'] for item in all_states['items']} == {'normal', 'triggered'}
    assert triggered_states['total'] == 1
    assert triggered_states['items'][0]['subject_key'] == 'ETHUSDT'


def test_evaluation_is_skipped_when_rule_lease_is_held(db_session, monkeypatch):
    configure_notifications(monkeypatch)
    channel = create_channel(db_session)
    rule = create_rule(
        db_session, channel, notifications.EVENT_FUNDING_RATE, 'all_market',
        {'threshold': 0.001, 'direction': 'absolute'},
    )
    monkeypatch.setattr(notifications, 'acquire_evaluation_run_lease', lambda *_args: (True, None))
    monkeypatch.setattr(notifications, '_acquire_evaluation_lease', lambda *_args: (False, None))

    result = notifications.evaluate_rule_with_run(rule, 'scheduled', session=db_session)

    assert result['status'] == 'skipped'
    run = db_session.query(AlertEvaluationRun).filter_by(rule_id=rule.id).one()
    assert run.status == 'skipped'
    assert run.completed_at is not None


def test_notification_management_page_renders():
    if not hasattr(werkzeug, '__version__'):
        werkzeug.__version__ = '3'
    app = Flask(__name__, template_folder='../src/coinx/web/templates')
    app.register_blueprint(pages_bp)

    response = app.test_client().get('/notification-management')

    assert response.status_code == 200
    assert '告警管理' in response.get_data(as_text=True)
