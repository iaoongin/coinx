"""Configurable alert evaluation and Apprise delivery.

All evaluators operate on data already stored by CoinX jobs.  Notification
failures are recorded but never allowed to fail a collection or repair job.
"""
import time
from collections import defaultdict
from datetime import datetime

from sqlalchemy import text, update
from sqlalchemy.exc import IntegrityError

from cryptography.fernet import Fernet, InvalidToken
from coinx import config
from coinx.database import get_session
from coinx.models import (
    AlertEvaluationMetric,
    AlertEvaluationRun,
    AlertRule,
    AlertRuleChannel,
    AlertState,
    NotificationChannel,
    NotificationDelivery,
)
from coinx.repositories.funding_rate import load_latest_funding_rates
from coinx.utils import logger


EVENT_FUNDING_RATE = 'market.funding_rate.threshold'
EVENT_PRICE_VOLUME = 'market.price_volume.threshold'
EVENT_JOB_FAILURE = 'system.job.failure'

EVENT_SCOPE = {
    EVENT_FUNDING_RATE: 'all_market',
    EVENT_PRICE_VOLUME: 'market_rank_top',
    EVENT_JOB_FAILURE: 'system_jobs',
}


class NotificationConfigError(ValueError):
    """Raised for invalid notification configuration without exposing secrets."""


def now_ms():
    return int(time.time() * 1000)


def format_notification_time(timestamp=None):
    """Format the delivery time in the application host's local timezone."""
    moment = datetime.fromtimestamp((timestamp or now_ms()) / 1000).astimezone()
    return moment.strftime('%Y-%m-%d %H:%M:%S')


def get_fernet():
    key = config.NOTIFICATION_ENCRYPTION_KEY
    if not key:
        raise NotificationConfigError('NOTIFICATION_ENCRYPTION_KEY is not configured')
    try:
        return Fernet(key.encode('utf-8') if isinstance(key, str) else key)
    except (TypeError, ValueError) as exc:
        raise NotificationConfigError('NOTIFICATION_ENCRYPTION_KEY is invalid') from exc


def encrypt_apprise_url(url):
    normalized = (url or '').strip()
    if '://' not in normalized:
        raise NotificationConfigError('invalid Apprise URL')
    return get_fernet().encrypt(normalized.encode('utf-8')).decode('utf-8')


def decrypt_apprise_url(channel):
    try:
        return get_fernet().decrypt(channel.config_encrypted.encode('utf-8')).decode('utf-8')
    except (InvalidToken, UnicodeDecodeError) as exc:
        raise NotificationConfigError('unable to decrypt notification channel') from exc


def serialize_channel(channel):
    return {
        'id': channel.id,
        'name': channel.name,
        'channel_type': channel.channel_type,
        'enabled': bool(channel.enabled),
        'configured': bool(channel.config_encrypted),
        'key_version': channel.key_version,
        'created_at': channel.created_at.isoformat() if channel.created_at else None,
        'updated_at': channel.updated_at.isoformat() if channel.updated_at else None,
    }


def serialize_rule(rule, channel_ids=None):
    return {
        'id': rule.id,
        'name': rule.name,
        'event_type': rule.event_type,
        'scope_type': rule.scope_type,
        'scope': rule.scope_json or {},
        'params': rule.params_json or {},
        'cooldown_seconds': rule.cooldown_seconds,
        'recovery_enabled': bool(rule.recovery_enabled),
        'enabled': bool(rule.enabled),
        'channel_ids': channel_ids if channel_ids is not None else [],
        'created_at': rule.created_at.isoformat() if rule.created_at else None,
        'updated_at': rule.updated_at.isoformat() if rule.updated_at else None,
    }


def validate_rule_payload(payload):
    payload = payload or {}
    event_type = (payload.get('event_type') or '').strip()
    expected_scope = EVENT_SCOPE.get(event_type)
    if not expected_scope:
        raise NotificationConfigError('unsupported event_type')
    scope_type = (payload.get('scope_type') or '').strip()
    if scope_type != expected_scope:
        raise NotificationConfigError('scope_type does not match event_type')

    name = (payload.get('name') or '').strip()
    if not name or len(name) > 120:
        raise NotificationConfigError('invalid rule name')
    params = payload.get('params') or {}
    if not isinstance(params, dict):
        raise NotificationConfigError('params must be an object')
    scope = payload.get('scope') or {}
    if not isinstance(scope, dict):
        raise NotificationConfigError('scope must be an object')
    try:
        cooldown_seconds = int(payload.get('cooldown_seconds', 1800))
    except (TypeError, ValueError) as exc:
        raise NotificationConfigError('invalid cooldown_seconds') from exc
    if cooldown_seconds < 0 or cooldown_seconds > 7 * 24 * 3600:
        raise NotificationConfigError('invalid cooldown_seconds')

    if event_type == EVENT_FUNDING_RATE:
        threshold = _positive_number(params.get('threshold'), 'threshold')
        direction = params.get('direction', 'absolute')
        if direction not in {'positive', 'negative', 'absolute'}:
            raise NotificationConfigError('invalid direction')
        try:
            recovery_confirmations = int(params.get('recovery_confirmations', 3))
        except (TypeError, ValueError) as exc:
            raise NotificationConfigError('invalid recovery_confirmations') from exc
        if recovery_confirmations < 1 or recovery_confirmations > 12:
            raise NotificationConfigError('invalid recovery_confirmations')
        params = {
            'threshold': threshold,
            'direction': direction,
            'recovery_confirmations': recovery_confirmations,
        }
        scope = {}
    elif event_type == EVENT_PRICE_VOLUME:
        if params.get('period', '5m') != '5m':
            raise NotificationConfigError('only 5m period is supported')
        _positive_number(params.get('price_change_threshold'), 'price_change_threshold')
        _positive_number(params.get('volume_ratio_threshold'), 'volume_ratio_threshold')
        if params.get('direction', 'absolute') not in {'up', 'down', 'absolute'}:
            raise NotificationConfigError('invalid direction')
        rank_type = scope.get('rank_type', 'quote_volume')
        try:
            limit = int(scope.get('limit', config.FETCH_COINS_TOP_VOLUME_COUNT))
        except (TypeError, ValueError) as exc:
            raise NotificationConfigError('invalid rank limit') from exc
        if rank_type != 'quote_volume' or limit < 1 or limit > 500:
            raise NotificationConfigError('invalid market rank scope')
        scope = {'rank_type': rank_type, 'limit': limit}
    else:
        job_ids = params.get('job_ids') or []
        if not isinstance(job_ids, list) or not all(isinstance(item, str) and item.strip() for item in job_ids):
            raise NotificationConfigError('job_ids must be a non-empty string list')
        try:
            failures = int(params.get('consecutive_failures', 1))
        except (TypeError, ValueError) as exc:
            raise NotificationConfigError('invalid consecutive_failures') from exc
        if failures < 1 or failures > 100:
            raise NotificationConfigError('invalid consecutive_failures')
        params = {'job_ids': [item.strip() for item in job_ids], 'consecutive_failures': failures}
        scope = {}

    return {
        'name': name,
        'event_type': event_type,
        'scope_type': scope_type,
        'scope_json': scope,
        'params_json': params,
        'cooldown_seconds': cooldown_seconds,
        'recovery_enabled': bool(payload.get('recovery_enabled', True)),
        'enabled': bool(payload.get('enabled', False)),
    }


def _positive_number(value, field):
    try:
        value = float(value)
    except (TypeError, ValueError) as exc:
        raise NotificationConfigError(f'invalid {field}') from exc
    if value <= 0:
        raise NotificationConfigError(f'invalid {field}')
    return value


def set_rule_channels(db, rule, channel_ids):
    channel_ids = channel_ids or []
    if not isinstance(channel_ids, list) or not channel_ids:
        raise NotificationConfigError('at least one channel is required')
    try:
        normalized = sorted({int(channel_id) for channel_id in channel_ids})
    except (TypeError, ValueError) as exc:
        raise NotificationConfigError('invalid channel_ids') from exc
    channels = db.query(NotificationChannel).filter(NotificationChannel.id.in_(normalized)).all()
    if len(channels) != len(normalized) or any(not channel.enabled for channel in channels):
        raise NotificationConfigError('channels must exist and be enabled')
    db.query(AlertRuleChannel).filter(AlertRuleChannel.rule_id == rule.id).delete()
    db.add_all([AlertRuleChannel(rule_id=rule.id, channel_id=channel_id) for channel_id in normalized])


def get_rule_channel_ids(db, rule_id):
    return [row.channel_id for row in db.query(AlertRuleChannel).filter(AlertRuleChannel.rule_id == rule_id).all()]


def _rule_channels(db, rule_id):
    return db.query(NotificationChannel).join(
        AlertRuleChannel, AlertRuleChannel.channel_id == NotificationChannel.id
    ).filter(
        AlertRuleChannel.rule_id == rule_id,
        NotificationChannel.enabled.is_(True),
    ).all()


def send_apprise(url, title, body):
    try:
        import apprise
    except ImportError as exc:
        raise RuntimeError('apprise is not installed') from exc
    notifier = apprise.Apprise()
    if not notifier.add(url):
        raise NotificationConfigError('invalid Apprise URL')
    if not notifier.notify(title=title, body=body, notify_type=apprise.NotifyType.INFO):
        raise RuntimeError('Apprise delivery failed')
    return True


def _delivery(db, rule, channel, event_key, event_status, payload, title, body):
    timestamp = now_ms()
    body_with_time = f'{body}\n\n时间：{format_notification_time(timestamp)}'
    payload = dict(payload or {})
    # Keep the exact rendered content so delivery history can be audited later.
    payload['message'] = {'title': title, 'body': body_with_time}
    try:
        send_apprise(decrypt_apprise_url(channel), title, body_with_time)
        delivery = NotificationDelivery(
            rule_id=rule.id if rule else None,
            channel_id=channel.id,
            event_key=event_key,
            event_status=event_status,
            payload_json=payload,
            delivery_status='success',
            sent_at=timestamp,
        )
    except Exception as exc:  # A notification must never abort market collection.
        logger.warning('通知发送失败: channel=%s event=%s error=%s', channel.id, event_key, str(exc))
        delivery = NotificationDelivery(
            rule_id=rule.id if rule else None,
            channel_id=channel.id,
            event_key=event_key,
            event_status=event_status,
            payload_json=payload,
            delivery_status='failed',
            error_message=str(exc)[:500],
            sent_at=timestamp,
        )
    db.add(delivery)
    return delivery


def test_channel(db, channel):
    payload = {'kind': 'channel_test', 'channel_name': channel.name}
    return _delivery(db, None, channel, f'channel:{channel.id}|test', 'test', payload, 'CoinX 渠道测试', 'CoinX 通知渠道配置测试成功。')


def _insert_alert_states_ignore_conflicts(db, records):
    """Create missing alert states in one statement without overwriting a peer."""
    if not records:
        return
    dialect = db.get_bind().dialect.name
    if dialect == 'mysql':
        from sqlalchemy.dialects.mysql import insert
        statement = insert(AlertState).values(records).prefix_with('IGNORE')
    elif dialect == 'sqlite':
        from sqlalchemy.dialects.sqlite import insert
        statement = insert(AlertState).values(records).on_conflict_do_nothing(
            index_elements=('rule_id', 'subject_key', 'dimension_key'),
        )
    elif dialect == 'postgresql':
        from sqlalchemy.dialects.postgresql import insert
        statement = insert(AlertState).values(records).on_conflict_do_nothing(
            index_elements=('rule_id', 'subject_key', 'dimension_key'),
        )
    else:
        for record in records:
            try:
                with db.begin_nested():
                    db.add(AlertState(**record))
                    db.flush()
            except IntegrityError:
                pass
        return
    db.execute(statement)


def _load_rule_states(db, rule_id, dimension_key, subject_keys):
    """Batch-create and load state rows for an evaluation scope."""
    keys = list(dict.fromkeys(subject_keys))
    if not keys:
        return {}
    timestamp = datetime.now()
    _insert_alert_states_ignore_conflicts(db, [{
        'rule_id': rule_id,
        'subject_key': key,
        'dimension_key': dimension_key,
        'state': 'normal',
        'consecutive_matches': 0,
        'updated_at': timestamp,
    } for key in keys])
    rows = db.query(AlertState).filter(
        AlertState.rule_id == rule_id,
        AlertState.dimension_key == dimension_key,
        AlertState.subject_key.in_(keys),
    ).all()
    return {row.subject_key: row for row in rows}


def _cas_update_alert_state(db, state, values):
    """Compare-and-swap on the observed target state only."""
    criteria = [
        AlertState.id == state.id,
        AlertState.state == state.state,
    ]
    result = db.execute(update(AlertState).where(*criteria).values(**values))
    if result.rowcount:
        return True
    db.expire(state)
    db.refresh(state)
    return False


def _observe(
    db, rule, subject_key, dimension_key, matched, values, title, summary,
    consecutive_matches=None, recovery_confirmations=1, state=None, aggregate=False,
):
    """Apply the normal/triggered/recovered state machine for one observation."""
    timestamp = now_ms()
    if state is None:
        state = _load_rule_states(db, rule.id, dimension_key, [subject_key]).get(subject_key)
    if state is None:
        raise RuntimeError(f'unable to initialize alert state: {rule.id}/{subject_key}/{dimension_key}')

    # A peer evaluator may update this subject between loading the scope and
    # committing it. Rebuild the state transition from the latest row on CAS loss.
    for _attempt in range(3):
        previous = state.state
        previous_values = state.last_value_json or {}
        consecutive_value = (
            consecutive_matches if consecutive_matches is not None
            else (int(state.consecutive_matches or 0) + 1 if matched else 0)
        )
        recovery_count = int(previous_values.get('_recovery_count', 0))

        # Most periodic observations do not advance the state machine. Avoid
        # a CAS write for normal observations and sustained triggered states.
        if (
            not matched
            and previous != 'triggered'
            and consecutive_value == int(state.consecutive_matches or 0)
        ):
            return {'event_status': None, 'sent': 0}
        if matched and previous == 'triggered' and recovery_count == 0:
            return {'event_status': None, 'sent': 0}

        observed_values = dict(values)
        next_state = previous
        next_triggered_at = state.last_triggered_at
        next_recovered_at = state.last_recovered_at
        event_status = None
        if matched:
            observed_values['_recovery_count'] = 0
            observed_values['_last_triggered_values'] = dict(observed_values)
            if previous != 'triggered':
                next_state = 'triggered'
                next_triggered_at = timestamp
                event_status = 'triggered'
        elif previous == 'triggered':
            recovery_count += 1
            observed_values['_recovery_count'] = recovery_count
            last_triggered_values = previous_values.get('_last_triggered_values', previous_values)
            observed_values['_last_triggered_values'] = last_triggered_values
            if recovery_count >= recovery_confirmations:
                next_state = 'normal'
                next_recovered_at = timestamp
                event_status = 'recovered'
                previous_values = last_triggered_values
                title = f'{title}已恢复'
                summary = f'{summary}；当前已恢复至规则阈值内。'
        else:
            observed_values['_recovery_count'] = 0

        if _cas_update_alert_state(db, state, {
            'state': next_state,
            'consecutive_matches': consecutive_value,
            'last_value_json': observed_values,
            'last_triggered_at': next_triggered_at,
            'last_recovered_at': next_recovered_at,
        }):
            values = observed_values
            break
    else:
        raise RuntimeError(f'alert state CAS conflict: {rule.id}/{subject_key}/{dimension_key}')

    if not event_status or (event_status == 'recovered' and not rule.recovery_enabled):
        return {'event_status': event_status, 'sent': 0}
    if event_status == 'triggered' and state.last_notified_at and (
        timestamp - state.last_notified_at < rule.cooldown_seconds * 1000
    ):
        return {'event_status': event_status, 'sent': 0}

    if aggregate:
        return {'event_status': event_status, 'sent': 0, 'state': state, 'previous_values': previous_values}

    event_key = f'rule:{rule.id}|subject:{subject_key}|dimension:{dimension_key}'
    payload = {
        'event_key': event_key,
        'event_type': rule.event_type,
        'status': event_status,
        'occurred_at': timestamp,
        'title': title,
        'summary': summary,
        'data': values,
    }
    deliveries = [_delivery(db, rule, channel, event_key, event_status, payload, title, summary) for channel in _rule_channels(db, rule.id)]
    if deliveries:
        db.execute(update(AlertState).where(AlertState.id == state.id).values(
            last_notified_at=timestamp,
            updated_at=datetime.now(),
        ))
    return {'event_status': event_status, 'sent': len(deliveries)}


def _enabled_rules(db, event_type, rule_id=None):
    query = db.query(AlertRule).filter(
        AlertRule.event_type == event_type,
        AlertRule.enabled.is_(True),
    )
    if rule_id is not None:
        query = query.filter(AlertRule.id == rule_id)
    return query.all()


def _deliver_evaluation_summary(db, rule, checked, events, condition):
    """Send one consistently formatted summary per rule evaluation and channel."""
    if not events:
        return 0
    triggered = [event['triggered'] for event in events if event['status'] == 'triggered']
    recovered = [event['recovered'] for event in events if event['status'] == 'recovered']
    sections = [
        f'本次评估完成\n检查对象：{checked}\n触发异常：{len(triggered)}\n恢复正常：{len(recovered)}',
    ]
    if triggered:
        sections.append('触发异常\n' + '\n'.join(triggered))
    if recovered:
        sections.append('恢复正常\n' + '\n'.join(recovered))
    sections.append(f'规则条件：{condition}')

    timestamp = now_ms()
    payload = {
        'event_type': rule.event_type,
        'status': 'summary',
        'occurred_at': timestamp,
        'checked': checked,
        'triggered': len(triggered),
        'recovered': len(recovered),
    }
    deliveries = [
        _delivery(
            db, rule, channel, f'rule:{rule.id}|evaluation:{timestamp}', 'summary', payload,
            f'CoinX · {rule.name}', '\n\n'.join(sections),
        )
        for channel in _rule_channels(db, rule.id)
    ]
    if deliveries:
        state_ids = [event['state'].id for event in events if event.get('state') is not None]
        if state_ids:
            db.execute(update(AlertState).where(AlertState.id.in_(state_ids)).values(
                last_notified_at=timestamp,
                updated_at=datetime.now(),
            ))
    return len(deliveries)


def _evaluation_metrics(started_at, stages, **extra):
    total_ms = round((time.perf_counter() - started_at) * 1000, 2)
    return {
        'duration_ms': total_ms,
        'stage_ms': {name: round(value, 2) for name, value in stages.items()},
        **extra,
    }


PRICE_VOLUME_LOOKBACK_MS = 26 * 60 * 60 * 1000


def _load_price_volume_metrics(db, scope_limit):
    """Calculate the per-symbol price and volume metrics in the database.

    The 26-hour range bounds each 5-minute series to roughly 312 rows before
    the window function keeps the latest 289 observations.
    """
    rows = db.execute(text("""
        WITH latest_snapshot AS (
          SELECT MAX(close_time) AS snapshot_time FROM market_tickers
        ), top_symbols AS (
          SELECT mt.symbol, latest_snapshot.snapshot_time
          FROM market_tickers AS mt
          JOIN latest_snapshot ON mt.close_time = latest_snapshot.snapshot_time
          ORDER BY mt.quote_volume DESC
          LIMIT :scope_limit
        ), ranked_klines AS (
          SELECT
            k.symbol, k.open_time, k.open_price, k.close_price, k.quote_volume,
            ROW_NUMBER() OVER (
              PARTITION BY k.symbol ORDER BY k.open_time DESC
            ) AS rn
          FROM market_klines AS k
          JOIN top_symbols AS s ON s.symbol = k.symbol
          WHERE k.exchange = 'binance'
            AND k.period = '5m'
            AND k.open_time >= s.snapshot_time - :lookback_ms
        ), metrics AS (
          SELECT
            symbol,
            MAX(CASE WHEN rn = 1 THEN open_time END) AS open_time,
            1.0 * (MAX(CASE WHEN rn = 1 THEN close_price END)
              - MAX(CASE WHEN rn = 1 THEN open_price END))
              / NULLIF(MAX(CASE WHEN rn = 1 THEN open_price END), 0) AS price_change,
            MAX(CASE WHEN rn = 1 THEN quote_volume END)
              / NULLIF(AVG(CASE WHEN rn BETWEEN 2 AND 289 THEN quote_volume END), 0)
              AS volume_ratio,
            COUNT(*) AS kline_count,
            SUM(CASE WHEN rn BETWEEN 2 AND 289 AND quote_volume IS NOT NULL
              THEN 1 ELSE 0 END) AS historical_volume_count
          FROM ranked_klines
          WHERE rn <= 289
          GROUP BY symbol
        )
        SELECT
          s.symbol,
          m.open_time,
          m.price_change,
          m.volume_ratio,
          COALESCE(m.kline_count, 0) AS kline_count,
          COALESCE(m.historical_volume_count, 0) AS historical_volume_count
        FROM top_symbols AS s
        LEFT JOIN metrics AS m ON m.symbol = s.symbol
        ORDER BY s.symbol
    """), {
        'scope_limit': int(scope_limit),
        'lookback_ms': PRICE_VOLUME_LOOKBACK_MS,
    }).mappings().all()
    return {
        row['symbol']: {
            'open_time': row['open_time'],
            'price_change': float(row['price_change']) if row['price_change'] is not None else None,
            'volume_ratio': float(row['volume_ratio']) if row['volume_ratio'] is not None else None,
            'kline_count': int(row['kline_count'] or 0),
            'historical_volume_count': int(row['historical_volume_count'] or 0),
        }
        for row in rows
    }


def evaluate_funding_rate_rules(session=None, rule_id=None):
    if not config.NOTIFICATIONS_ENABLED:
        return {'status': 'disabled', 'evaluated': 0, 'checked': 0, 'matched': 0, 'sent': 0}
    own_session = session is None
    db = session or get_session()
    started_at = time.perf_counter()
    stages = defaultdict(float)
    try:
        rules = _enabled_rules(db, EVENT_FUNDING_RATE, rule_id=rule_id)
        if not rules:
            return {
                'status': 'success', 'evaluated': 0, 'checked': 0, 'matched': 0, 'sent': 0,
                'metrics': _evaluation_metrics(started_at, stages, symbols=0),
            }
        stage_started = time.perf_counter()
        rates = load_latest_funding_rates(session=db)
        stages['rate_load_ms'] += (time.perf_counter() - stage_started) * 1000
        sent = checked = matched_count = 0
        for rule in rules:
            params = rule.params_json or {}
            threshold = float(params.get('threshold'))
            direction = params.get('direction', 'absolute')
            recovery_confirmations = int(params.get('recovery_confirmations', 3))
            stage_started = time.perf_counter()
            states = _load_rule_states(db, rule.id, direction, rates)
            stages['state_load_ms'] += (time.perf_counter() - stage_started) * 1000
            events = []
            rule_checked = 0
            stage_started = time.perf_counter()
            for symbol, row in rates.items():
                if row['funding_rate'] is None:
                    continue
                rate = row['funding_rate']
                matched = abs(rate) >= threshold if direction == 'absolute' else (rate >= threshold if direction == 'positive' else rate <= -threshold)
                checked += 1
                rule_checked += 1
                matched_count += int(matched)
                result = _observe(
                    db, rule, symbol, direction, matched,
                    {'funding_rate': rate, 'threshold': threshold, 'direction': direction, 'event_time': row['event_time']},
                    f'{symbol} 资金费率异常',
                    f'资金费率 {rate * 100:.4f}%，规则阈值 {threshold * 100:.4f}%。',
                    recovery_confirmations=recovery_confirmations,
                    state=states.get(symbol),
                    aggregate=True,
                )
                if result.get('state') and result['event_status']:
                    previous_rate = float((result['previous_values'] or {}).get('funding_rate') or 0)
                    events.append({
                        'status': result['event_status'],
                        'state': result['state'],
                        'triggered': f'- {symbol} 资金费率 {rate * 100:.4f}%',
                        'recovered': f'- {symbol}\n  之前：{previous_rate * 100:.4f}%\n  当前：{rate * 100:.4f}%',
                    })
            stages['observation_ms'] += (time.perf_counter() - stage_started) * 1000
            stage_started = time.perf_counter()
            sent += _deliver_evaluation_summary(
                db, rule, rule_checked, events, f'|资金费率| >= {threshold * 100:.4f}%',
            )
            stages['delivery_ms'] += (time.perf_counter() - stage_started) * 1000
        stage_started = time.perf_counter()
        db.commit()
        stages['commit_ms'] += (time.perf_counter() - stage_started) * 1000
        return {
            'status': 'success', 'evaluated': len(rules), 'checked': checked,
            'matched': matched_count, 'sent': sent,
            'metrics': _evaluation_metrics(started_at, stages, symbols=len(rates)),
        }
    except Exception:
        db.rollback()
        logger.exception('资金费率告警评估失败')
        return {
            'status': 'error', 'evaluated': 0, 'checked': 0, 'matched': 0, 'sent': 0,
            'error': 'funding rate evaluation failed',
            'metrics': _evaluation_metrics(started_at, stages),
        }
    finally:
        if own_session:
            db.close()


def evaluate_price_volume_rules(session=None, rule_id=None):
    if not config.NOTIFICATIONS_ENABLED:
        return {'status': 'disabled', 'evaluated': 0, 'checked': 0, 'matched': 0, 'sent': 0}
    own_session = session is None
    db = session or get_session()
    started_at = time.perf_counter()
    stages = defaultdict(float)
    try:
        rules = _enabled_rules(db, EVENT_PRICE_VOLUME, rule_id=rule_id)
        sent = checked = matched_count = 0
        for rule in rules:
            params = rule.params_json or {}
            scope = rule.scope_json or {}
            events = []
            rule_checked = 0
            stage_started = time.perf_counter()
            metrics_by_symbol = _load_price_volume_metrics(
                db, int(scope.get('limit', config.FETCH_COINS_TOP_VOLUME_COUNT)),
            )
            stages['metric_load_ms'] += (time.perf_counter() - stage_started) * 1000
            symbols = list(metrics_by_symbol)
            direction = params.get('direction', 'absolute')
            stage_started = time.perf_counter()
            states = _load_rule_states(db, rule.id, direction, symbols)
            stages['state_load_ms'] += (time.perf_counter() - stage_started) * 1000
            stage_started = time.perf_counter()
            for symbol in symbols:
                metric = metrics_by_symbol[symbol]
                price_change = metric['price_change']
                volume_ratio = metric['volume_ratio']
                if (
                    metric['kline_count'] < 2
                    or metric['historical_volume_count'] == 0
                    or price_change is None
                    or volume_ratio is None
                ):
                    continue
                price_threshold = float(params['price_change_threshold'])
                volume_threshold = float(params['volume_ratio_threshold'])
                direction_match = abs(price_change) >= price_threshold if direction == 'absolute' else (price_change >= price_threshold if direction == 'up' else price_change <= -price_threshold)
                is_match = direction_match and volume_ratio >= volume_threshold
                checked += 1
                rule_checked += 1
                matched_count += int(is_match)
                result = _observe(
                    db, rule, symbol, direction, is_match,
                    {'price_change': price_change, 'price_change_threshold': price_threshold, 'volume_ratio': volume_ratio, 'volume_ratio_threshold': volume_threshold, 'open_time': metric['open_time']},
                    f'{symbol} 价格放量异动',
                    f'5分钟涨跌 {price_change * 100:.2f}%，成交额放大 {volume_ratio:.2f} 倍。',
                    state=states.get(symbol),
                    aggregate=True,
                )
                if result.get('state') and result['event_status']:
                    previous = result['previous_values'] or {}
                    previous_change = float(previous.get('price_change') or 0)
                    previous_ratio = float(previous.get('volume_ratio') or 0)
                    events.append({
                        'status': result['event_status'],
                        'state': result['state'],
                        'triggered': f'- {symbol} 5分钟涨跌 {price_change * 100:.2f}%，成交额放大 {volume_ratio:.2f} 倍',
                        'recovered': (
                            f'- {symbol}\n  之前：涨跌 {previous_change * 100:.2f}%，成交额 {previous_ratio:.2f} 倍'
                            f'\n  当前：涨跌 {price_change * 100:.2f}%，成交额 {volume_ratio:.2f} 倍'
                        ),
                    })
            stages['observation_ms'] += (time.perf_counter() - stage_started) * 1000
            stage_started = time.perf_counter()
            sent += _deliver_evaluation_summary(
                db, rule, rule_checked, events,
                f'5分钟涨跌 {price_threshold * 100:.2f}% 且成交额放大 >= {volume_threshold:.2f} 倍',
            )
            stages['delivery_ms'] += (time.perf_counter() - stage_started) * 1000
        stage_started = time.perf_counter()
        db.commit()
        stages['commit_ms'] += (time.perf_counter() - stage_started) * 1000
        return {
            'status': 'success', 'evaluated': len(rules), 'checked': checked,
            'matched': matched_count, 'sent': sent,
            'metrics': _evaluation_metrics(
                started_at, stages, symbols=len(symbols) if rules else 0,
                kline_rows=sum(metric['kline_count'] for metric in metrics_by_symbol.values()) if rules else 0,
            ),
        }
    except Exception:
        db.rollback()
        logger.exception('价格放量告警评估失败')
        return {
            'status': 'error', 'evaluated': 0, 'checked': 0, 'matched': 0, 'sent': 0,
            'error': 'price volume evaluation failed',
            'metrics': _evaluation_metrics(started_at, stages),
        }
    finally:
        if own_session:
            db.close()


def evaluate_job_failure_rules(metadata, session=None, rule_id=None):
    if not config.NOTIFICATIONS_ENABLED:
        return {'status': 'disabled', 'evaluated': 0, 'checked': 0, 'matched': 0, 'sent': 0}
    own_session = session is None
    db = session or get_session()
    started_at = time.perf_counter()
    stages = defaultdict(float)
    try:
        rules = _enabled_rules(db, EVENT_JOB_FAILURE, rule_id=rule_id)
        sent = checked = matched_count = 0
        for rule in rules:
            params = rule.params_json or {}
            required_failures = int(params.get('consecutive_failures', 1))
            events = []
            rule_checked = 0
            job_ids = params.get('job_ids', [])
            stage_started = time.perf_counter()
            states = _load_rule_states(db, rule.id, 'failure', job_ids)
            stages['state_load_ms'] += (time.perf_counter() - stage_started) * 1000
            stage_started = time.perf_counter()
            for job_id in job_ids:
                job = metadata.get(job_id) or {}
                failed = job.get('last_status') == 'error'
                state = states.get(job_id)
                current_failures = (int(state.consecutive_matches or 0) if state else 0) + 1 if failed else 0
                is_match = failed and current_failures >= required_failures
                checked += 1
                rule_checked += 1
                matched_count += int(is_match)
                result = _observe(
                    db, rule, job_id, 'failure', is_match,
                    {'last_status': job.get('last_status'), 'last_error': job.get('last_error'), 'consecutive_failures': current_failures},
                    f'任务 {job_id} 执行失败',
                    f'任务状态为 {job.get("last_status") or "unknown"}；错误：{job.get("last_error") or "未提供"}。',
                    consecutive_matches=current_failures,
                    aggregate=True,
                )
                if result.get('state') and result['event_status']:
                    previous = result['previous_values'] or {}
                    previous_error = previous.get('last_error') or '未提供'
                    events.append({
                        'status': result['event_status'],
                    'state': result['state'],
                        'triggered': f'- {job_id} 错误：{job.get("last_error") or "未提供"}',
                        'recovered': f'- {job_id}\n  之前：{previous_error}\n  当前：成功',
                    })
            stages['observation_ms'] += (time.perf_counter() - stage_started) * 1000
            stage_started = time.perf_counter()
            sent += _deliver_evaluation_summary(
                db, rule, rule_checked, events, f'连续失败 >= {required_failures} 次',
            )
            stages['delivery_ms'] += (time.perf_counter() - stage_started) * 1000
        stage_started = time.perf_counter()
        db.commit()
        stages['commit_ms'] += (time.perf_counter() - stage_started) * 1000
        return {
            'status': 'success', 'evaluated': len(rules), 'checked': checked,
            'matched': matched_count, 'sent': sent,
            'metrics': _evaluation_metrics(started_at, stages),
        }
    except Exception:
        db.rollback()
        logger.exception('任务失败告警评估失败')
        return {
            'status': 'error', 'evaluated': 0, 'checked': 0, 'matched': 0, 'sent': 0,
            'error': 'job failure evaluation failed',
            'metrics': _evaluation_metrics(started_at, stages),
        }
    finally:
        if own_session:
            db.close()


def evaluate_rule(rule, session=None, metadata=None):
    if rule.event_type == EVENT_FUNDING_RATE:
        return evaluate_funding_rate_rules(session=session, rule_id=rule.id)
    if rule.event_type == EVENT_PRICE_VOLUME:
        return evaluate_price_volume_rules(session=session, rule_id=rule.id)
    return evaluate_job_failure_rules(metadata or {}, session=session, rule_id=rule.id)


def evaluate_rule_with_run(rule, trigger_source, session=None, metadata=None):
    """Evaluate one rule and persist the same observable run for every source."""
    own_session = session is None
    db = session or get_session()
    run = None
    try:
        AlertEvaluationMetric.__table__.create(bind=db.get_bind(), checkfirst=True)
        run = AlertEvaluationRun(
            rule_id=rule.id,
            trigger_source=trigger_source,
            status='running',
            started_at=now_ms(),
        )
        db.add(run)
        db.commit()

        result = evaluate_rule(rule, session=db, metadata=metadata)
        run.status = result['status']
        run.checked_count = result.get('checked', 0)
        run.matched_count = result.get('matched', 0)
        run.sent_count = result.get('sent', 0)
        run.error_message = result.get('error')
        run.completed_at = now_ms()
        metrics = result.get('metrics') or {}
        if metrics:
            db.add(AlertEvaluationMetric(run_id=run.id, metrics_json=metrics))
        db.commit()
        result['run_id'] = run.id
        return result
    except Exception as exc:
        db.rollback()
        if run is not None:
            failed_run = db.get(AlertEvaluationRun, run.id)
            if failed_run:
                failed_run.status = 'error'
                failed_run.error_message = str(exc)[:500]
                failed_run.completed_at = now_ms()
                db.commit()
        logger.exception('%s alert evaluation failed: rule=%s', trigger_source, rule.id)
        return {
            'status': 'error', 'checked': 0, 'matched': 0, 'sent': 0,
            'error': 'rule evaluation failed', 'run_id': run.id if run else None,
        }
    finally:
        if own_session:
            db.close()


def evaluate_scheduled_rules(event_type, metadata=None):
    """Evaluate enabled rules for a scheduler event, recording one run per rule."""
    db = get_session()
    try:
        rules = _enabled_rules(db, event_type)
        results = [
            evaluate_rule_with_run(rule, 'scheduled', session=db, metadata=metadata)
            for rule in rules
        ]
        return {
            'status': 'error' if any(result['status'] == 'error' for result in results) else 'success',
            'evaluated': len(results),
            'checked': sum(result.get('checked', 0) for result in results),
            'matched': sum(result.get('matched', 0) for result in results),
            'sent': sum(result.get('sent', 0) for result in results),
            'runs': results,
        }
    finally:
        db.close()
