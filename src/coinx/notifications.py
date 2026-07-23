"""Configurable alert evaluation and Apprise delivery.

All evaluators operate on data already stored by CoinX jobs.  Notification
failures are recorded but never allowed to fail a collection or repair job.
"""
import time

from cryptography.fernet import Fernet, InvalidToken
from coinx import config
from coinx.database import get_session
from coinx.models import (
    AlertRule,
    AlertRuleChannel,
    AlertState,
    MarketKline,
    NotificationChannel,
    NotificationDelivery,
)
from coinx.repositories.funding_rate import load_latest_funding_rates
from coinx.repositories.market_tickers import get_market_ticker_symbols
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
        _positive_number(params.get('threshold'), 'threshold')
        if params.get('direction', 'absolute') not in {'positive', 'negative', 'absolute'}:
            raise NotificationConfigError('invalid direction')
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
    try:
        send_apprise(decrypt_apprise_url(channel), title, body)
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


def _observe(db, rule, subject_key, dimension_key, matched, values, title, summary, consecutive_matches=None, state=None, aggregate=False):
    """Apply the normal/triggered/recovered state machine for one observation."""
    timestamp = now_ms()
    if state is None:
        state = db.query(AlertState).filter_by(
            rule_id=rule.id,
            subject_key=subject_key,
            dimension_key=dimension_key,
        ).first()
    if state is None:
        state = AlertState(
            rule_id=rule.id,
            subject_key=subject_key,
            dimension_key=dimension_key,
            state='normal',
            consecutive_matches=0,
        )
        db.add(state)

    previous = state.state
    previous_values = state.last_value_json or {}
    state.last_value_json = values
    state.consecutive_matches = (
        consecutive_matches if consecutive_matches is not None
        else (state.consecutive_matches + 1 if matched else 0)
    )
    event_status = None
    if matched and previous != 'triggered':
        state.state = 'triggered'
        state.last_triggered_at = timestamp
        event_status = 'triggered'
    elif not matched and previous == 'triggered':
        state.state = 'normal'
        state.last_recovered_at = timestamp
        event_status = 'recovered'
        title = f'{title}已恢复'
        summary = f'{summary}；当前已恢复至规则阈值内。'

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
        state.last_notified_at = timestamp
    return {'event_status': event_status, 'sent': len(deliveries)}


def _enabled_rules(db, event_type, rule_id=None):
    query = db.query(AlertRule).filter(
        AlertRule.event_type == event_type,
        AlertRule.enabled.is_(True),
    )
    if rule_id is not None:
        query = query.filter(AlertRule.id == rule_id)
    return query.all()


def evaluate_funding_rate_rules(session=None, rule_id=None):
    if not config.NOTIFICATIONS_ENABLED:
        return {'status': 'disabled', 'evaluated': 0, 'checked': 0, 'matched': 0, 'sent': 0}
    own_session = session is None
    db = session or get_session()
    try:
        rules = _enabled_rules(db, EVENT_FUNDING_RATE, rule_id=rule_id)
        if not rules:
            return {'status': 'success', 'evaluated': 0, 'checked': 0, 'matched': 0, 'sent': 0}
        rates = load_latest_funding_rates(session=db)
        sent = checked = matched_count = 0
        for rule in rules:
            threshold = float((rule.params_json or {}).get('threshold'))
            direction = (rule.params_json or {}).get('direction', 'absolute')
            states = {
                state.subject_key: state
                for state in db.query(AlertState).filter(
                    AlertState.rule_id == rule.id,
                    AlertState.dimension_key == direction,
                    AlertState.subject_key.in_(list(rates)),
                ).all()
            } if rates else {}
            events = []
            for symbol, row in rates.items():
                if row['funding_rate'] is None:
                    continue
                rate = row['funding_rate']
                matched = abs(rate) >= threshold if direction == 'absolute' else (rate >= threshold if direction == 'positive' else rate <= -threshold)
                checked += 1
                matched_count += int(matched)
                result = _observe(
                    db, rule, symbol, direction, matched,
                    {'funding_rate': rate, 'threshold': threshold, 'direction': direction, 'event_time': row['event_time']},
                    f'{symbol} 资金费率异常',
                    f'资金费率 {rate * 100:.4f}%，规则阈值 {threshold * 100:.4f}%。',
                    state=states.get(symbol),
                    aggregate=True,
                )
                if result['event_status']:
                    events.append((symbol, rate, result))
            if events:
                triggered = [f'- {symbol} {rate * 100:.4f}%' for symbol, rate, result in events if result['event_status'] == 'triggered']
                recovered = [
                    f'- {symbol}\n  之前：{float((result["previous_values"] or {}).get("funding_rate") or 0) * 100:.4f}%\n  当前：{rate * 100:.4f}%'
                    for symbol, rate, result in events if result['event_status'] == 'recovered'
                ]
                sections = [f'本次评估完成\n检查币种：{checked}\n触发异常：{len(triggered)}\n恢复正常：{len(recovered)}']
                if triggered: sections.append('触发异常\n' + '\n'.join(triggered))
                if recovered: sections.append('恢复正常\n' + '\n'.join(recovered))
                sections.append(f'规则阈值：|资金费率| >= {threshold * 100:.4f}%')
                timestamp = now_ms()
                payload = {'event_type': rule.event_type, 'status': 'summary', 'occurred_at': timestamp, 'triggered': len(triggered), 'recovered': len(recovered)}
                deliveries = [_delivery(db, rule, channel, f'rule:{rule.id}|evaluation:{timestamp}', 'summary', payload, f'{rule.name} · 资金费率异常', '\n\n'.join(sections)) for channel in _rule_channels(db, rule.id)]
                if deliveries:
                    for _, _, result in events:
                        result['state'].last_notified_at = timestamp
                sent += len(deliveries)
        db.commit()
        return {'status': 'success', 'evaluated': len(rules), 'checked': checked, 'matched': matched_count, 'sent': sent}
    except Exception:
        db.rollback()
        logger.exception('资金费率告警评估失败')
        return {'status': 'error', 'evaluated': 0, 'checked': 0, 'matched': 0, 'sent': 0, 'error': 'funding rate evaluation failed'}
    finally:
        if own_session:
            db.close()


def evaluate_price_volume_rules(session=None, rule_id=None):
    if not config.NOTIFICATIONS_ENABLED:
        return {'status': 'disabled', 'evaluated': 0, 'checked': 0, 'matched': 0, 'sent': 0}
    own_session = session is None
    db = session or get_session()
    try:
        rules = _enabled_rules(db, EVENT_PRICE_VOLUME, rule_id=rule_id)
        sent = checked = matched_count = 0
        for rule in rules:
            params = rule.params_json or {}
            scope = rule.scope_json or {}
            symbols = get_market_ticker_symbols(
                rank_type='quote_volume',
                limit=int(scope.get('limit', config.FETCH_COINS_TOP_VOLUME_COUNT)),
                session=db,
            )
            for symbol in symbols:
                rows = db.query(MarketKline).filter(
                    MarketKline.exchange == 'binance',
                    MarketKline.symbol == symbol,
                    MarketKline.period == '5m',
                ).order_by(MarketKline.open_time.desc()).limit(289).all()
                if len(rows) < 2:
                    continue
                latest = rows[0]
                historical_volumes = [float(row.quote_volume) for row in rows[1:] if row.quote_volume is not None]
                if latest.open_price in (None, 0) or latest.close_price is None or latest.quote_volume is None or not historical_volumes:
                    continue
                price_change = (float(latest.close_price) - float(latest.open_price)) / float(latest.open_price)
                volume_ratio = float(latest.quote_volume) / (sum(historical_volumes) / len(historical_volumes))
                direction = params.get('direction', 'absolute')
                price_threshold = float(params['price_change_threshold'])
                volume_threshold = float(params['volume_ratio_threshold'])
                direction_match = abs(price_change) >= price_threshold if direction == 'absolute' else (price_change >= price_threshold if direction == 'up' else price_change <= -price_threshold)
                is_match = direction_match and volume_ratio >= volume_threshold
                checked += 1
                matched_count += int(is_match)
                result = _observe(
                    db, rule, symbol, direction, is_match,
                    {'price_change': price_change, 'price_change_threshold': price_threshold, 'volume_ratio': volume_ratio, 'volume_ratio_threshold': volume_threshold, 'open_time': latest.open_time},
                    f'{symbol} 价格放量异动',
                    f'5分钟涨跌 {price_change * 100:.2f}%，成交额放大 {volume_ratio:.2f} 倍。',
                )
                sent += result['sent']
        db.commit()
        return {'status': 'success', 'evaluated': len(rules), 'checked': checked, 'matched': matched_count, 'sent': sent}
    except Exception:
        db.rollback()
        logger.exception('价格放量告警评估失败')
        return {'status': 'error', 'evaluated': 0, 'checked': 0, 'matched': 0, 'sent': 0, 'error': 'price volume evaluation failed'}
    finally:
        if own_session:
            db.close()


def evaluate_job_failure_rules(metadata, session=None, rule_id=None):
    if not config.NOTIFICATIONS_ENABLED:
        return {'status': 'disabled', 'evaluated': 0, 'checked': 0, 'matched': 0, 'sent': 0}
    own_session = session is None
    db = session or get_session()
    try:
        rules = _enabled_rules(db, EVENT_JOB_FAILURE, rule_id=rule_id)
        sent = checked = matched_count = 0
        for rule in rules:
            params = rule.params_json or {}
            required_failures = int(params.get('consecutive_failures', 1))
            for job_id in params.get('job_ids', []):
                job = metadata.get(job_id) or {}
                failed = job.get('last_status') == 'error'
                state = db.query(AlertState).filter_by(rule_id=rule.id, subject_key=job_id, dimension_key='failure').first()
                current_failures = (state.consecutive_matches if state else 0) + 1 if failed else 0
                is_match = failed and current_failures >= required_failures
                checked += 1
                matched_count += int(is_match)
                result = _observe(
                    db, rule, job_id, 'failure', is_match,
                    {'last_status': job.get('last_status'), 'last_error': job.get('last_error'), 'consecutive_failures': current_failures},
                    f'任务 {job_id} 执行失败',
                    f'任务状态为 {job.get("last_status") or "unknown"}；错误：{job.get("last_error") or "未提供"}。',
                    consecutive_matches=current_failures,
                )
                sent += result['sent']
        db.commit()
        return {'status': 'success', 'evaluated': len(rules), 'checked': checked, 'matched': matched_count, 'sent': sent}
    except Exception:
        db.rollback()
        logger.exception('任务失败告警评估失败')
        return {'status': 'error', 'evaluated': 0, 'checked': 0, 'matched': 0, 'sent': 0, 'error': 'job failure evaluation failed'}
    finally:
        if own_session:
            db.close()


def evaluate_rule(rule, session=None, metadata=None):
    if rule.event_type == EVENT_FUNDING_RATE:
        return evaluate_funding_rate_rules(session=session, rule_id=rule.id)
    if rule.event_type == EVENT_PRICE_VOLUME:
        return evaluate_price_volume_rules(session=session, rule_id=rule.id)
    return evaluate_job_failure_rules(metadata or {}, session=session, rule_id=rule.id)
