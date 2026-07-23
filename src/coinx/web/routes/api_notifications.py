from flask import Blueprint, jsonify, request

from coinx import config
from coinx.database import get_session
from coinx.models import AlertEvaluationRun, AlertRule, AlertRuleChannel, AlertState, NotificationChannel, NotificationDelivery
from coinx.notifications import (
    NotificationConfigError,
    decrypt_apprise_url,
    encrypt_apprise_url,
    evaluate_rule,
    get_rule_channel_ids,
    serialize_channel,
    serialize_rule,
    set_rule_channels,
    test_channel,
    now_ms,
    validate_rule_payload,
)
from coinx.scheduler import get_all_job_runtime_metadata
from coinx.utils import logger


api_notifications_bp = Blueprint('api_notifications', __name__)
MANUAL_EVALUATION_TIMEOUT_MS = 2 * 60 * 1000


def _error(message, status=400):
    return jsonify({'status': 'error', 'message': message}), status


def _finalize_stale_manual_evaluations(db):
    cutoff = now_ms() - MANUAL_EVALUATION_TIMEOUT_MS
    rows = db.query(AlertEvaluationRun).filter(
        AlertEvaluationRun.trigger_source == 'manual',
        AlertEvaluationRun.status == 'running',
        AlertEvaluationRun.started_at < cutoff,
    ).all()
    if not rows:
        return
    finished_at = now_ms()
    for row in rows:
        row.status = 'error'
        row.error_message = 'evaluation timed out or was interrupted by a service restart'
        row.completed_at = finished_at
    db.commit()


def _channel_payload(channel):
    data = request.get_json(silent=True) or {}
    name = (data.get('name') or '').strip()
    if not name or len(name) > 100:
        raise NotificationConfigError('invalid channel name')
    url = data.get('url')
    if url is None:
        return name, None, bool(data.get('enabled', channel.enabled if channel else True))
    return name, encrypt_apprise_url(url), bool(data.get('enabled', channel.enabled if channel else True))


@api_notifications_bp.route('/api/notification-channels', methods=['GET'])
def list_notification_channels():
    db = get_session()
    try:
        channels = db.query(NotificationChannel).order_by(NotificationChannel.name.asc()).all()
        return jsonify({'status': 'success', 'data': [serialize_channel(channel) for channel in channels]})
    finally:
        db.close()


@api_notifications_bp.route('/api/notification-channels', methods=['POST'])
def create_notification_channel():
    db = get_session()
    try:
        name, encrypted, enabled = _channel_payload(None)
        if not encrypted:
            return _error('url is required')
        if db.query(NotificationChannel).filter(NotificationChannel.name == name).first():
            return _error('channel name already exists', 409)
        channel = NotificationChannel(
            name=name,
            channel_type='apprise',
            config_encrypted=encrypted,
            key_version=config.NOTIFICATION_ENCRYPTION_KEY_VERSION,
            enabled=enabled,
        )
        db.add(channel)
        db.commit()
        return jsonify({'status': 'success', 'data': serialize_channel(channel)}), 201
    except NotificationConfigError as exc:
        db.rollback()
        return _error(str(exc))
    finally:
        db.close()


@api_notifications_bp.route('/api/notification-channels/<int:channel_id>', methods=['PATCH'])
def update_notification_channel(channel_id):
    db = get_session()
    try:
        channel = db.get(NotificationChannel, channel_id)
        if not channel:
            return _error('channel not found', 404)
        name, encrypted, enabled = _channel_payload(channel)
        if name != channel.name and db.query(NotificationChannel).filter(NotificationChannel.name == name).first():
            return _error('channel name already exists', 409)
        channel.name = name
        channel.enabled = enabled
        if encrypted:
            channel.config_encrypted = encrypted
            channel.key_version = config.NOTIFICATION_ENCRYPTION_KEY_VERSION
        db.commit()
        return jsonify({'status': 'success', 'data': serialize_channel(channel)})
    except NotificationConfigError as exc:
        db.rollback()
        return _error(str(exc))
    finally:
        db.close()


@api_notifications_bp.route('/api/notification-channels/<int:channel_id>', methods=['DELETE'])
def delete_notification_channel(channel_id):
    db = get_session()
    try:
        channel = db.get(NotificationChannel, channel_id)
        if not channel:
            return _error('channel not found', 404)
        if db.query(AlertRuleChannel).filter(AlertRuleChannel.channel_id == channel_id).first():
            return _error('channel is used by alert rules', 409)
        db.delete(channel)
        db.commit()
        return jsonify({'status': 'success', 'message': 'channel deleted'})
    finally:
        db.close()


@api_notifications_bp.route('/api/notification-channels/<int:channel_id>/test', methods=['POST'])
def test_notification_channel(channel_id):
    db = get_session()
    try:
        channel = db.get(NotificationChannel, channel_id)
        if not channel:
            return _error('channel not found', 404)
        if not channel.enabled:
            return _error('channel is disabled', 409)
        delivery = test_channel(db, channel)
        db.commit()
        status = 'success' if delivery.delivery_status == 'success' else 'error'
        code = 200 if status == 'success' else 502
        return jsonify({'status': status, 'data': {'delivery_status': delivery.delivery_status}}), code
    finally:
        db.close()


def _save_rule(db, rule, data):
    validated = validate_rule_payload(data)
    for key, value in validated.items():
        setattr(rule, key, value)
    db.flush()
    channel_ids = data.get('channel_ids')
    if channel_ids is None:
        channel_ids = get_rule_channel_ids(db, rule.id)
    set_rule_channels(db, rule, channel_ids)
    return rule


@api_notifications_bp.route('/api/alert-rules', methods=['GET'])
def list_alert_rules():
    db = get_session()
    try:
        rules = db.query(AlertRule).order_by(AlertRule.name.asc()).all()
        data = []
        for rule in rules:
            item = serialize_rule(rule, get_rule_channel_ids(db, rule.id))
            total = db.query(AlertState).filter(AlertState.rule_id == rule.id).count()
            triggered = db.query(AlertState).filter(AlertState.rule_id == rule.id, AlertState.state == 'triggered').count()
            latest_run = db.query(AlertEvaluationRun).filter(AlertEvaluationRun.rule_id == rule.id).order_by(AlertEvaluationRun.started_at.desc()).first()
            item['state_summary'] = {'total': total, 'triggered': triggered}
            item['latest_run'] = None if not latest_run else {
                'status': latest_run.status, 'started_at': latest_run.started_at,
                'checked_count': latest_run.checked_count, 'matched_count': latest_run.matched_count,
                'sent_count': latest_run.sent_count,
            }
            data.append(item)
        return jsonify({'status': 'success', 'data': data})
    finally:
        db.close()


@api_notifications_bp.route('/api/alert-rules', methods=['POST'])
def create_alert_rule():
    db = get_session()
    try:
        data = request.get_json(silent=True) or {}
        if db.query(AlertRule).filter(AlertRule.name == (data.get('name') or '').strip()).first():
            return _error('rule name already exists', 409)
        rule = AlertRule()
        db.add(rule)
        rule = _save_rule(db, rule, data)
        db.commit()
        return jsonify({'status': 'success', 'data': serialize_rule(rule, get_rule_channel_ids(db, rule.id))}), 201
    except NotificationConfigError as exc:
        db.rollback()
        return _error(str(exc))
    finally:
        db.close()


@api_notifications_bp.route('/api/alert-rules/<int:rule_id>', methods=['PATCH'])
def update_alert_rule(rule_id):
    db = get_session()
    try:
        rule = db.get(AlertRule, rule_id)
        if not rule:
            return _error('rule not found', 404)
        data = request.get_json(silent=True) or {}
        merged = serialize_rule(rule, get_rule_channel_ids(db, rule.id))
        merged.update(data)
        if 'scope' not in data:
            merged['scope'] = rule.scope_json or {}
        if 'params' not in data:
            merged['params'] = rule.params_json or {}
        if 'channel_ids' not in data:
            merged['channel_ids'] = get_rule_channel_ids(db, rule.id)
        if merged['name'] != rule.name and db.query(AlertRule).filter(AlertRule.name == merged['name']).first():
            return _error('rule name already exists', 409)
        _save_rule(db, rule, merged)
        db.commit()
        return jsonify({'status': 'success', 'data': serialize_rule(rule, get_rule_channel_ids(db, rule.id))})
    except NotificationConfigError as exc:
        db.rollback()
        return _error(str(exc))
    finally:
        db.close()


def _pagination_args():
    limit = min(max(request.args.get('limit', 50, type=int), 1), 100)
    return limit, max(request.args.get('offset', 0, type=int), 0)


def _rule_or_404(db, rule_id):
    rule = db.get(AlertRule, rule_id)
    return rule


@api_notifications_bp.route('/api/alert-rules/<int:rule_id>/states', methods=['GET'])
def list_rule_states(rule_id):
    db = get_session()
    try:
        if not _rule_or_404(db, rule_id): return _error('rule not found', 404)
        limit, offset = _pagination_args(); status = request.args.get('status', 'triggered')
        query = db.query(AlertState).filter(AlertState.rule_id == rule_id)
        if status in {'normal', 'triggered'}: query = query.filter(AlertState.state == status)
        total = query.count(); rows = query.order_by(AlertState.updated_at.desc()).offset(offset).limit(limit).all()
        return jsonify({'status':'success','data':{'items':[{'subject_key':r.subject_key,'dimension_key':r.dimension_key,'state':r.state,'last_value':r.last_value_json,'last_triggered_at':r.last_triggered_at,'last_recovered_at':r.last_recovered_at} for r in rows],'total':total,'limit':limit,'offset':offset}})
    finally: db.close()


@api_notifications_bp.route('/api/alert-rules/<int:rule_id>/evaluation-runs', methods=['GET'])
def list_rule_evaluation_runs(rule_id):
    db = get_session()
    try:
        if not _rule_or_404(db, rule_id): return _error('rule not found', 404)
        _finalize_stale_manual_evaluations(db); limit, offset = _pagination_args()
        query = db.query(AlertEvaluationRun).filter(AlertEvaluationRun.rule_id == rule_id); total=query.count(); rows=query.order_by(AlertEvaluationRun.started_at.desc()).offset(offset).limit(limit).all()
        return jsonify({'status':'success','data':{'items':[{'id':r.id,'status':r.status,'checked_count':r.checked_count,'matched_count':r.matched_count,'sent_count':r.sent_count,'error_message':r.error_message,'started_at':r.started_at,'completed_at':r.completed_at} for r in rows],'total':total,'limit':limit,'offset':offset}})
    finally: db.close()


@api_notifications_bp.route('/api/alert-rules/<int:rule_id>/deliveries', methods=['GET'])
def list_rule_deliveries(rule_id):
    db = get_session()
    try:
        if not _rule_or_404(db, rule_id): return _error('rule not found', 404)
        limit, offset = _pagination_args(); query=db.query(NotificationDelivery).filter(NotificationDelivery.rule_id == rule_id); total=query.count(); rows=query.order_by(NotificationDelivery.sent_at.desc()).offset(offset).limit(limit).all()
        return jsonify({'status':'success','data':{'items':[{'id':r.id,'event_status':r.event_status,'delivery_status':r.delivery_status,'error_message':r.error_message,'sent_at':r.sent_at} for r in rows],'total':total,'limit':limit,'offset':offset}})
    finally: db.close()


@api_notifications_bp.route('/api/alert-rules/<int:rule_id>', methods=['DELETE'])
def delete_alert_rule(rule_id):
    db = get_session()
    try:
        rule = db.get(AlertRule, rule_id)
        if not rule:
            return _error('rule not found', 404)
        db.query(AlertRuleChannel).filter(AlertRuleChannel.rule_id == rule_id).delete()
        db.query(AlertState).filter(AlertState.rule_id == rule_id).delete()
        db.query(NotificationDelivery).filter(NotificationDelivery.rule_id == rule_id).delete()
        db.delete(rule)
        db.commit()
        return jsonify({'status': 'success', 'message': 'rule deleted'})
    finally:
        db.close()


@api_notifications_bp.route('/api/alert-rules/<int:rule_id>/evaluate', methods=['POST'])
def evaluate_alert_rule(rule_id):
    db = get_session()
    run = None
    try:
        rule = db.get(AlertRule, rule_id)
        if not rule:
            return _error('rule not found', 404)
        if not rule.enabled:
            return _error('rule is disabled', 409)
        run = AlertEvaluationRun(rule_id=rule.id, trigger_source='manual', status='running', started_at=now_ms())
        db.add(run)
        db.commit()
        result = evaluate_rule(rule, session=db, metadata=get_all_job_runtime_metadata())
        run.status = result['status']
        run.checked_count = result.get('checked', 0)
        run.matched_count = result.get('matched', 0)
        run.sent_count = result.get('sent', 0)
        run.error_message = result.get('error')
        run.completed_at = now_ms()
        db.commit()
        result['run_id'] = run.id
        code = 200 if result['status'] in {'success', 'disabled'} else 500
        return jsonify({'status': result['status'], 'data': result}), code
    except Exception as exc:
        db.rollback()
        if run is not None:
            failed_run = db.get(AlertEvaluationRun, run.id)
            if failed_run:
                failed_run.status = 'error'
                failed_run.error_message = str(exc)[:500]
                failed_run.completed_at = now_ms()
                db.commit()
        logger.exception('manual alert evaluation failed: rule=%s', rule_id)
        return _error('rule evaluation failed', 500)
    finally:
        db.close()


@api_notifications_bp.route('/api/notification-deliveries', methods=['GET'])
def list_notification_deliveries():
    db = get_session()
    try:
        limit = min(max(request.args.get('limit', 100, type=int), 1), 500)
        rows = db.query(NotificationDelivery).order_by(NotificationDelivery.sent_at.desc()).limit(limit).all()
        return jsonify({'status': 'success', 'data': [{
            'id': row.id, 'rule_id': row.rule_id, 'channel_id': row.channel_id,
            'event_key': row.event_key, 'event_status': row.event_status,
            'delivery_status': row.delivery_status, 'error_message': row.error_message,
            'sent_at': row.sent_at,
        } for row in rows]})
    finally:
        db.close()


@api_notifications_bp.route('/api/alert-evaluation-runs', methods=['GET'])
def list_alert_evaluation_runs():
    db = get_session()
    try:
        _finalize_stale_manual_evaluations(db)
        limit = min(max(request.args.get('limit', 100, type=int), 1), 500)
        rows = db.query(AlertEvaluationRun).order_by(AlertEvaluationRun.started_at.desc()).limit(limit).all()
        return jsonify({'status': 'success', 'data': [{
            'id': row.id, 'rule_id': row.rule_id, 'trigger_source': row.trigger_source,
            'status': row.status, 'checked_count': row.checked_count,
            'matched_count': row.matched_count, 'sent_count': row.sent_count,
            'error_message': row.error_message, 'started_at': row.started_at,
            'completed_at': row.completed_at,
        } for row in rows]})
    finally:
        db.close()


@api_notifications_bp.route('/api/alert-evaluation-runs/<int:run_id>/logs', methods=['GET'])
def get_alert_evaluation_run_logs(run_id):
    db = get_session()
    try:
        _finalize_stale_manual_evaluations(db)
        run = db.get(AlertEvaluationRun, run_id)
        if not run:
            return _error('evaluation run not found', 404)
        completed_at = run.completed_at or now_ms()
        logs = [{
            'timestamp': run.started_at,
            'level': 'info',
            'message': f'manual evaluation started for rule #{run.rule_id}',
        }]
        if run.completed_at:
            logs.append({
                'timestamp': run.completed_at,
                'level': 'error' if run.status == 'error' else 'info',
                'message': run.error_message or (
                    f'evaluation completed: checked={run.checked_count}, '
                    f'matched={run.matched_count}, sent={run.sent_count}'
                ),
            })
        else:
            logs.append({
                'timestamp': now_ms(),
                'level': 'warning',
                'message': 'evaluation is still running',
            })
        deliveries = db.query(NotificationDelivery).filter(
            NotificationDelivery.rule_id == run.rule_id,
            NotificationDelivery.sent_at >= run.started_at,
            NotificationDelivery.sent_at <= completed_at,
        ).order_by(NotificationDelivery.sent_at.asc()).all()
        for delivery in deliveries:
            logs.append({
                'timestamp': delivery.sent_at,
                'level': 'error' if delivery.delivery_status == 'failed' else 'info',
                'message': (
                    f'delivery {delivery.delivery_status}: {delivery.event_status}'
                    + (f' ({delivery.error_message})' if delivery.error_message else '')
                ),
            })
        return jsonify({'status': 'success', 'data': {'run_id': run.id, 'logs': logs}})
    finally:
        db.close()


@api_notifications_bp.route('/api/alert-states', methods=['GET'])
def list_alert_states():
    db = get_session()
    try:
        rows = db.query(AlertState).order_by(AlertState.updated_at.desc()).all()
        return jsonify({'status': 'success', 'data': [{
            'rule_id': row.rule_id, 'subject_key': row.subject_key,
            'dimension_key': row.dimension_key, 'state': row.state,
            'last_value': row.last_value_json, 'last_triggered_at': row.last_triggered_at,
            'last_recovered_at': row.last_recovered_at,
        } for row in rows]})
    finally:
        db.close()
