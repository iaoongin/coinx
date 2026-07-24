from flask import Blueprint, jsonify, request

from coinx import config
from coinx.database import get_session
from coinx.models import (
    AlertEvaluationMetric, AlertEvaluationRun, AlertRule, AlertRuleChannel,
    AlertState, NotificationChannel, NotificationDelivery,
)
from coinx.notifications import (
    NotificationConfigError,
    decrypt_apprise_url,
    encrypt_apprise_url,
    evaluate_rule_with_run,
    get_rule_channel_ids,
    is_evaluation_run_active,
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
EVALUATION_STALE_TIMEOUT_MS = 5 * 60 * 1000


def _error(message, status=400):
    return jsonify({'status': 'error', 'message': message}), status


def _ensure_evaluation_metrics_table(db):
    """Create the additive timing table for deployments with an older schema."""
    AlertEvaluationMetric.__table__.create(bind=db.get_bind(), checkfirst=True)


def _finalize_stale_evaluations(db):
    """Mark abandoned manual and scheduled evaluations as failed."""
    cutoff = now_ms() - EVALUATION_STALE_TIMEOUT_MS
    rows = db.query(AlertEvaluationRun).filter(
        AlertEvaluationRun.status == 'running',
        AlertEvaluationRun.started_at < cutoff,
    ).all()
    if not rows:
        return
    finished_at = now_ms()
    for row in rows:
        if is_evaluation_run_active(db, row.id):
            logger.info('evaluation remains active; stale finalization skipped: run=%s', row.id)
            continue
        row.status = 'error'
        row.error_message = 'evaluation exceeded the 5-minute timeout or was interrupted by a service restart'
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
        response = {
            'status': status,
            'data': {
                'delivery_status': delivery.delivery_status,
                'error_message': delivery.error_message,
            },
        }
        if delivery.error_message:
            response['message'] = f'测试发送失败：{delivery.error_message}'
        return jsonify(response), code
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
        previous_evaluation_signature = (
            rule.event_type,
            rule.scope_type,
            rule.scope_json or {},
            rule.params_json or {},
        )
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
        state_reset = previous_evaluation_signature != (
            rule.event_type,
            rule.scope_type,
            rule.scope_json or {},
            rule.params_json or {},
        )
        if state_reset:
            db.query(AlertState).filter(AlertState.rule_id == rule.id).delete(synchronize_session=False)
        db.commit()
        return jsonify({
            'status': 'success',
            'data': serialize_rule(rule, get_rule_channel_ids(db, rule.id)),
            'meta': {'state_reset': state_reset},
        })
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


def _delivery_item(row):
    payload = row.payload_json or {}
    return {
        'id': row.id,
        'event_status': row.event_status,
        'delivery_status': row.delivery_status,
        'error_message': row.error_message,
        'sent_at': row.sent_at,
        'message': payload.get('message'),
    }


@api_notifications_bp.route('/api/alert-rules/<int:rule_id>/states', methods=['GET'])
def list_rule_states(rule_id):
    db = get_session()
    try:
        if not _rule_or_404(db, rule_id): return _error('rule not found', 404)
        limit, offset = _pagination_args(); status = request.args.get('status')
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
        _finalize_stale_evaluations(db); limit, offset = _pagination_args()
        query = db.query(AlertEvaluationRun).filter(AlertEvaluationRun.rule_id == rule_id); total=query.count(); rows=query.order_by(AlertEvaluationRun.started_at.desc()).offset(offset).limit(limit).all()
        return jsonify({'status':'success','data':{'items':[{'id':r.id,'rule_id':r.rule_id,'status':r.status,'checked_count':r.checked_count,'matched_count':r.matched_count,'sent_count':r.sent_count,'error_message':r.error_message,'started_at':r.started_at,'completed_at':r.completed_at,'duration_ms':(r.completed_at-r.started_at) if r.completed_at else None} for r in rows],'total':total,'limit':limit,'offset':offset}})
    finally: db.close()


@api_notifications_bp.route('/api/alert-rules/<int:rule_id>/deliveries', methods=['GET'])
def list_rule_deliveries(rule_id):
    db = get_session()
    try:
        if not _rule_or_404(db, rule_id): return _error('rule not found', 404)
        limit, offset = _pagination_args(); query=db.query(NotificationDelivery).filter(NotificationDelivery.rule_id == rule_id); total=query.count(); rows=query.order_by(NotificationDelivery.sent_at.desc()).offset(offset).limit(limit).all()
        return jsonify({'status':'success','data':{'items':[_delivery_item(row) for row in rows],'total':total,'limit':limit,'offset':offset}})
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
    try:
        rule = db.get(AlertRule, rule_id)
        if not rule:
            return _error('rule not found', 404)
        if not rule.enabled:
            return _error('rule is disabled', 409)
        result = evaluate_rule_with_run(
            rule, 'manual', session=db, metadata=get_all_job_runtime_metadata(),
        )
        code = 200 if result['status'] in {'success', 'disabled', 'skipped'} else 500
        return jsonify({'status': result['status'], 'data': result}), code
    finally:
        db.close()


@api_notifications_bp.route('/api/notification-deliveries', methods=['GET'])
def list_notification_deliveries():
    db = get_session()
    try:
        limit = min(max(request.args.get('limit', 100, type=int), 1), 500)
        rows = db.query(NotificationDelivery).order_by(NotificationDelivery.sent_at.desc()).limit(limit).all()
        return jsonify({'status': 'success', 'data': [{
            **_delivery_item(row),
            'rule_id': row.rule_id,
            'channel_id': row.channel_id,
            'event_key': row.event_key,
        } for row in rows]})
    finally:
        db.close()


@api_notifications_bp.route('/api/alert-evaluation-runs', methods=['GET'])
def list_alert_evaluation_runs():
    db = get_session()
    try:
        _finalize_stale_evaluations(db)
        limit = min(max(request.args.get('limit', 100, type=int), 1), 500)
        rows = db.query(AlertEvaluationRun).order_by(AlertEvaluationRun.started_at.desc()).limit(limit).all()
        return jsonify({'status': 'success', 'data': [{
            'id': row.id, 'rule_id': row.rule_id, 'trigger_source': row.trigger_source,
            'status': row.status, 'checked_count': row.checked_count,
            'matched_count': row.matched_count, 'sent_count': row.sent_count,
            'error_message': row.error_message, 'started_at': row.started_at,
            'completed_at': row.completed_at,
            'duration_ms': (row.completed_at - row.started_at) if row.completed_at else None,
        } for row in rows]})
    finally:
        db.close()


@api_notifications_bp.route('/api/alert-evaluation-runs/<int:run_id>/logs', methods=['GET'])
def get_alert_evaluation_run_logs(run_id):
    db = get_session()
    try:
        _ensure_evaluation_metrics_table(db)
        _finalize_stale_evaluations(db)
        run = db.get(AlertEvaluationRun, run_id)
        if not run:
            return _error('evaluation run not found', 404)
        completed_at = run.completed_at or now_ms()
        logs = [{
            'timestamp': run.started_at,
            'level': 'info',
            'message': f'{run.trigger_source} evaluation started for rule #{run.rule_id}',
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
        metric = db.query(AlertEvaluationMetric).filter(
            AlertEvaluationMetric.run_id == run.id,
        ).one_or_none()
        if metric:
            metrics = metric.metrics_json or {}
            stages = metrics.get('stage_ms') or {}
            total = metrics.get('duration_ms')
            stage_text = ', '.join(f'{name}={value:.0f}ms' for name, value in stages.items())
            logs.append({
                'timestamp': run.completed_at or now_ms(),
                'level': 'info',
                'message': (
                    f'evaluation timing: total={total:.0f}ms'
                    + (f'; {stage_text}' if stage_text else '')
                ),
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
        logs.sort(key=lambda item: item['timestamp'], reverse=True)
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
