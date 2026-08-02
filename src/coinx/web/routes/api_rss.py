from urllib.parse import urlparse

from flask import Blueprint, jsonify, request

from coinx.database import get_session
from coinx.models import RssArticle, RssSubscription
from coinx.rss_monitor import (
    ensure_rss_schema,
    list_articles,
    serialize_article,
    serialize_subscription,
    sync_subscription,
)


api_rss_bp = Blueprint('api_rss', __name__)


def _error(message, status=400):
    return jsonify({'status': 'error', 'message': message}), status


def _subscription_payload(data, existing=None):
    data = data or {}
    name = str(data.get('name', existing.name if existing else '')).strip()
    url = str(data.get('url', existing.url if existing else '')).strip()
    parsed = urlparse(url)
    if not name or len(name) > 160:
        raise ValueError('invalid subscription name')
    if parsed.scheme not in {'http', 'https'} or not parsed.netloc or len(url) > 500:
        raise ValueError('RSS URL must be an http(s) URL')
    channel_ids = data.get('notification_channel_ids', existing.notification_channel_ids if existing else None)
    if channel_ids is not None:
        if not isinstance(channel_ids, list) or any(not isinstance(item, int) or item < 1 for item in channel_ids):
            raise ValueError('notification channel IDs must be a list of positive integers')
        channel_ids = sorted(set(channel_ids))
    return {
        'name': name,
        'url': url,
        'enabled': bool(data.get('enabled', existing.enabled if existing else True)),
        'monitor_enabled': bool(data.get('monitor_enabled', existing.monitor_enabled if existing else True)),
        'notification_channel_ids': channel_ids,
    }


def _subscription_item(db, row):
    count = db.query(RssArticle).filter(RssArticle.subscription_id == row.id).count()
    return serialize_subscription(row, count)


@api_rss_bp.route('/api/rss/subscriptions', methods=['GET'])
def get_rss_subscriptions():
    db = get_session()
    try:
        ensure_rss_schema(db.get_bind())
        rows = db.query(RssSubscription).order_by(RssSubscription.created_at.asc(), RssSubscription.id.asc()).all()
        return jsonify({'status': 'success', 'data': [_subscription_item(db, row) for row in rows]})
    finally:
        db.close()


@api_rss_bp.route('/api/rss/subscriptions', methods=['POST'])
def create_rss_subscription():
    db = get_session()
    try:
        ensure_rss_schema(db.get_bind())
        payload = _subscription_payload(request.get_json(silent=True))
        if db.query(RssSubscription).filter(RssSubscription.url == payload['url']).first():
            return _error('subscription URL already exists', 409)
        row = RssSubscription(**payload)
        db.add(row)
        db.commit()
        return jsonify({'status': 'success', 'data': _subscription_item(db, row)}), 201
    except ValueError as exc:
        db.rollback()
        return _error(str(exc))
    finally:
        db.close()


@api_rss_bp.route('/api/rss/subscriptions/<int:subscription_id>', methods=['PATCH'])
def update_rss_subscription(subscription_id):
    db = get_session()
    try:
        ensure_rss_schema(db.get_bind())
        row = db.get(RssSubscription, subscription_id)
        if not row:
            return _error('subscription not found', 404)
        payload = _subscription_payload(request.get_json(silent=True), row)
        duplicate = db.query(RssSubscription).filter(
            RssSubscription.url == payload['url'],
            RssSubscription.id != row.id,
        ).first()
        if duplicate:
            return _error('subscription URL already exists', 409)
        for key, value in payload.items():
            setattr(row, key, value)
        db.commit()
        return jsonify({'status': 'success', 'data': _subscription_item(db, row)})
    except ValueError as exc:
        db.rollback()
        return _error(str(exc))
    finally:
        db.close()


@api_rss_bp.route('/api/rss/subscriptions/<int:subscription_id>', methods=['DELETE'])
def delete_rss_subscription(subscription_id):
    db = get_session()
    try:
        ensure_rss_schema(db.get_bind())
        row = db.get(RssSubscription, subscription_id)
        if not row:
            return _error('subscription not found', 404)
        db.query(RssArticle).filter(RssArticle.subscription_id == row.id).delete(synchronize_session=False)
        db.delete(row)
        db.commit()
        return jsonify({'status': 'success', 'message': 'subscription deleted'})
    finally:
        db.close()


@api_rss_bp.route('/api/rss/subscriptions/<int:subscription_id>/refresh', methods=['POST'])
def refresh_rss_subscription(subscription_id):
    db = get_session()
    try:
        ensure_rss_schema(db.get_bind())
        row = db.get(RssSubscription, subscription_id)
        if not row:
            return _error('subscription not found', 404)
        payload = request.get_json(silent=True) or {}
        if not isinstance(payload, dict):
            return _error('request body must be an object')
        result = sync_subscription(db, row, notify=bool(payload.get('notify', True)))
        if result['status'] == 'error':
            return jsonify({
                'status': 'error',
                'message': result.get('error') or 'RSS fetch failed',
                'data': result,
            }), 502
        return jsonify({'status': result['status'], 'data': result})
    finally:
        db.close()


@api_rss_bp.route('/api/rss/articles', methods=['GET'])
def get_rss_articles():
    db = get_session()
    try:
        ensure_rss_schema(db.get_bind())
        limit = min(max(request.args.get('limit', 50, type=int), 1), 100)
        offset = max(request.args.get('offset', 0, type=int), 0)
        subscription_id = request.args.get('subscription_id', type=int)
        total, items = list_articles(db, subscription_id, limit, offset)
        return jsonify({'status': 'success', 'data': {
            'items': items, 'total': total, 'limit': limit, 'offset': offset,
        }})
    finally:
        db.close()


@api_rss_bp.route('/api/rss/articles/<int:article_id>', methods=['GET'])
def get_rss_article(article_id):
    db = get_session()
    try:
        ensure_rss_schema(db.get_bind())
        row = db.get(RssArticle, article_id)
        if not row:
            return _error('article not found', 404)
        subscription = db.get(RssSubscription, row.subscription_id)
        return jsonify({'status': 'success', 'data': serialize_article(row, subscription)})
    finally:
        db.close()
