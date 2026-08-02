"""RSS subscription storage, fetching, article browsing, and notifications."""

import html
import re
import time
from email.utils import parsedate_to_datetime
from urllib.parse import urlsplit, urlunsplit
from xml.etree import ElementTree

import requests
from sqlalchemy import desc, inspect, text
from sqlalchemy.orm import sessionmaker

from coinx import config
from coinx.database import Base, engine, get_session
from coinx.models import NotificationChannel, RssArticle, RssSubscription
from coinx.utils import logger


_TAG_RE = re.compile(r'<[^>]+>')
_WHITESPACE_RE = re.compile(r'\s+')


def now_ms():
    return int(time.time() * 1000)


def ensure_rss_schema(bind=None):
    """Create additive RSS tables on deployments without a migration runner."""
    bind = bind or engine
    RssSubscription.__table__.create(bind=bind, checkfirst=True)
    RssArticle.__table__.create(bind=bind, checkfirst=True)
    columns = {column['name'] for column in inspect(bind).get_columns('rss_subscriptions')}
    if 'notification_channel_ids' not in columns:
        with bind.begin() as connection:
            connection.execute(text('ALTER TABLE rss_subscriptions ADD COLUMN notification_channel_ids JSON NULL'))


def _feed_text(element):
    if element is None:
        return ''
    return ''.join(element.itertext()).strip()


def _local_name(tag):
    return tag.rsplit('}', 1)[-1].lower()


def _child_text(parent, *names):
    if parent is None:
        return ''
    wanted = {name.lower() for name in names}
    for child in list(parent):
        if _local_name(child.tag) in wanted:
            return _feed_text(child)
    return ''


def _clean_text(value):
    value = html.unescape(value or '')
    value = _TAG_RE.sub(' ', value)
    return _WHITESPACE_RE.sub(' ', value).strip()


def _parse_timestamp(value):
    value = (value or '').strip()
    if not value:
        return None
    try:
        return int(parsedate_to_datetime(value).timestamp() * 1000)
    except (TypeError, ValueError, OverflowError):
        pass
    try:
        from datetime import datetime
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        return int(parsed.timestamp() * 1000)
    except (TypeError, ValueError, OverflowError):
        return None


def _item_link(item):
    link = _child_text(item, 'link')
    if link:
        return link
    for child in list(item):
        if _local_name(child.tag) == 'link' and child.attrib.get('href'):
            return child.attrib['href']
    return ''


def parse_rss_document(payload):
    """Parse RSS/Atom XML into a small, stable internal representation."""
    root = ElementTree.fromstring(payload)
    channel = next((node for node in root.iter() if _local_name(node.tag) == 'channel'), None)
    if channel is None and _local_name(root.tag) == 'feed':
        channel = root
    entries = [node for node in root.iter() if _local_name(node.tag) in {'item', 'entry'}]
    feed_title = _child_text(channel, 'title') if channel is not None else ''
    feed_link = _child_text(channel, 'link') if channel is not None else ''
    items = []
    for item in entries:
        link = _item_link(item)
        guid = _child_text(item, 'guid', 'id') or link
        title = _clean_text(_child_text(item, 'title')) or link or 'Untitled article'
        summary_raw = _child_text(item, 'description', 'summary', 'subtitle')
        content_raw = _child_text(item, 'encoded', 'content', 'content:encoded')
        summary = _clean_text(summary_raw)
        content = _clean_text(content_raw or summary_raw)
        published = _parse_timestamp(_child_text(item, 'pubdate', 'published', 'updated', 'date'))
        author = _clean_text(_child_text(item, 'author', 'creator'))
        if not guid or not link:
            continue
        items.append({
            'guid': guid[:512],
            'title': title[:1000],
            'link': link[:2000],
            'author': author[:255] or None,
            'summary': summary,
            'content': content,
            'published_at': published,
        })
    return {'title': feed_title[:255] or None, 'link': feed_link[:1000] or None, 'items': items}


def _request_options():
    options = {
        'timeout': max(1, int(config.RSS_REQUEST_TIMEOUT_SECONDS)),
        'headers': {'User-Agent': config.RSS_USER_AGENT},
    }
    if config.USE_PROXY:
        options['proxies'] = {'http': config.PROXY_URL, 'https': config.HTTPS_PROXY_URL}
    return options


def _fetch_url(url):
    """Route RSS sources through the optional HTTP reverse-proxy base URL."""
    base_url = (config.RSS_PROXY_BASE_URL or '').strip().rstrip('/')
    if not base_url:
        return url
    parsed = urlsplit(url)
    if parsed.scheme not in {'http', 'https'} or not parsed.netloc:
        return url
    path = f'/{parsed.netloc}{parsed.path or "/"}'
    return urlunsplit(('', '', f'{base_url}{path}', parsed.query, ''))


def fetch_feed(url):
    # Windows can expose an HTTPS-form local proxy through the system registry.
    # Use only CoinX's explicit proxy settings so requests does not negotiate TLS
    # with a plain HTTP proxy listener.
    session = requests.Session()
    session.trust_env = False
    response = session.get(_fetch_url(url), **_request_options())
    response.raise_for_status()
    if not response.content:
        raise ValueError('RSS response is empty')
    content_type = response.headers.get('Content-Type', '').split(';', 1)[0].strip().lower()
    if content_type in {'text/html', 'application/xhtml+xml'}:
        raise ValueError(f'RSS source returned HTML instead of XML (content-type: {content_type})')
    return parse_rss_document(response.content)


def serialize_subscription(row, article_count=None):
    return {
        'id': row.id,
        'name': row.name,
        'url': row.url,
        'site_url': row.site_url,
        'feed_title': row.feed_title,
        'enabled': bool(row.enabled),
        'monitor_enabled': bool(row.monitor_enabled),
        'notification_channel_ids': row.notification_channel_ids,
        'last_checked_at': row.last_checked_at,
        'last_success_at': row.last_success_at,
        'last_error': row.last_error,
        'article_count': article_count,
        'created_at': row.created_at.isoformat() if row.created_at else None,
        'updated_at': row.updated_at.isoformat() if row.updated_at else None,
    }


def serialize_article(row, subscription=None):
    return {
        'id': row.id,
        'subscription_id': row.subscription_id,
        'subscription_name': subscription.name if subscription else None,
        'guid': row.guid,
        'title': row.title,
        'link': row.link,
        'author': row.author,
        'summary': row.summary,
        'content': row.content,
        'published_at': row.published_at,
        'notified_at': row.notified_at,
        'fetched_at': row.fetched_at,
    }


def _notify_articles(db, subscription, articles):
    """Send one grouped message to each enabled configured notification channel."""
    if not articles or not config.NOTIFICATIONS_ENABLED:
        return {'channels': 0, 'sent': 0}
    from coinx.notifications import _delivery

    channels = db.query(NotificationChannel).filter(NotificationChannel.enabled.is_(True))
    channel_ids = subscription.notification_channel_ids
    if channel_ids is not None:
        channels = channels.filter(NotificationChannel.id.in_(channel_ids)) if channel_ids else channels.filter(False)
    channels = channels.all()
    if not channels:
        return {'channels': 0, 'sent': 0}
    lines = [f'订阅源：{subscription.name}', f'新增文章：{len(articles)} 条', '']
    for article in articles[:10]:
        lines.append(f'• {article.title}')
        lines.append(article.link)
    if len(articles) > 10:
        lines.append(f'其余 {len(articles) - 10} 条请在 RSS 页面查看。')
    body = '\n'.join(lines)
    event_key = f'rss:{subscription.id}:' + ','.join(str(article.id) for article in articles)
    sent = 0
    for channel in channels:
        delivery = _delivery(
            db, None, channel, event_key, 'new_articles',
            {'subscription_id': subscription.id, 'article_ids': [article.id for article in articles]},
            f'RSS 更新 · {subscription.name}', body,
        )
        if delivery.delivery_status == 'success':
            sent += 1
    if sent:
        for article in articles:
            article.notified_at = now_ms()
    return {'channels': len(channels), 'sent': sent}


def sync_subscription(db, subscription, notify=True):
    if not subscription.enabled:
        return {'status': 'disabled', 'subscription_id': subscription.id, 'new_count': 0}
    checked_at = now_ms()
    subscription.last_checked_at = checked_at
    try:
        feed = fetch_feed(subscription.url)
        subscription.feed_title = feed.get('title') or subscription.feed_title
        subscription.site_url = feed.get('link') or subscription.site_url
        existing = {
            row.guid for row in db.query(RssArticle.guid).filter(
                RssArticle.subscription_id == subscription.id,
            ).all()
        }
        new_articles = []
        for item in feed['items']:
            if item['guid'] in existing:
                continue
            row = RssArticle(
                subscription_id=subscription.id,
                guid=item['guid'],
                title=item['title'],
                link=item['link'],
                author=item['author'],
                summary=item['summary'],
                content=item['content'],
                published_at=item['published_at'],
                fetched_at=checked_at,
            )
            db.add(row)
            new_articles.append(row)
            existing.add(item['guid'])
        db.flush()
        subscription.last_success_at = checked_at
        subscription.last_error = None
        notification = {'channels': 0, 'sent': 0}
        if notify and subscription.monitor_enabled and new_articles:
            notification = _notify_articles(db, subscription, new_articles)
        db.commit()
        return {
            'status': 'success',
            'subscription_id': subscription.id,
            'new_count': len(new_articles),
            'notification': notification,
        }
    except Exception as exc:
        db.rollback()
        subscription = db.get(RssSubscription, subscription.id)
        subscription.last_checked_at = checked_at
        subscription.last_error = str(exc)[:500]
        db.commit()
        logger.warning('RSS 抓取失败: subscription=%s error=%s', subscription.id, exc)
        return {
            'status': 'error',
            'subscription_id': subscription.id,
            'new_count': 0,
            'error': str(exc),
        }


def monitor_all_subscriptions():
    ensure_rss_schema()
    db = get_session()
    try:
        subscription_ids = [
            row.id for row in db.query(RssSubscription.id).filter(
                RssSubscription.enabled.is_(True),
            ).all()
        ]
    finally:
        db.close()

    summaries = []
    # An independent session contains transaction failures from one source.
    session_factory = sessionmaker(bind=engine)
    for subscription_id in subscription_ids:
        db = session_factory()
        try:
            subscription = db.get(RssSubscription, subscription_id)
            if subscription:
                result = sync_subscription(db, subscription, notify=True)
                result['subscription_name'] = subscription.name
                summaries.append(result)
        except Exception as exc:
            db.rollback()
            logger.exception('RSS 订阅同步异常: subscription=%s', subscription_id)
            summaries.append({
                'status': 'error',
                'subscription_id': subscription_id,
                'subscription_name': None,
                'new_count': 0,
                'error': str(exc),
            })
        finally:
            db.close()

    failure_count = sum(item['status'] == 'error' for item in summaries)
    success_count = len(summaries) - failure_count
    return {
        'status': 'error' if failure_count and not success_count else ('partial' if failure_count else 'success'),
        'subscription_count': len(subscription_ids),
        'success_count': success_count,
        'failure_count': failure_count,
        'new_count': sum(item.get('new_count', 0) for item in summaries),
        'summaries': summaries,
    }


def list_articles(db, subscription_id=None, limit=50, offset=0):
    query = db.query(RssArticle, RssSubscription).join(
        RssSubscription, RssSubscription.id == RssArticle.subscription_id,
    )
    if subscription_id:
        query = query.filter(RssArticle.subscription_id == subscription_id)
    total = query.count()
    rows = query.order_by(desc(RssArticle.published_at), desc(RssArticle.id)).offset(offset).limit(limit).all()
    return total, [serialize_article(article, subscription) for article, subscription in rows]
