from cryptography.fernet import Fernet
import pytest
from sqlalchemy.orm import sessionmaker

from coinx import config, notifications
from coinx.models import NotificationChannel, RssArticle, RssSubscription
from coinx.rss_monitor import fetch_feed, monitor_all_subscriptions, parse_rss_document, sync_subscription
from coinx.scheduler import _rss_summary_error


def test_parse_rss_document_extracts_items_and_cleans_html():
    payload = b'''<rss><channel><title>Example feed</title><item>
      <guid>article-1</guid><title> Hello </title>
      <link>https://example.test/1</link>
      <description><![CDATA[<p>Summary <b>text</b></p>]]></description>
      <pubDate>Fri, 31 Jul 2026 15:27:16 GMT</pubDate>
    </item></channel></rss>'''

    result = parse_rss_document(payload)

    assert result['title'] == 'Example feed'
    assert result['items'][0]['guid'] == 'article-1'
    assert result['items'][0]['summary'] == 'Summary text'
    assert result['items'][0]['published_at'] == 1785511636000


def test_fetch_feed_reports_html_response_clearly(monkeypatch):
    class Response:
        content = b'<!DOCTYPE html><html><body>X profile</body></html>'
        headers = {'Content-Type': 'text/html; charset=utf-8'}

        def raise_for_status(self):
            pass

    class Session:
        trust_env = False

        def get(self, *_args, **_kwargs):
            return Response()

    monkeypatch.setattr('coinx.rss_monitor.requests.Session', Session)

    with pytest.raises(ValueError, match='returned HTML instead of XML'):
        fetch_feed('https://x.com/example/rss')


def test_rss_summary_error_identifies_failed_subscription():
    error = _rss_summary_error({
        'summaries': [
            {'status': 'success', 'subscription_name': 'Working feed'},
            {
                'status': 'error',
                'subscription_id': 4,
                'subscription_name': 'TALK君',
                'error': 'RSS source returned HTML instead of XML',
            },
        ],
    })

    assert error == 'TALK君: RSS source returned HTML instead of XML'


def test_monitor_continues_after_one_subscription_fails(test_db, db_session, monkeypatch):
    RssSubscription.__table__.create(bind=test_db, checkfirst=True)
    RssArticle.__table__.create(bind=test_db, checkfirst=True)
    db_session.add_all([
        RssSubscription(name='Broken feed', url='https://example.test/broken'),
        RssSubscription(name='Working feed', url='https://example.test/working'),
    ])
    db_session.commit()
    monkeypatch.setattr(config, 'NOTIFICATIONS_ENABLED', False)
    monkeypatch.setattr('coinx.rss_monitor.engine', test_db)
    monkeypatch.setattr('coinx.rss_monitor.get_session', sessionmaker(bind=test_db))

    def fetch(url):
        if url.endswith('/broken'):
            raise ValueError('RSS source returned HTML instead of XML')
        return {
            'title': 'Working feed',
            'link': 'https://example.test',
            'items': [{
                'guid': 'article-1',
                'title': 'New article',
                'link': 'https://example.test/1',
                'author': None,
                'summary': 'Summary',
                'content': 'Summary',
                'published_at': 100,
            }],
        }

    monkeypatch.setattr('coinx.rss_monitor.fetch_feed', fetch)

    summary = monitor_all_subscriptions()

    assert summary['status'] == 'partial'
    assert summary['success_count'] == 1
    assert summary['failure_count'] == 1
    assert summary['new_count'] == 1
    assert summary['summaries'][0]['subscription_name'] == 'Broken feed'
    assert summary['summaries'][1]['subscription_name'] == 'Working feed'
    assert db_session.query(RssArticle).count() == 1


def test_sync_subscription_deduplicates_and_delivers_new_articles(test_db, db_session, monkeypatch):
    RssSubscription.__table__.create(bind=test_db, checkfirst=True)
    RssArticle.__table__.create(bind=test_db, checkfirst=True)
    monkeypatch.setattr(config, 'NOTIFICATIONS_ENABLED', True)
    monkeypatch.setattr(config, 'NOTIFICATION_ENCRYPTION_KEY', Fernet.generate_key().decode())
    monkeypatch.setattr(
        notifications,
        'send_apprise',
        lambda *_args, **_kwargs: True,
    )
    channel = NotificationChannel(
        name='bark',
        channel_type='apprise',
        config_encrypted=notifications.encrypt_apprise_url('barks://test-key'),
        key_version='v1',
        enabled=True,
    )
    subscription = RssSubscription(name='Example', url='https://example.test/rss', monitor_enabled=True)
    db_session.add_all([channel, subscription])
    db_session.commit()
    monkeypatch.setattr(
        'coinx.rss_monitor.fetch_feed',
        lambda _url: {
            'title': 'Example',
            'link': 'https://example.test',
            'items': [{
                'guid': 'article-1',
                'title': 'New article',
                'link': 'https://example.test/1',
                'author': None,
                'summary': 'Summary',
                'content': 'Summary',
                'published_at': 100,
            }],
        },
    )

    first = sync_subscription(db_session, subscription)
    second = sync_subscription(db_session, subscription)

    assert first['new_count'] == 1
    assert first['notification']['sent'] == 1
    assert second['new_count'] == 0
    assert db_session.query(RssArticle).count() == 1


def test_sync_subscription_delivers_only_to_selected_channels(test_db, db_session, monkeypatch):
    RssSubscription.__table__.create(bind=test_db, checkfirst=True)
    RssArticle.__table__.create(bind=test_db, checkfirst=True)
    monkeypatch.setattr(config, 'NOTIFICATIONS_ENABLED', True)
    monkeypatch.setattr(config, 'NOTIFICATION_ENCRYPTION_KEY', Fernet.generate_key().decode())
    monkeypatch.setattr(notifications, 'send_apprise', lambda *_args, **_kwargs: True)
    bark = NotificationChannel(name='bark', channel_type='apprise', config_encrypted=notifications.encrypt_apprise_url('barks://test-key'), key_version='v1', enabled=True)
    telegram = NotificationChannel(name='telegram', channel_type='apprise', config_encrypted=notifications.encrypt_apprise_url('tgram://token/chat'), key_version='v1', enabled=True)
    db_session.add_all([bark, telegram])
    db_session.flush()
    subscription = RssSubscription(name='Example', url='https://example.test/rss', monitor_enabled=True, notification_channel_ids=[bark.id])
    db_session.add(subscription)
    db_session.commit()
    monkeypatch.setattr('coinx.rss_monitor.fetch_feed', lambda _url: {'title': 'Example', 'link': 'https://example.test', 'items': [{'guid': 'article-1', 'title': 'New article', 'link': 'https://example.test/1', 'author': None, 'summary': 'Summary', 'content': 'Summary', 'published_at': 100}]})

    result = sync_subscription(db_session, subscription)

    assert result['notification'] == {'channels': 1, 'sent': 1}
