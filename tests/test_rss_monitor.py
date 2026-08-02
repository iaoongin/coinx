from cryptography.fernet import Fernet

from coinx import config, notifications
from coinx.models import NotificationChannel, RssArticle, RssSubscription
from coinx.rss_monitor import parse_rss_document, sync_subscription


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
