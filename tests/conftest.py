import os
import sys
import tempfile
import threading
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from coinx.database import Base
from coinx.models import (
    AlertEvaluationRun,
    AlertEvaluationMetric,
    AlertRule,
    AlertRuleChannel,
    AlertState,
    MarketFundingRate,
    MarketKline,
    MarketOpenInterestHist,
    MarketTickers,
    MarketTakerBuySellVol,
    NotificationChannel,
    NotificationDelivery,
    ScheduledJobRun,
)

TEST_TABLES = [
    MarketFundingRate.__table__,
    MarketOpenInterestHist.__table__,
    MarketKline.__table__,
    MarketTakerBuySellVol.__table__,
    MarketTickers.__table__,
    NotificationChannel.__table__,
    AlertRule.__table__,
    AlertRuleChannel.__table__,
    AlertState.__table__,
    NotificationDelivery.__table__,
    AlertEvaluationRun.__table__,
    AlertEvaluationMetric.__table__,
    ScheduledJobRun.__table__,
]


@pytest.fixture()
def test_db():
    """创建一个共享的测试数据库（临时文件，所有连接共享）"""
    tmp = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
    tmp.close()
    db_path = tmp.name
    engine = create_engine(f"sqlite:///{db_path}", connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=engine, tables=TEST_TABLES)
    try:
        yield engine
    finally:
        engine.dispose()
        os.unlink(db_path)


@pytest.fixture()
def db_session(test_db):
    """测试会话"""
    maker = sessionmaker(bind=test_db)
    session = maker()
    try:
        yield session
    finally:
        session.close()


@pytest.fixture(autouse=True)
def fresh_market_structure_lock(monkeypatch):
    """每个测试用全新的 MARKET_STRUCTURE_REFRESH_LOCK，避免 daemon 线程持锁泄漏到下一个测试"""
    monkeypatch.setattr('coinx.web.routes.api_data.MARKET_STRUCTURE_REFRESH_LOCK', threading.Lock())


@pytest.fixture(autouse=True)
def allow_manual_collection_routes_in_tests(monkeypatch):
    """Keep legacy route tests explicit while production may be scheduler-only."""
    for module in (
        'coinx.web.routes.api_data',
        'coinx.web.routes.api_funding_rate',
        'coinx.web.routes.api_config',
        'coinx.web.routes.api_rss',
    ):
        monkeypatch.setattr(f'{module}.COLLECTION_SCHEDULER_ONLY', False, raising=False)


@pytest.fixture(autouse=True)
def patch_get_session(monkeypatch, test_db):
    """将所有 get_session() 调用重定向到测试数据库"""
    maker = sessionmaker(bind=test_db)
    monkeypatch.setattr('coinx.database.get_session', maker)


@pytest.fixture(autouse=True)
def use_mysql_market_writer_for_unit_tests(monkeypatch):
    """Keep existing ORM tests isolated from the production CK write switch."""
    monkeypatch.setattr('coinx.config.MARKET_WRITE_BACKEND', 'mysql')
