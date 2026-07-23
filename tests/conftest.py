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
def patch_get_session(monkeypatch, test_db):
    """将所有 get_session() 调用重定向到测试数据库"""
    maker = sessionmaker(bind=test_db)
    monkeypatch.setattr('coinx.database.get_session', maker)
