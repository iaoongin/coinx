import sys
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from coinx.database import Base
from coinx.models import (
    BinanceTopLongShortPositionRatio,
    BinanceTopLongShortAccountRatio,
    BinanceOpenInterestHist,
    BinanceKline,
    BinanceGlobalLongShortAccountRatio,
    MarketTickers,
)


@pytest.fixture()
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(
        bind=engine,
        tables=[
            BinanceTopLongShortPositionRatio.__table__,
            BinanceTopLongShortAccountRatio.__table__,
            BinanceOpenInterestHist.__table__,
            BinanceKline.__table__,
            BinanceGlobalLongShortAccountRatio.__table__,
            MarketTickers.__table__,
        ],
    )
    session = sessionmaker(bind=engine)()
    try:
        yield session
    finally:
        session.close()
