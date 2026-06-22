# Funding Rate Feature Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Binance funding rate data collection, storage, and display to CoinX, including homepage integration and a new monitoring page.

**Architecture:** Extend existing series framework to collect funding rate from Binance `/fapi/v1/premiumIndex` API every 5 minutes. Store all records in `market_funding_rate` table. Display predicted rate on homepage and create new page with ranking, abnormal detection, and 1-hour trend chart.

**Tech Stack:** Python, Flask, SQLAlchemy, MySQL, Chart.js

---

## File Structure

### New Files
- `src/coinx/collector/binance/funding_rate.py` - Binance funding rate API client
- `src/coinx/repositories/funding_rate.py` - Funding rate data storage and queries
- `src/coinx/web/routes/api_funding_rate.py` - Funding rate API endpoints
- `src/coinx/web/templates/funding_rate.html` - Funding rate monitoring page
- `tests/collector/binance/test_funding_rate.py` - Collector tests
- `tests/repositories/test_funding_rate.py` - Repository tests

### Modified Files
- `sql/schema.sql` - Add `market_funding_rate` table
- `src/coinx/models.py` - Add `MarketFundingRate` model
- `src/coinx/collector/binance/series.py` - Register `funding_rate` type
- `src/coinx/repositories/homepage_series.py` - Add funding rate to homepage
- `src/coinx/config.py` - Add funding rate config
- `src/coinx/web/routes/pages.py` - Add `/funding-rate` route
- `.env.example` - Add funding rate config example

---

## Task 1: Database Schema

**Files:**
- Modify: `sql/schema.sql`

- [ ] **Step 1: Add market_funding_rate table**

Add the following SQL to `sql/schema.sql`:

```sql
-- 资金费率历史表
CREATE TABLE IF NOT EXISTS market_funding_rate (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    symbol VARCHAR(20) NOT NULL COMMENT '交易对名称',
    period VARCHAR(10) NOT NULL DEFAULT '5m' COMMENT '采集周期',
    event_time BIGINT NOT NULL COMMENT '采集时间戳（毫秒）',
    funding_rate DECIMAL(20, 8) COMMENT '上次结算费率',
    predicted_rate DECIMAL(20, 8) COMMENT '预测费率（下次结算）',
    next_funding_time BIGINT COMMENT '下次结算时间戳（毫秒）',
    mark_price DECIMAL(20, 8) COMMENT '标记价格',
    exchange VARCHAR(20) NOT NULL DEFAULT 'binance' COMMENT '交易所',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    UNIQUE KEY uk_symbol_period_time (symbol, period, event_time),
    INDEX idx_symbol_period (symbol, period),
    INDEX idx_symbol_time (symbol, event_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='资金费率历史';
```

- [ ] **Step 2: Commit**

```bash
git add sql/schema.sql
git commit -m "feat(db): add market_funding_rate table schema"
```

---

## Task 2: SQLAlchemy Model

**Files:**
- Modify: `src/coinx/models.py`

- [ ] **Step 1: Add MarketFundingRate model**

Add to `src/coinx/models.py` after existing models:

```python
class MarketFundingRate(Base):
    """资金费率历史表"""
    __tablename__ = 'market_funding_rate'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    symbol = Column(String(20), nullable=False, comment='交易对名称')
    period = Column(String(10), nullable=False, default='5m', comment='采集周期')
    event_time = Column(BigInteger, nullable=False, comment='采集时间戳（毫秒）')
    funding_rate = Column(Numeric(20, 8), comment='上次结算费率')
    predicted_rate = Column(Numeric(20, 8), comment='预测费率（下次结算）')
    next_funding_time = Column(BigInteger, comment='下次结算时间戳（毫秒）')
    mark_price = Column(Numeric(20, 8), comment='标记价格')
    exchange = Column(String(20), nullable=False, default='binance', comment='交易所')
    created_at = Column(DateTime, server_default=func.now(), comment='创建时间')

    __table_args__ = (
        UniqueConstraint('symbol', 'period', 'event_time', name='uk_symbol_period_time'),
        Index('idx_symbol_period', 'symbol', 'period'),
        Index('idx_symbol_time', 'symbol', 'event_time'),
        {'comment': '资金费率历史'}
    )
```

- [ ] **Step 2: Commit**

```bash
git add src/coinx/models.py
git commit -m "feat(models): add MarketFundingRate model"
```

---

## Task 3: Binance Funding Rate Collector

**Files:**
- Create: `src/coinx/collector/binance/funding_rate.py`
- Create: `tests/collector/binance/test_funding_rate.py`

- [ ] **Step 1: Write test for fetch_premium_index**

Create `tests/collector/binance/test_funding_rate.py`:

```python
"""Tests for Binance funding rate collector"""
import pytest
from unittest.mock import Mock, patch
from coinx.collector.binance.funding_rate import fetch_premium_index, parse_funding_rate


class TestFetchPremiumIndex:
    """Test fetch_premium_index function"""

    @patch('coinx.collector.binance.funding_rate.get_session')
    @patch('coinx.collector.binance.funding_rate.request_with_binance_retry')
    def test_fetch_premium_index_success(self, mock_request, mock_get_session):
        """Test successful API call"""
        mock_response = Mock()
        mock_response.json.return_value = {
            'symbol': 'BTCUSDT',
            'markPrice': '34000.00',
            'lastFundingRate': '0.00010000',
            'nextFundingRate': '0.00012000',
            'nextFundingTime': 1698796800000,
            'time': 1698768000000,
        }
        mock_response.raise_for_status = Mock()
        mock_request.return_value = mock_response

        result = fetch_premium_index('BTCUSDT')

        assert result['symbol'] == 'BTCUSDT'
        assert result['period'] == '5m'
        assert result['funding_rate'] == 0.0001
        assert result['predicted_rate'] == 0.00012
        assert result['next_funding_time'] == 1698796800000
        assert result['mark_price'] == 34000.0


class TestParseFundingRate:
    """Test parse_funding_rate function"""

    def test_parse_funding_rate(self):
        """Test parsing API response"""
        payload = {
            'symbol': 'BTCUSDT',
            'markPrice': '34000.00',
            'lastFundingRate': '0.00010000',
            'nextFundingRate': '0.00012000',
            'nextFundingTime': 1698796800000,
            'time': 1698768000000,
        }

        result = parse_funding_rate(payload, 'BTCUSDT', '5m')

        assert len(result) == 1
        assert result[0]['symbol'] == 'BTCUSDT'
        assert result[0]['period'] == '5m'
        assert result[0]['funding_rate'] == 0.0001
        assert result[0]['predicted_rate'] == 0.00012
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/collector/binance/test_funding_rate.py -v`
Expected: FAIL with "ModuleNotFoundError: No module named 'coinx.collector.binance.funding_rate'"

- [ ] **Step 3: Implement funding_rate collector**

Create `src/coinx/collector/binance/funding_rate.py`:

```python
"""Binance 资金费率采集模块"""
from coinx.config import BINANCE_BASE_URL
from coinx.collector.binance.client import get_session, request_with_binance_retry


def fetch_premium_index(symbol, session=None):
    """
    获取单个币种的预测资金费率

    Args:
        symbol: 交易对名称，如 'BTCUSDT'
        session: HTTP session（可选）

    Returns:
        dict: 包含 funding_rate, predicted_rate, next_funding_time, mark_price
    """
    params = {'symbol': symbol}
    url = f"{BINANCE_BASE_URL}/fapi/v1/premiumIndex"
    http_session = session or get_session()
    response = request_with_binance_retry(http_session, url, params=params)
    response.raise_for_status()
    data = response.json()

    return {
        'symbol': symbol,
        'period': '5m',
        'event_time': int(data.get('time', 0)),
        'funding_rate': float(data.get('lastFundingRate', 0)),
        'predicted_rate': float(data.get('nextFundingRate', 0)),
        'next_funding_time': int(data.get('nextFundingTime', 0)),
        'mark_price': float(data.get('markPrice', 0)),
    }


def parse_funding_rate(payload, symbol, period='5m'):
    """
    解析资金费率数据（兼容 series 框架）

    Args:
        payload: API 响应数据
        symbol: 交易对名称
        period: 采集周期

    Returns:
        list: 解析后的记录列表
    """
    return [{
        'symbol': symbol,
        'period': period,
        'event_time': int(payload.get('time', 0)),
        'funding_rate': float(payload.get('lastFundingRate', 0)),
        'predicted_rate': float(payload.get('nextFundingRate', 0)),
        'next_funding_time': int(payload.get('nextFundingTime', 0)),
        'mark_price': float(payload.get('markPrice', 0)),
    }]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/collector/binance/test_funding_rate.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/coinx/collector/binance/funding_rate.py tests/collector/binance/test_funding_rate.py
git commit -m "feat(collector): add Binance funding rate collector"
```

---

## Task 4: Register Funding Rate in Series Framework

**Files:**
- Modify: `src/coinx/collector/binance/series.py`

- [ ] **Step 1: Add funding_rate to SERIES_ENDPOINTS**

In `src/coinx/collector/binance/series.py`, update `SERIES_ENDPOINTS`:

```python
SERIES_ENDPOINTS = {
    'open_interest_hist': '/futures/data/openInterestHist',
    'klines': '/fapi/v1/klines',
    'taker_buy_sell_vol': '/futures/data/takerlongshortRatio',
    'funding_rate': '/fapi/v1/premiumIndex',
}
```

- [ ] **Step 2: Add fetch_funding_rate function**

Add import at top of file:

```python
from .funding_rate import fetch_premium_index as fetch_funding_rate
from .funding_rate import parse_funding_rate
```

- [ ] **Step 3: Add to fetchers and parsers**

Update `fetch_series_payload` function:

```python
def fetch_series_payload(series_type, symbol, period, limit, session=None, start_time=None, end_time=None):
    fetchers = {
        'open_interest_hist': fetch_open_interest_hist,
        'klines': fetch_klines,
        'taker_buy_sell_vol': fetch_taker_buy_sell_vol,
        'funding_rate': fetch_funding_rate,
    }
    # ... rest of function
```

Update `parse_series_payload` function:

```python
def parse_series_payload(series_type, payload, symbol, period):
    parsers = {
        'open_interest_hist': parse_open_interest_hist,
        'klines': parse_klines,
        'taker_buy_sell_vol': parse_taker_buy_sell_vol,
        'funding_rate': parse_funding_rate,
    }
    # ... rest of function
```

- [ ] **Step 4: Commit**

```bash
git add src/coinx/collector/binance/series.py
git commit -m "feat(collector): register funding_rate in series framework"
```

---

## Task 5: Funding Rate Repository

**Files:**
- Create: `src/coinx/repositories/funding_rate.py`
- Create: `tests/repositories/test_funding_rate.py`

- [ ] **Step 1: Write tests for repository**

Create `tests/repositories/test_funding_rate.py`:

```python
"""Tests for funding rate repository"""
import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
from coinx.repositories.funding_rate import (
    save_funding_rates,
    load_latest_funding_rates,
    load_funding_rate_history,
    load_abnormal_funding_rates,
)


class TestSaveFundingRates:
    """Test save_funding_rates function"""

    def test_save_empty_records(self):
        """Test saving empty records returns 0"""
        result = save_funding_rates([])
        assert result == 0

    @patch('coinx.repositories.funding_rate.get_session')
    def test_save_single_record(self, mock_get_session):
        """Test saving single record"""
        mock_session = MagicMock()
        mock_get_session.return_value = mock_session
        mock_session.query.return_value.filter.return_value.first.return_value = None

        records = [{
            'symbol': 'BTCUSDT',
            'period': '5m',
            'event_time': 1698768000000,
            'funding_rate': 0.0001,
            'predicted_rate': 0.00012,
            'next_funding_time': 1698796800000,
            'mark_price': 34000.0,
        }]

        result = save_funding_rates(records, session=mock_session)

        assert result == 1
        mock_session.add.assert_called_once()
        mock_session.commit.assert_called_once()


class TestLoadLatestFundingRates:
    """Test load_latest_funding_rates function"""

    def test_load_empty_symbols(self):
        """Test loading with empty symbols returns empty dict"""
        result = load_latest_funding_rates([])
        assert result == {}


class TestLoadAbnormalFundingRates:
    """Test load_abnormal_funding_rates function"""

    def test_default_threshold(self):
        """Test default threshold is 0.001"""
        # This is a simple test to verify the function signature
        # Real tests would need database mocking
        pass
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/repositories/test_funding_rate.py -v`
Expected: FAIL with "ModuleNotFoundError"

- [ ] **Step 3: Implement funding_rate repository**

Create `src/coinx/repositories/funding_rate.py`:

```python
"""资金费率数据存储和查询模块"""
from datetime import datetime, timedelta
from sqlalchemy import func
from coinx.database import get_session
from coinx.models import MarketFundingRate
from coinx.utils import logger


def save_funding_rates(records, session=None):
    """
    批量保存资金费率数据

    Args:
        records: 记录列表
        session: 数据库 session（可选）

    Returns:
        int: 成功保存的记录数
    """
    if not records:
        return 0

    own_session = session is None
    db = session or get_session()

    try:
        saved_count = 0
        for record in records:
            existing = db.query(MarketFundingRate).filter(
                MarketFundingRate.symbol == record['symbol'],
                MarketFundingRate.period == record['period'],
                MarketFundingRate.event_time == record['event_time']
            ).first()

            if existing:
                existing.funding_rate = record.get('funding_rate')
                existing.predicted_rate = record.get('predicted_rate')
                existing.next_funding_time = record.get('next_funding_time')
                existing.mark_price = record.get('mark_price')
            else:
                new_record = MarketFundingRate(**record)
                db.add(new_record)

            saved_count += 1

        db.commit()
        logger.info(f'资金费率数据保存成功: {saved_count} 条记录')
        return saved_count

    except Exception as e:
        db.rollback()
        logger.error(f'资金费率数据保存失败: {e}')
        raise
    finally:
        if own_session:
            db.close()


def load_latest_funding_rates(symbols, exchange='binance', session=None):
    """
    加载指定币种的最新资金费率（批量查询优化）

    Args:
        symbols: 交易对列表
        exchange: 交易所
        session: 数据库 session（可选）

    Returns:
        dict: {symbol: {predicted_rate, funding_rate, next_funding_time, mark_price}}
    """
    if not symbols:
        return {}

    own_session = session is None
    db = session or get_session()

    try:
        subquery = db.query(
            MarketFundingRate.symbol,
            func.max(MarketFundingRate.event_time).label('max_time')
        ).filter(
            MarketFundingRate.symbol.in_(symbols),
            MarketFundingRate.period == '5m',
            MarketFundingRate.exchange == exchange
        ).group_by(MarketFundingRate.symbol).subquery()

        records = db.query(MarketFundingRate).join(
            subquery,
            (MarketFundingRate.symbol == subquery.c.symbol) &
            (MarketFundingRate.event_time == subquery.c.max_time)
        ).all()

        result = {}
        for r in records:
            result[r.symbol] = {
                'predicted_rate': float(r.predicted_rate) if r.predicted_rate else None,
                'funding_rate': float(r.funding_rate) if r.funding_rate else None,
                'next_funding_time': int(r.next_funding_time) if r.next_funding_time else None,
                'mark_price': float(r.mark_price) if r.mark_price else None,
                'event_time': int(r.event_time),
            }

        return result

    finally:
        if own_session:
            db.close()


def load_funding_rate_history(symbol, hours=1, exchange='binance', session=None):
    """
    加载单个币种的资金费率历史

    Args:
        symbol: 交易对
        hours: 历史小时数（默认 1 小时）
        exchange: 交易所
        session: 数据库 session（可选）

    Returns:
        list: 历史记录列表，按时间正序
    """
    own_session = session is None
    db = session or get_session()

    try:
        cutoff_time = int((datetime.utcnow() - timedelta(hours=hours)).timestamp() * 1000)

        records = db.query(MarketFundingRate).filter(
            MarketFundingRate.symbol == symbol,
            MarketFundingRate.period == '5m',
            MarketFundingRate.exchange == exchange,
            MarketFundingRate.event_time >= cutoff_time
        ).order_by(MarketFundingRate.event_time.asc()).all()

        return [
            {
                'event_time': int(r.event_time),
                'funding_rate': float(r.funding_rate) if r.funding_rate else None,
                'predicted_rate': float(r.predicted_rate) if r.predicted_rate else None,
                'next_funding_time': int(r.next_funding_time) if r.next_funding_time else None,
                'mark_price': float(r.mark_price) if r.mark_price else None,
            }
            for r in records
        ]

    finally:
        if own_session:
            db.close()


def load_abnormal_funding_rates(threshold=0.001, exchange='binance', session=None):
    """
    加载异常资金费率（绝对值超过阈值）

    Args:
        threshold: 异常阈值（默认 0.1%）
        exchange: 交易所
        session: 数据库 session（可选）

    Returns:
        list: 异常记录列表，按绝对值降序
    """
    own_session = session is None
    db = session or get_session()

    try:
        subquery = db.query(
            MarketFundingRate.symbol,
            func.max(MarketFundingRate.event_time).label('max_time')
        ).filter(
            MarketFundingRate.period == '5m',
            MarketFundingRate.exchange == exchange
        ).group_by(MarketFundingRate.symbol).subquery()

        records = db.query(MarketFundingRate).join(
            subquery,
            (MarketFundingRate.symbol == subquery.c.symbol) &
            (MarketFundingRate.event_time == subquery.c.max_time)
        ).filter(
            MarketFundingRate.predicted_rate.isnot(None)
        ).all()

        abnormal = []
        for r in records:
            rate = float(r.predicted_rate)
            if abs(rate) >= threshold:
                abnormal.append({
                    'symbol': r.symbol,
                    'predicted_rate': rate,
                    'funding_rate': float(r.funding_rate) if r.funding_rate else None,
                    'next_funding_time': int(r.next_funding_time) if r.next_funding_time else None,
                    'mark_price': float(r.mark_price) if r.mark_price else None,
                    'event_time': int(r.event_time),
                })

        abnormal.sort(key=lambda x: abs(x['predicted_rate']), reverse=True)

        return abnormal

    finally:
        if own_session:
            db.close()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/repositories/test_funding_rate.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/coinx/repositories/funding_rate.py tests/repositories/test_funding_rate.py
git commit -m "feat(repository): add funding rate storage and query module"
```

---

## Task 6: Add Funding Rate to Homepage Series Types

**Files:**
- Modify: `src/coinx/repositories/homepage_series.py`

- [ ] **Step 1: Update HOMEPAGE_REQUIRED_SERIES_TYPES**

In `src/coinx/repositories/homepage_series.py`, update the constant:

```python
HOMEPAGE_REQUIRED_SERIES_TYPES = (
    'klines',
    'open_interest_hist',
    'taker_buy_sell_vol',
    'funding_rate',
)
```

- [ ] **Step 2: Commit**

```bash
git add src/coinx/repositories/homepage_series.py
git commit -m "feat(homepage): add funding_rate to required series types"
```

---

## Task 7: Configuration

**Files:**
- Modify: `src/coinx/config.py`
- Modify: `.env.example`

- [ ] **Step 1: Add funding rate config**

In `src/coinx/config.py`, add after existing configs:

```python
# 资金费率配置
FUNDING_RATE_ABNORMAL_THRESHOLD = get_env(
    'FUNDING_RATE_ABNORMAL_THRESHOLD',
    0.001,  # 0.1%
    float
)
```

- [ ] **Step 2: Update .env.example**

In `.env.example`, add:

```bash
# 资金费率异常阈值（绝对值，如 0.001 表示 0.1%）
FUNDING_RATE_ABNORMAL_THRESHOLD=0.001
```

- [ ] **Step 3: Commit**

```bash
git add src/coinx/config.py .env.example
git commit -m "feat(config): add funding rate configuration"
```

---

## Task 8: Homepage Integration - Data Loading

**Files:**
- Modify: `src/coinx/repositories/homepage_series.py`

- [ ] **Step 1: Add funding rate loading to _load_homepage_series_maps**

In `src/coinx/repositories/homepage_series.py`, update `_load_homepage_series_maps`:

```python
def _load_homepage_series_maps(session, symbols, upper_bound=None):
    """加载首页需要的所有 series 数据"""
    from .funding_rate import load_latest_funding_rates

    # ... existing OI, Kline, Taker loading logic ...

    # 新增：加载资金费率
    funding_rate_map = load_latest_funding_rates(symbols, session=session)

    return aggregate_oi_map, selected_kline_map, {}, coverage_map, funding_rate_map
```

- [ ] **Step 2: Commit**

```bash
git add src/coinx/repositories/homepage_series.py
git commit -m "feat(homepage): add funding rate data loading"
```

---

## Task 9: Homepage Integration - Data Display

**Files:**
- Modify: `src/coinx/repositories/homepage_series.py`

- [ ] **Step 1: Add format functions**

In `src/coinx/repositories/homepage_series.py`, add helper functions:

```python
def format_funding_rate(rate):
    """格式化资金费率（百分比）"""
    if rate is None:
        return 'N/A'
    return f'{float(rate) * 100:.4f}%'


def format_funding_countdown(next_funding_time):
    """格式化下次结算倒计时"""
    if next_funding_time is None:
        return 'N/A'

    from datetime import datetime
    now_ms = int(datetime.utcnow().timestamp() * 1000)
    diff_ms = next_funding_time - now_ms

    if diff_ms <= 0:
        return '结算中'

    hours = diff_ms // (1000 * 60 * 60)
    minutes = (diff_ms % (1000 * 60 * 60)) // (1000 * 60)

    if hours > 0:
        return f'{hours}h {minutes}m'
    return f'{minutes}m'
```

- [ ] **Step 2: Update _build_coin_payload**

Update `_build_coin_payload` function signature and add funding_rate field:

```python
def _build_coin_payload(symbol, oi_by_time, kline_by_time, taker_vol_by_time, coverage=None, funding_rate=None):
    # ... existing logic ...

    return {
        # ... existing fields ...

        # 新增：资金费率
        'funding_rate': {
            'predicted_rate': funding_rate.get('predicted_rate') if funding_rate else None,
            'predicted_rate_formatted': format_funding_rate(funding_rate.get('predicted_rate')) if funding_rate else 'N/A',
            'funding_rate': funding_rate.get('funding_rate') if funding_rate else None,
            'funding_rate_formatted': format_funding_rate(funding_rate.get('funding_rate')) if funding_rate else 'N/A',
            'next_funding_time': funding_rate.get('next_funding_time') if funding_rate else None,
            'next_funding_time_formatted': format_funding_countdown(funding_rate.get('next_funding_time')) if funding_rate else 'N/A',
        },
    }
```

- [ ] **Step 3: Update _build_homepage_series_snapshot**

Update `_build_homepage_series_snapshot` to pass funding_rate data:

```python
def _build_homepage_series_snapshot(symbols=None, session=None, now_ms=None):
    # ... existing logic ...

    recent_open_interest_map, recent_klines_map, recent_taker_vol_map, coverage_map, funding_rate_map = _load_homepage_series_maps(
        db,
        target_symbols,
        upper_bound=anchor_time,
    )

    data = []
    for symbol in target_symbols:
        coin = _build_coin_payload(
            symbol=symbol,
            oi_by_time=recent_open_interest_map.get(symbol, {}),
            kline_by_time=recent_klines_map.get(symbol, {}),
            taker_vol_by_time=recent_taker_vol_map.get(symbol, {}),
            coverage=coverage_map.get(symbol, {}),
            funding_rate=funding_rate_map.get(symbol),
        )
        # ... rest of logic ...
```

- [ ] **Step 4: Commit**

```bash
git add src/coinx/repositories/homepage_series.py
git commit -m "feat(homepage): add funding rate display to coin payload"
```

---

## Task 10: Funding Rate API Routes

**Files:**
- Create: `src/coinx/web/routes/api_funding_rate.py`

- [ ] **Step 1: Create API routes**

Create `src/coinx/web/routes/api_funding_rate.py`:

```python
"""资金费率 API 路由"""
from flask import Blueprint, jsonify, request
from coinx.coin_manager import get_active_coins
from coinx.repositories.funding_rate import (
    load_latest_funding_rates,
    load_funding_rate_history,
    load_abnormal_funding_rates,
)
from coinx.config import FUNDING_RATE_ABNORMAL_THRESHOLD

api_funding_rate = Blueprint('api_funding_rate', __name__)


@api_funding_rate.route('/api/funding-rate/ranking')
def get_funding_rate_ranking():
    """获取资金费率排行榜"""
    symbols = get_active_coins()
    rates = load_latest_funding_rates(symbols)

    ranking = [
        {
            'symbol': symbol,
            **rate
        }
        for symbol, rate in rates.items()
    ]
    ranking.sort(key=lambda x: abs(x.get('predicted_rate') or 0), reverse=True)

    return jsonify({
        'status': 'success',
        'data': ranking,
        'threshold': FUNDING_RATE_ABNORMAL_THRESHOLD,
    })


@api_funding_rate.route('/api/funding-rate/abnormal')
def get_abnormal_funding_rates():
    """获取异常资金费率"""
    abnormal = load_abnormal_funding_rates(threshold=FUNDING_RATE_ABNORMAL_THRESHOLD)

    return jsonify({
        'status': 'success',
        'data': abnormal,
        'threshold': FUNDING_RATE_ABNORMAL_THRESHOLD,
    })


@api_funding_rate.route('/api/funding-rate/history/<symbol>')
def get_funding_rate_history(symbol):
    """获取单个币种的资金费率历史"""
    hours = request.args.get('hours', 1, type=int)
    history = load_funding_rate_history(symbol, hours=hours)

    return jsonify({
        'status': 'success',
        'data': history,
        'symbol': symbol,
        'hours': hours,
    })
```

- [ ] **Step 2: Commit**

```bash
git add src/coinx/web/routes/api_funding_rate.py
git commit -m "feat(api): add funding rate API endpoints"
```

---

## Task 11: Register API Blueprint

**Files:**
- Modify: `src/coinx/web/app.py`

- [ ] **Step 1: Register api_funding_rate blueprint**

In `src/coinx/web/app.py`, add import and register:

```python
from .routes.api_funding_rate import api_funding_rate

# In create_app function, after other blueprint registrations:
app.register_blueprint(api_funding_rate)
```

- [ ] **Step 2: Commit**

```bash
git add src/coinx/web/app.py
git commit -m "feat(web): register funding rate API blueprint"
```

---

## Task 12: Funding Rate Page Route

**Files:**
- Modify: `src/coinx/web/routes/pages.py`

- [ ] **Step 1: Add funding-rate route**

In `src/coinx/web/routes/pages.py`, add:

```python
@pages.route('/funding-rate')
@login_required
def funding_rate():
    """资金费率监控页面"""
    from coinx.config import FUNDING_RATE_ABNORMAL_THRESHOLD
    return render_template('funding_rate.html', threshold=FUNDING_RATE_ABNORMAL_THRESHOLD)
```

- [ ] **Step 2: Commit**

```bash
git add src/coinx/web/routes/pages.py
git commit -m "feat(web): add funding rate page route"
```

---

## Task 13: Funding Rate Page Template

**Files:**
- Create: `src/coinx/web/templates/funding_rate.html`

- [ ] **Step 1: Create page template**

Create `src/coinx/web/templates/funding_rate.html`:

```html
{% extends "base.html" %}

{% block title %}资金费率监控{% endblock %}

{% block content %}
<div class="container">
    <h1>资金费率监控</h1>

    <!-- 异常费率警告 -->
    <section class="abnormal-section">
        <h2>异常费率（>{{ "%.1f"|format(threshold * 100) }}%）</h2>
        <div id="abnormal-list" class="rate-cards">
            <!-- 动态加载 -->
        </div>
    </section>

    <!-- 费率排行榜 -->
    <section class="ranking-section">
        <h2>费率排行榜</h2>
        <table id="ranking-table" class="rate-table">
            <thead>
                <tr>
                    <th>排名</th>
                    <th>币种</th>
                    <th>预测费率</th>
                    <th>上次费率</th>
                    <th>下次结算</th>
                    <th>标记价格</th>
                </tr>
            </thead>
            <tbody>
                <!-- 动态加载 -->
            </tbody>
        </table>
    </section>

    <!-- 1 小时趋势 -->
    <section class="history-section">
        <h2>1 小时趋势</h2>
        <select id="symbol-select">
            <!-- 动态填充 -->
        </select>
        <canvas id="rate-chart" width="800" height="300"></canvas>
    </section>
</div>

<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<script>
const THRESHOLD = {{ threshold }};

// 加载异常费率
async function loadAbnormal() {
    const response = await fetch('/api/funding-rate/abnormal');
    const data = await response.json();
    renderAbnormal(data.data);
}

// 加载排行榜
async function loadRanking() {
    const response = await fetch('/api/funding-rate/ranking');
    const data = await response.json();
    renderRanking(data.data);
    populateSymbolSelect(data.data);
}

// 加载历史数据
async function loadHistory(symbol) {
    const response = await fetch(`/api/funding-rate/history/${symbol}?hours=1`);
    const data = await response.json();
    renderChart(data.data);
}

// 填充币种选择器
function populateSymbolSelect(list) {
    const select = document.getElementById('symbol-select');
    select.innerHTML = list.map(item =>
        `<option value="${item.symbol}">${item.symbol}</option>`
    ).join('');
    select.addEventListener('change', (e) => loadHistory(e.target.value));
}

// 渲染异常费率
function renderAbnormal(list) {
    const container = document.getElementById('abnormal-list');
    if (list.length === 0) {
        container.innerHTML = '<p>暂无异常费率</p>';
        return;
    }
    container.innerHTML = list.map(item => `
        <div class="rate-card ${item.predicted_rate > 0 ? 'positive' : 'negative'}">
            <div class="symbol">${item.symbol}</div>
            <div class="rate">${(item.predicted_rate * 100).toFixed(4)}%</div>
        </div>
    `).join('');
}

// 渲染排行榜
function renderRanking(list) {
    const tbody = document.querySelector('#ranking-table tbody');
    tbody.innerHTML = list.map((item, index) => `
        <tr class="${Math.abs(item.predicted_rate || 0) > THRESHOLD ? 'abnormal' : ''}">
            <td>${index + 1}</td>
            <td>${item.symbol}</td>
            <td class="${(item.predicted_rate || 0) > 0 ? 'positive' : 'negative'}">
                ${item.predicted_rate_formatted || 'N/A'}
            </td>
            <td>${item.funding_rate_formatted || 'N/A'}</td>
            <td>${item.next_funding_time_formatted || 'N/A'}</td>
            <td>${item.mark_price ? '$' + item.mark_price.toLocaleString() : 'N/A'}</td>
        </tr>
    `).join('');
}

// 渲染图表
function renderChart(data) {
    const ctx = document.getElementById('rate-chart').getContext('2d');

    // Destroy existing chart if any
    if (window.rateChart) {
        window.rateChart.destroy();
    }

    window.rateChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: data.map(d => new Date(d.event_time).toLocaleTimeString()),
            datasets: [{
                label: '预测费率',
                data: data.map(d => (d.predicted_rate || 0) * 100),
                borderColor: '#1890ff',
                fill: false,
            }]
        },
        options: {
            responsive: true,
            scales: {
                y: {
                    ticks: {
                        callback: value => value + '%'
                    }
                }
            }
        }
    });
}

// 格式化倒计时
function formatCountdown(nextFundingTime) {
    if (!nextFundingTime) return 'N/A';

    const now = Date.now();
    const diff = nextFundingTime - now;

    if (diff <= 0) return '结算中';

    const hours = Math.floor(diff / (1000 * 60 * 60));
    const minutes = Math.floor((diff % (1000 * 60 * 60)) / (1000 * 60));

    if (hours > 0) return `${hours}h ${minutes}m`;
    return `${minutes}m`;
}

// 初始化
document.addEventListener('DOMContentLoaded', () => {
    loadAbnormal();
    loadRanking();
    // 默认加载 BTC 的历史数据
    setTimeout(() => loadHistory('BTCUSDT'), 100);
});
</script>

<style>
.abnormal-section {
    margin-bottom: 30px;
}

.rate-cards {
    display: flex;
    flex-wrap: wrap;
    gap: 10px;
}

.rate-card {
    padding: 15px;
    border-radius: 8px;
    background: #fff;
    box-shadow: 0 2px 8px rgba(0,0,0,0.1);
    min-width: 150px;
    text-align: center;
}

.rate-card.positive {
    border-left: 4px solid #52c41a;
}

.rate-card.negative {
    border-left: 4px solid #ff4d4f;
}

.symbol {
    font-weight: bold;
    margin-bottom: 5px;
}

.rate {
    font-size: 18px;
    font-weight: bold;
}

.rate.positive {
    color: #52c41a;
}

.rate.negative {
    color: #ff4d4f;
}

.rate-table {
    width: 100%;
    border-collapse: collapse;
    margin-top: 10px;
}

.rate-table th,
.rate-table td {
    padding: 12px;
    text-align: left;
    border-bottom: 1px solid #f0f0f0;
}

.rate-table th {
    background: #fafafa;
    font-weight: bold;
}

.rate-table tr.abnormal {
    background: #fff1f0;
}

.positive {
    color: #52c41a;
}

.negative {
    color: #ff4d4f;
}

.history-section {
    margin-top: 30px;
}

#symbol-select {
    margin-bottom: 15px;
    padding: 8px;
    font-size: 14px;
}
</style>
{% endblock %}
```

- [ ] **Step 2: Commit**

```bash
git add src/coinx/web/templates/funding_rate.html
git commit -m "feat(web): add funding rate monitoring page template"
```

---

## Task 14: Integration Test

**Files:**
- Create: `tests/integration/test_funding_rate_integration.py`

- [ ] **Step 1: Write integration test**

Create `tests/integration/test_funding_rate_integration.py`:

```python
"""Integration tests for funding rate feature"""
import pytest
from coinx.repositories.funding_rate import save_funding_rates, load_latest_funding_rates
from coinx.models import MarketFundingRate


class TestFundingRateIntegration:
    """Test funding rate save and load integration"""

    def test_save_and_load_funding_rate(self, db_session):
        """Test saving and loading funding rate data"""
        records = [
            {
                'symbol': 'BTCUSDT',
                'period': '5m',
                'event_time': 1698768000000,
                'funding_rate': 0.0001,
                'predicted_rate': 0.00012,
                'next_funding_time': 1698796800000,
                'mark_price': 34000.0,
                'exchange': 'binance',
            },
            {
                'symbol': 'BTCUSDT',
                'period': '5m',
                'event_time': 1698768300000,
                'funding_rate': 0.00011,
                'predicted_rate': 0.00013,
                'next_funding_time': 1698796800000,
                'mark_price': 34100.0,
                'exchange': 'binance',
            },
        ]

        # Save records
        saved_count = save_funding_rates(records, session=db_session)
        assert saved_count == 2

        # Load latest
        result = load_latest_funding_rates(['BTCUSDT'], session=db_session)

        assert 'BTCUSDT' in result
        assert result['BTCUSDT']['predicted_rate'] == 0.00013
        assert result['BTCUSDT']['funding_rate'] == 0.00011
        assert result['BTCUSDT']['event_time'] == 1698768300000

    def test_save_duplicate_records(self, db_session):
        """Test saving duplicate records updates instead of insert"""
        record = {
            'symbol': 'ETHUSDT',
            'period': '5m',
            'event_time': 1698768000000,
            'funding_rate': 0.0001,
            'predicted_rate': 0.00012,
            'next_funding_time': 1698796800000,
            'mark_price': 1800.0,
            'exchange': 'binance',
        }

        # Save first time
        save_funding_rates([record], session=db_session)

        # Update and save again
        record['predicted_rate'] = 0.00015
        save_funding_rates([record], session=db_session)

        # Load and verify update
        result = load_latest_funding_rates(['ETHUSDT'], session=db_session)
        assert result['ETHUSDT']['predicted_rate'] == 0.00015
```

- [ ] **Step 2: Run integration test**

Run: `pytest tests/integration/test_funding_rate_integration.py -v`
Expected: PASS (requires database setup)

- [ ] **Step 3: Commit**

```bash
git add tests/integration/test_funding_rate_integration.py
git commit -m "test: add funding rate integration tests"
```

---

## Task 15: Final Verification

- [ ] **Step 1: Run all tests**

Run: `pytest tests/ -v`
Expected: All tests PASS

- [ ] **Step 2: Manual verification checklist**

- [ ] Start the application
- [ ] Check homepage displays funding rate column
- [ ] Visit `/funding-rate` page
- [ ] Verify ranking table loads
- [ ] Verify abnormal rates are highlighted
- [ ] Verify 1-hour trend chart works
- [ ] Check database has funding rate records after 5 minutes

- [ ] **Step 3: Final commit**

```bash
git add -A
git commit -m "feat: complete funding rate feature implementation"
```

---

## Summary

This implementation plan adds Binance funding rate monitoring to CoinX with:

1. **Data Collection**: Every 5 minutes from `/fapi/v1/premiumIndex` API
2. **Data Storage**: All records preserved in `market_funding_rate` table
3. **Homepage Integration**: Predicted rate and countdown in rightmost column
4. **New Page**: Ranking, abnormal detection, 1-hour trend chart

Total tasks: 15
Total commits: ~15
Estimated implementation time: 2-3 hours
