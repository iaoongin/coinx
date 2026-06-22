# 资金费率 (Funding Rate) 数据拉取功能设计

> 状态：设计完成，待实现

## 1. 概述

### 1.1 目标

为 CoinX 添加 Binance 永续合约资金费率数据的采集、存储和展示功能，支持：
- 每 5 分钟采集一次预测资金费率
- 首页矩阵最右边显示预测费率
- 新页面展示费率排行榜、异常标记和 1 小时数据

### 1.2 背景

资金费率是永续合约的核心机制，用于锚定合约价格和现货价格：
- **正费率**：多头付费给空头（市场偏多）
- **负费率**：空头付费给多头（市场偏空）
- **预测费率**：下次结算时的预期费率，反映市场预期

监控资金费率有助于判断市场情绪和潜在的反转信号。

---

## 2. 数据源

### 2.1 Binance API

**预测资金费率 API**：
```
GET /fapi/v1/premiumIndex
```

**请求参数**：
- `symbol` (必需)：交易对名称，如 `BTCUSDT`

**响应示例**：
```json
{
  "symbol": "BTCUSDT",
  "markPrice": "34000.00",
  "indexPrice": "33995.00",
  "estimatedSettlePrice": "34002.00",
  "lastFundingRate": "0.00010000",
  "nextFundingRate": "0.00012000",
  "nextFundingTime": 1698796800000,
  "interestRate": "0.00010000",
  "time": 1698768000000
}
```

**关键字段**：
- `lastFundingRate`：上次结算时的资金费率
- `nextFundingRate`：下次结算的预测费率（重点关注）
- `nextFundingTime`：下次结算时间戳（毫秒）
- `markPrice`：当前标记价格

### 2.2 数据采集策略

- **采集频率**：每 5 分钟（与其他 series 一致）
- **采集范围**：所有跟踪币种 + 成交额前 100
- **存储方式**：保留所有采集记录，累积历史
- **数据清理**：可选定期清理超过 7 天的数据

---

## 3. 数据模型

### 3.1 数据库表设计

**表名**：`market_funding_rate`

```sql
CREATE TABLE market_funding_rate (
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

**字段说明**：
- `symbol`：交易对名称，如 `BTCUSDT`
- `period`：采集周期，固定为 `5m`
- `event_time`：采集时间戳（毫秒），用于时间窗口计算
- `funding_rate`：上次结算时的实际费率
- `predicted_rate`：下次结算的预测费率（首页展示重点）
- `next_funding_time`：下次结算时间，用于计算倒计时
- `mark_price`：采集时的标记价格
- `exchange`：交易所标识，支持未来扩展

### 3.2 SQLAlchemy 模型

**文件**：`src/coinx/models.py`

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

---

## 4. 数据采集层

### 4.1 采集模块

**文件**：`src/coinx/collector/binance/funding_rate.py`

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

### 4.2 集成到 Series 框架

**修改文件**：`src/coinx/collector/binance/series.py`

```python
# 在 SERIES_ENDPOINTS 中添加
SERIES_ENDPOINTS = {
    'open_interest_hist': '/futures/data/openInterestHist',
    'klines': '/fapi/v1/klines',
    'taker_buy_sell_vol': '/futures/data/takerlongshortRatio',
    'funding_rate': '/fapi/v1/premiumIndex',  # 新增
}

# 在 fetchers 和 parsers 中添加
def fetch_series_payload(series_type, symbol, period, limit, session=None, ...):
    fetchers = {
        'open_interest_hist': fetch_open_interest_hist,
        'klines': fetch_klines,
        'taker_buy_sell_vol': fetch_taker_buy_sell_vol,
        'funding_rate': fetch_funding_rate,  # 新增
    }
    # ...

def parse_series_payload(series_type, payload, symbol, period):
    parsers = {
        'open_interest_hist': parse_open_interest_hist,
        'klines': parse_klines,
        'taker_buy_sell_vol': parse_taker_buy_sell_vol,
        'funding_rate': parse_funding_rate,  # 新增
    }
    # ...
```

---

## 5. 数据存储层

### 5.1 存储模块

**文件**：`src/coinx/repositories/funding_rate.py`

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
        records: 记录列表，每条包含 symbol, period, event_time, funding_rate, predicted_rate 等
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
            # 使用 upsert 逻辑：如果存在则更新，否则插入
            existing = db.query(MarketFundingRate).filter(
                MarketFundingRate.symbol == record['symbol'],
                MarketFundingRate.period == record['period'],
                MarketFundingRate.event_time == record['event_time']
            ).first()

            if existing:
                # 更新现有记录
                existing.funding_rate = record.get('funding_rate')
                existing.predicted_rate = record.get('predicted_rate')
                existing.next_funding_time = record.get('next_funding_time')
                existing.mark_price = record.get('mark_price')
            else:
                # 插入新记录
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
        # 使用子查询获取每个 symbol 的最新 event_time
        subquery = db.query(
            MarketFundingRate.symbol,
            func.max(MarketFundingRate.event_time).label('max_time')
        ).filter(
            MarketFundingRate.symbol.in_(symbols),
            MarketFundingRate.period == '5m',
            MarketFundingRate.exchange == exchange
        ).group_by(MarketFundingRate.symbol).subquery()

        # 批量查询所有最新记录
        records = db.query(MarketFundingRate).join(
            subquery,
            (MarketFundingRate.symbol == subquery.c.symbol) &
            (MarketFundingRate.event_time == subquery.c.max_time)
        ).all()

        # 转换为字典
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
    加载单个币种的资金费率历史（用于新页面展示）

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
        # 查询每个 symbol 的最新记录
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

        # 过滤异常记录
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

        # 按绝对值降序排序
        abnormal.sort(key=lambda x: abs(x['predicted_rate']), reverse=True)

        return abnormal

    finally:
        if own_session:
            db.close()
```

---

## 6. 调度任务集成

### 6.1 修改滚动修补任务

**修改文件**：`src/coinx/scheduler.py`

在滚动修补任务中添加 `funding_rate` 系列类型：

```python
from .repositories.homepage_series import HOMEPAGE_REQUIRED_SERIES_TYPES

# HOMEPAGE_REQUIRED_SERIES_TYPES 已包含 'funding_rate'
# 滚动修补任务会自动采集资金费率
```

### 6.2 修改配置

**修改文件**：`src/coinx/repositories/homepage_series.py`

```python
# 在 HOMEPAGE_REQUIRED_SERIES_TYPES 中添加 funding_rate
HOMEPAGE_REQUIRED_SERIES_TYPES = (
    'klines',
    'open_interest_hist',
    'taker_buy_sell_vol',
    'funding_rate',  # 新增
)
```

### 6.3 新增配置项

**修改文件**：`src/coinx/config.py`

```python
# 资金费率配置
FUNDING_RATE_ABNORMAL_THRESHOLD = get_env(
    'FUNDING_RATE_ABNORMAL_THRESHOLD',
    0.001,  # 0.1%
    float
)
```

---

## 7. 首页集成

### 7.1 数据加载

**修改文件**：`src/coinx/repositories/homepage_series.py`

在 `_load_homepage_series_maps` 中添加资金费率数据加载：

```python
def _load_homepage_series_maps(session, symbols, upper_bound=None):
    # ... 现有的 OI、Kline、Taker 加载逻辑 ...

    # 新增：加载资金费率
    from .funding_rate import load_latest_funding_rates
    funding_rate_map = load_latest_funding_rates(symbols, session=session)

    return aggregate_oi_map, selected_kline_map, {}, coverage_map, funding_rate_map
```

### 7.2 数据展示

在 `_build_coin_payload` 中添加 funding_rate 字段：

```python
def _build_coin_payload(symbol, oi_by_time, kline_by_time, taker_vol_by_time, coverage=None, funding_rate=None):
    # ... 现有逻辑 ...

    return {
        # ... 现有字段 ...

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


def format_funding_rate(rate):
    """格式化资金费率（百分比）"""
    if rate is None:
        return 'N/A'
    return f'{float(rate) * 100:.4f}%'


def format_funding_countdown(next_funding_time):
    """格式化下次结算倒计时"""
    if next_funding_time is None:
        return 'N/A'

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

### 7.3 前端展示

**修改文件**：首页模板（如 `src/coinx/web/templates/index.html`）

在表格最右边添加资金费率列：

```html
<thead>
    <tr>
        <th>币种</th>
        <!-- ... 现有列 ... -->
        <th class="funding-rate-col">预测费率</th>  <!-- 新增 -->
    </tr>
</thead>
<tbody>
    {% for coin in coins %}
    <tr>
        <td>{{ coin.symbol }}</td>
        <!-- ... 现有列 ... -->
        <td class="funding-rate-col">
            <span class="rate-value {{ 'positive' if coin.funding_rate.predicted_rate > 0 else 'negative' }}">
                {{ coin.funding_rate.predicted_rate_formatted }}
            </span>
            <span class="countdown">{{ coin.funding_rate.next_funding_time_formatted }}</span>
        </td>
    </tr>
    {% endfor %}
</tbody>
```

**CSS 样式**：
```css
.funding-rate-col {
    min-width: 100px;
    text-align: right;
}

.rate-value {
    font-weight: bold;
    display: block;
}

.rate-value.positive {
    color: #52c41a;  /* 绿色 */
}

.rate-value.negative {
    color: #ff4d4f;  /* 红色 */
}

.countdown {
    font-size: 12px;
    color: #8c8c8c;
    display: block;
}
```

---

## 8. 新页面：资金费率监控

### 8.1 页面功能

**路由**：`GET /funding-rate`

**功能模块**：
1. **费率排行榜**：显示所有跟踪币种的当前预测费率
2. **异常标记**：高亮显示绝对值超过阈值的费率（如 > 0.1%）
3. **1 小时数据**：显示最近 1 小时的资金费率变化

### 8.2 API 设计

**文件**：`src/coinx/web/routes/api_funding_rate.py`

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

    # 转换为列表并排序
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

### 8.3 页面模板

**文件**：`src/coinx/web/templates/funding_rate.html`

```html
{% extends "base.html" %}

{% block title %}资金费率监控{% endblock %}

{% block content %}
<div class="container">
    <h1>资金费率监控</h1>

    <!-- 异常费率警告 -->
    <section class="abnormal-section">
        <h2>⚠️ 异常费率（>{{ "%.1f"|format(threshold * 100) }}%）</h2>
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
}

// 加载历史数据
async function loadHistory(symbol) {
    const response = await fetch(`/api/funding-rate/history/${symbol}?hours=1`);
    const data = await response.json();
    renderChart(data.data);
}

// 渲染异常费率
function renderAbnormal(list) {
    const container = document.getElementById('abnormal-list');
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
        <tr class="${Math.abs(item.predicted_rate) > ${threshold} ? 'abnormal' : ''}">
            <td>${index + 1}</td>
            <td>${item.symbol}</td>
            <td class="${item.predicted_rate > 0 ? 'positive' : 'negative'}">
                ${(item.predicted_rate * 100).toFixed(4)}%
            </td>
            <td>${(item.funding_rate * 100).toFixed(4)}%</td>
            <td>${formatCountdown(item.next_funding_time)}</td>
            <td>$${item.mark_price.toLocaleString()}</td>
        </tr>
    `).join('');
}

// 渲染图表
function renderChart(data) {
    const ctx = document.getElementById('rate-chart').getContext('2d');
    new Chart(ctx, {
        type: 'line',
        data: {
            labels: data.map(d => new Date(d.event_time).toLocaleTimeString()),
            datasets: [{
                label: '预测费率',
                data: data.map(d => d.predicted_rate * 100),
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

// 初始化
document.addEventListener('DOMContentLoaded', () => {
    loadAbnormal();
    loadRanking();
    // 默认加载 BTC 的历史数据
    loadHistory('BTCUSDT');
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

---

## 9. 配置项

### 9.1 新增配置

**文件**：`src/coinx/config.py`

```python
# 资金费率配置
FUNDING_RATE_ABNORMAL_THRESHOLD = get_env(
    'FUNDING_RATE_ABNORMAL_THRESHOLD',
    0.001,  # 0.1%
    float
)
```

**文件**：`.env.example`

```bash
# 资金费率异常阈值（绝对值，如 0.001 表示 0.1%）
FUNDING_RATE_ABNORMAL_THRESHOLD=0.001
```

---

## 10. 文件改动清单

| 文件 | 改动类型 | 说明 |
|------|---------|------|
| `sql/schema.sql` | 修改 | 添加 market_funding_rate 建表语句 |
| `src/coinx/models.py` | 修改 | 添加 MarketFundingRate 模型 |
| `src/coinx/collector/binance/funding_rate.py` | 新增 | 资金费率采集模块 |
| `src/coinx/collector/binance/series.py` | 修改 | 注册 funding_rate 类型 |
| `src/coinx/repositories/funding_rate.py` | 新增 | 存储和查询模块 |
| `src/coinx/repositories/homepage_series.py` | 修改 | 首页集成 |
| `src/coinx/scheduler.py` | 无需修改 | 复用现有滚动修补任务（通过 HOMEPAGE_REQUIRED_SERIES_TYPES 自动支持） |
| `src/coinx/config.py` | 修改 | 添加配置项 |
| `src/coinx/web/routes/pages.py` | 修改 | 添加 /funding-rate 路由 |
| `src/coinx/web/routes/api_funding_rate.py` | 新增 | API 路由 |
| `src/coinx/web/templates/funding_rate.html` | 新增 | 页面模板 |
| `.env.example` | 修改 | 添加配置示例 |

---

## 11. 测试计划

### 11.1 单元测试

- 测试 `fetch_premium_index` 函数
- 测试 `save_funding_rates` 批量保存逻辑
- 测试 `load_latest_funding_rates` 查询逻辑
- 测试 `load_abnormal_funding_rates` 异常检测逻辑

### 11.2 集成测试

- 测试滚动修补任务是否正确采集资金费率
- 测试首页是否正确显示预测费率
- 测试新页面 API 是否正常工作

### 11.3 验收标准

- [ ] 每 5 分钟自动采集资金费率
- [ ] 首页最右边显示预测费率和倒计时
- [ ] 新页面显示费率排行榜
- [ ] 异常费率正确标记（绝对值 > 0.1%）
- [ ] 1 小时历史数据正常展示
- [ ] 数据库正确保存所有采集记录

---

## 12. 未来扩展

### 12.1 可能的增强

1. **多交易所支持**：扩展到 OKX、Gate 等交易所
2. **告警功能**：当费率超过阈值时发送通知
3. **数据分析**：费率与价格的相关性分析
4. **历史深度**：支持更长时间的历史查询（30 天、90 天）

### 12.2 性能优化

1. **索引优化**：根据查询模式调整索引
2. **数据清理**：定期清理超过 7 天的历史数据
3. **缓存策略**：对热点数据添加 Redis 缓存

---

## 13. 风险与缓解

| 风险 | 影响 | 缓解措施 |
|------|------|---------|
| Binance API 限流 | 数据采集中断 | 复用现有的 retry 和限流机制 |
| 数据库存储增长 | 存储空间不足 | 定期清理超过 7 天的数据 |
| 预测费率不准确 | 展示误导 | 标注"预测"字样，提示仅供参考 |
| 前端展示性能 | 页面加载慢 | 使用分页和懒加载 |

---

## 14. 总结

本设计方案为 CoinX 添加了资金费率监控功能，包括：

1. **数据采集**：每 5 分钟从 Binance 采集预测资金费率
2. **数据存储**：保留所有采集记录，支持历史查询
3. **首页集成**：在最右边显示预测费率和倒计时
4. **新页面**：费率排行榜、异常标记、1 小时趋势图

设计遵循现有架构模式，复用 series 框架和调度任务，实现成本低，易于维护和扩展。
