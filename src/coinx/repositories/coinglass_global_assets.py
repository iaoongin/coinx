"""Persistence and read queries for CoinGlass global asset snapshots."""

from __future__ import annotations

import time
from datetime import datetime
from typing import Any, Iterable, Mapping, Optional

from sqlalchemy import func
from sqlalchemy.dialects.mysql import insert as mysql_insert

from coinx import config
from coinx.collector.coinglass_global_assets import fetch_pages
from coinx.database import get_session
from coinx.models import MarketCoinglassGlobalAsset
from coinx.utils import logger


ASSET_COLUMNS = (
    'symbol', 'snapshot_time', 'oi_market_cap_ratio', 'h1_oi_change',
    'h4_oi_change', 'h24_oi_change', 'market_cap', 'open_interest', 'price',
    'h24_price_change', 'avg_funding_rate', 'h24_volume', 'h24_volume_change',
    'h24_liquidation', 'created_at',
)
NUMERIC_COLUMNS = ASSET_COLUMNS[2:-1]
DISPLAY_COLUMNS = ('symbol', 'snapshot_time') + NUMERIC_COLUMNS
SORT_COLUMNS = {column: getattr(MarketCoinglassGlobalAsset, column) for column in DISPLAY_COLUMNS if column != 'snapshot_time'}


def _record_values(records: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    now = datetime.now()
    rows = []
    for record in records:
        row = {key: value for key, value in dict(record).items() if key in ASSET_COLUMNS}
        row.setdefault('created_at', now)
        if row.get('symbol') and row.get('snapshot_time') is not None:
            rows.append(row)
    return rows


def save_global_assets(records: Iterable[Mapping[str, Any]], session=None) -> int:
    rows = _record_values(records)
    if not rows:
        return 0

    own_session = session is None
    db = session or get_session()
    try:
        MarketCoinglassGlobalAsset.__table__.create(bind=db.get_bind(), checkfirst=True)
        if db.bind.dialect.name == 'mysql':
            statement = mysql_insert(MarketCoinglassGlobalAsset).values(rows)
            update_values = {
                column: getattr(statement.inserted, column)
                for column in NUMERIC_COLUMNS
            }
            db.execute(statement.on_duplicate_key_update(**update_values))
        else:
            for row in rows:
                identity = {
                    'symbol': row['symbol'],
                    'snapshot_time': row['snapshot_time'],
                }
                current = db.query(MarketCoinglassGlobalAsset).filter_by(**identity).one_or_none()
                if current is None:
                    db.add(MarketCoinglassGlobalAsset(**row))
                else:
                    for key, value in row.items():
                        if key not in {'id', 'symbol', 'snapshot_time'}:
                            setattr(current, key, value)
        db.commit()
        logger.info('CoinGlass 全球资产快照保存成功: %d 条记录', len(rows))
        return len(rows)
    except Exception:
        db.rollback()
        logger.exception('CoinGlass 全球资产快照保存失败')
        raise
    finally:
        if own_session:
            db.close()


def collect_and_save_global_assets(
    session=None,
    http_session=None,
    snapshot_time: Optional[int] = None,
) -> dict[str, Any]:
    """Fetch all configured pages, then persist the complete batch once."""
    captured_at = int(snapshot_time if snapshot_time is not None else time.time() * 1000)
    payload = fetch_pages(
        pages=config.COINGLASS_GLOBAL_ASSETS_PAGES,
        page_size=config.COINGLASS_GLOBAL_ASSETS_PAGE_SIZE,
        category=config.COINGLASS_GLOBAL_ASSETS_CATEGORY,
        api_url=config.COINGLASS_GLOBAL_ASSETS_API_URL,
        timeout=config.COINGLASS_GLOBAL_ASSETS_TIMEOUT_SECONDS,
        session=http_session,
        snapshot_time=captured_at,
    )
    expected_rows = sum(int(page.get('returned') or 0) for page in payload.get('pages', []))
    rows = payload.get('rows') or []
    own_session = session is None
    db = session or get_session()
    try:
        written = save_global_assets(rows, session=db)
        return {
            'status': 'success' if written == expected_rows else 'partial',
            'snapshot_time': captured_at,
            'total': payload.get('total'),
            'pages': len(payload.get('pages') or []),
            'page_size': config.COINGLASS_GLOBAL_ASSETS_PAGE_SIZE,
            'returned_count': expected_rows,
            'written_count': written,
            'skipped_count': max(0, expected_rows - written),
        }
    finally:
        if own_session:
            db.close()


def _serialize(row: MarketCoinglassGlobalAsset) -> dict[str, Any]:
    data = {'symbol': row.symbol, 'snapshot_time': int(row.snapshot_time)}
    for column in NUMERIC_COLUMNS:
        value = getattr(row, column)
        data[column] = float(value) if value is not None else None
    return data


def _snapshot_times(db, limit: int = 50) -> list[int]:
    rows = db.query(MarketCoinglassGlobalAsset.snapshot_time).distinct().order_by(
        MarketCoinglassGlobalAsset.snapshot_time.desc()
    ).limit(max(1, min(int(limit), 200))).all()
    return [int(row[0]) for row in rows if row[0] is not None]


def load_global_assets_page(
    *,
    keyword: str = '',
    sort_by: str = 'oi_market_cap_ratio',
    sort_order: str = 'desc',
    page: int = 1,
    page_size: int = 10,
    snapshot_time: Optional[int] = None,
    min_market_cap: Optional[float] = None,
    min_oi_market_cap_ratio: Optional[float] = None,
    min_h1_oi_change: Optional[float] = None,
    min_h4_oi_change: Optional[float] = None,
    min_h24_oi_change: Optional[float] = None,
    session=None,
) -> dict[str, Any]:
    own_session = session is None
    db = session or get_session()
    try:
        latest_time = db.query(func.max(MarketCoinglassGlobalAsset.snapshot_time)).scalar()
        selected_time = int(snapshot_time) if snapshot_time is not None else latest_time
        if selected_time is None:
            return {
                'data': [], 'total_count': 0, 'page': max(int(page or 1), 1),
                'page_size': max(int(page_size or 100), 1), 'snapshot_time': None,
                'snapshots': [], 'stats': _empty_stats(),
            }

        page = max(int(page or 1), 1)
        page_size = min(max(int(page_size or 100), 1), 500)
        query = db.query(MarketCoinglassGlobalAsset).filter(
            MarketCoinglassGlobalAsset.snapshot_time == selected_time,
        )
        keyword = (keyword or '').strip().upper()
        if keyword:
            query = query.filter(func.upper(MarketCoinglassGlobalAsset.symbol).like(f'%{keyword}%'))
        filters = (
            (MarketCoinglassGlobalAsset.market_cap, min_market_cap),
            (MarketCoinglassGlobalAsset.oi_market_cap_ratio, min_oi_market_cap_ratio),
            (MarketCoinglassGlobalAsset.h1_oi_change, min_h1_oi_change),
            (MarketCoinglassGlobalAsset.h4_oi_change, min_h4_oi_change),
            (MarketCoinglassGlobalAsset.h24_oi_change, min_h24_oi_change),
        )
        for column, threshold in filters:
            if threshold is not None:
                query = query.filter(column > threshold)
        total_count = query.count()
        stats_row = query.with_entities(
            func.sum(MarketCoinglassGlobalAsset.market_cap),
            func.sum(MarketCoinglassGlobalAsset.open_interest),
            func.sum(MarketCoinglassGlobalAsset.h24_volume),
            func.sum(MarketCoinglassGlobalAsset.h24_liquidation),
        ).first()
        sort_column = SORT_COLUMNS.get(sort_by, MarketCoinglassGlobalAsset.market_cap)
        query = query.order_by(sort_column.asc() if sort_order == 'asc' else sort_column.desc())
        query = query.order_by(MarketCoinglassGlobalAsset.symbol.asc())
        rows = query.offset((page - 1) * page_size).limit(page_size).all()
        stats = {
            'market_cap': float(stats_row[0]) if stats_row and stats_row[0] is not None else 0,
            'open_interest': float(stats_row[1]) if stats_row and stats_row[1] is not None else 0,
            'h24_volume': float(stats_row[2]) if stats_row and stats_row[2] is not None else 0,
            'h24_liquidation': float(stats_row[3]) if stats_row and stats_row[3] is not None else 0,
        }
        return {
            'data': [_serialize(row) for row in rows],
            'total_count': total_count,
            'page': page,
            'page_size': page_size,
            'snapshot_time': int(selected_time),
            'snapshots': _snapshot_times(db),
            'stats': stats,
        }
    finally:
        if own_session:
            db.close()


def _empty_stats() -> dict[str, float]:
    return {'market_cap': 0, 'open_interest': 0, 'h24_volume': 0, 'h24_liquidation': 0}


def load_latest_global_assets(session=None) -> dict[str, dict[str, Any]]:
    own_session = session is None
    db = session or get_session()
    try:
        latest_time = db.query(func.max(MarketCoinglassGlobalAsset.snapshot_time)).scalar()
        if latest_time is None:
            return {}
        rows = db.query(MarketCoinglassGlobalAsset).filter(
            MarketCoinglassGlobalAsset.snapshot_time == latest_time,
        ).all()
        return {row.symbol: _serialize(row) for row in rows}
    finally:
        if own_session:
            db.close()


def load_global_asset_history(symbol: str, limit: int = 100, session=None) -> list[dict[str, Any]]:
    own_session = session is None
    db = session or get_session()
    try:
        normalized = (symbol or '').strip()
        rows = db.query(MarketCoinglassGlobalAsset).filter(
            func.upper(MarketCoinglassGlobalAsset.symbol) == normalized.upper(),
        ).order_by(
            MarketCoinglassGlobalAsset.snapshot_time.desc(),
        ).limit(min(max(int(limit or 100), 1), 500)).all()
        return [_serialize(row) for row in rows]
    finally:
        if own_session:
            db.close()
