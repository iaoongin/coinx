"""Read-only MySQL table size statistics for the storage dashboard."""

import time

from sqlalchemy import text

from coinx.database import get_session


TABLE_STATS_QUERY = text(
    """
    SELECT
        TABLE_NAME AS table_name,
        TABLE_COMMENT AS table_comment,
        COALESCE(DATA_LENGTH, 0) AS data_bytes,
        COALESCE(INDEX_LENGTH, 0) AS index_bytes,
        COALESCE(DATA_FREE, 0) AS free_bytes
    FROM information_schema.TABLES
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_TYPE = 'BASE TABLE'
    ORDER BY (COALESCE(DATA_LENGTH, 0) + COALESCE(INDEX_LENGTH, 0)) DESC,
             TABLE_NAME ASC
    """
)


def _quote_table_name(table_name):
    """Quote a table identifier returned by information_schema."""
    return f"`{str(table_name).replace('`', '``')}`"


def _exact_row_count(session, table_name):
    statement = text(f"SELECT COUNT(*) FROM {_quote_table_name(table_name)}")
    return int(session.execute(statement).scalar() or 0)


def _table_stats(row, row_count):
    data_bytes = int(row['data_bytes'] or 0)
    index_bytes = int(row['index_bytes'] or 0)
    free_bytes = int(row['free_bytes'] or 0)
    used_bytes = data_bytes + index_bytes
    total_bytes = used_bytes + free_bytes
    free_percent = (free_bytes * 100 / total_bytes) if total_bytes else 0

    return {
        'table_name': row['table_name'],
        'table_comment': row['table_comment'] or '',
        'row_count': row_count,
        'data_bytes': data_bytes,
        'index_bytes': index_bytes,
        'used_bytes': used_bytes,
        'free_bytes': free_bytes,
        'free_percent': round(free_percent, 2),
    }


def get_database_stats(session=None):
    """Return current MySQL table row and space statistics."""
    own_session = session is None
    db = session or get_session()
    started_at = time.perf_counter()

    try:
        bind = db.get_bind()
        if bind.dialect.name != 'mysql':
            raise RuntimeError('数据存储统计仅支持 MySQL')

        rows = db.execute(TABLE_STATS_QUERY).mappings().all()
        tables = [
            _table_stats(row, _exact_row_count(db, row['table_name']))
            for row in rows
        ]
        tables.sort(key=lambda item: (-item['used_bytes'], item['table_name']))

        row_count = sum(item['row_count'] for item in tables)
        used_bytes = sum(item['used_bytes'] for item in tables)
        free_bytes = sum(item['free_bytes'] for item in tables)
        data_bytes = sum(item['data_bytes'] for item in tables)
        index_bytes = sum(item['index_bytes'] for item in tables)
        total_bytes = used_bytes + free_bytes

        return {
            'database': db.execute(text('SELECT DATABASE()')).scalar(),
            'refreshed_at_ms': int(time.time() * 1000),
            'query_duration_ms': round((time.perf_counter() - started_at) * 1000),
            'summary': {
                'table_count': len(tables),
                'row_count': row_count,
                'data_bytes': data_bytes,
                'index_bytes': index_bytes,
                'used_bytes': used_bytes,
                'free_bytes': free_bytes,
                'total_bytes': total_bytes,
                'free_percent': round((free_bytes * 100 / total_bytes) if total_bytes else 0, 2),
                'largest_table_name': tables[0]['table_name'] if tables else None,
                'largest_table_used_bytes': tables[0]['used_bytes'] if tables else 0,
            },
            'tables': tables,
        }
    finally:
        if own_session:
            db.close()
