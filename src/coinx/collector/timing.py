import time
from contextlib import contextmanager

from coinx.collector.rate_limit import consume_rate_limit_wait_seconds


DURATION_BREAKDOWN_KEYS = (
    'api_ms',
    'rate_limit_wait_ms',
    'cooldown_skip_ms',
    'db_read_ms',
    'db_write_ms',
    'parse_ms',
    'precheck_ms',
    'other_ms',
)


def empty_duration_breakdown():
    return {key: 0.0 for key in DURATION_BREAKDOWN_KEYS}


def normalize_duration_breakdown(breakdown=None):
    normalized = empty_duration_breakdown()
    if not breakdown:
        return normalized
    for key, value in breakdown.items():
        if key in normalized:
            normalized[key] += max(0.0, float(value or 0.0))
    return normalized


def add_duration_breakdown(target, source):
    if target is None:
        target = empty_duration_breakdown()
    for key, value in normalize_duration_breakdown(source).items():
        target[key] = target.get(key, 0.0) + value
    return target


def sum_duration_breakdowns(items):
    total = empty_duration_breakdown()
    for item in items or []:
        add_duration_breakdown(total, item)
    return total


def round_duration_breakdown(breakdown):
    return {key: round(value, 2) for key, value in normalize_duration_breakdown(breakdown).items()}


@contextmanager
def timed_category(breakdown, key):
    started_at = time.perf_counter()
    wait_before = consume_rate_limit_wait_seconds()
    try:
        yield
    finally:
        elapsed_ms = (time.perf_counter() - started_at) * 1000
        wait_ms = max(0.0, (consume_rate_limit_wait_seconds() - wait_before) * 1000)
        if key == 'api_ms':
            breakdown['rate_limit_wait_ms'] = breakdown.get('rate_limit_wait_ms', 0.0) + wait_ms
            breakdown[key] = breakdown.get(key, 0.0) + max(0.0, elapsed_ms - wait_ms)
        else:
            breakdown[key] = breakdown.get(key, 0.0) + elapsed_ms


def record_sleep_ms(breakdown, seconds):
    seconds = max(0.0, float(seconds or 0.0))
    if seconds <= 0:
        return
    breakdown['rate_limit_wait_ms'] = breakdown.get('rate_limit_wait_ms', 0.0) + seconds * 1000


def attach_other_duration(breakdown, total_ms):
    measured = sum(
        value
        for key, value in normalize_duration_breakdown(breakdown).items()
        if key not in ('other_ms', 'cooldown_skip_ms', 'precheck_ms')
    )
    breakdown['other_ms'] = max(0.0, float(total_ms or 0.0) - measured)
    return round_duration_breakdown(breakdown)


def format_duration_ms(duration_ms):
    duration_ms = max(0.0, float(duration_ms or 0.0))
    if duration_ms < 1000:
        return f'{duration_ms:.0f}ms'
    seconds = duration_ms / 1000
    if seconds < 60:
        return f'{seconds:.2f}s'
    minutes = int(seconds // 60)
    remaining_seconds = seconds - minutes * 60
    return f'{minutes}m{remaining_seconds:.1f}s'


def format_duration_breakdown(breakdown):
    breakdown = normalize_duration_breakdown(breakdown)
    labels = (
        ('API', 'api_ms'),
        ('限流等待', 'rate_limit_wait_ms'),
        ('读库', 'db_read_ms'),
        ('写库', 'db_write_ms'),
        ('解析', 'parse_ms'),
        ('预检墙钟', 'precheck_ms'),
        ('其他', 'other_ms'),
    )
    parts = [f'{label}={format_duration_ms(breakdown.get(key, 0.0))}' for label, key in labels]
    cooldown_skip_ms = breakdown.get('cooldown_skip_ms', 0.0)
    if cooldown_skip_ms > 0:
        parts.append(f'冷却剩余={format_duration_ms(cooldown_skip_ms)}')
    return ','.join(parts)
