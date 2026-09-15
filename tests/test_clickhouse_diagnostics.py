import json
import logging
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest
import requests

from coinx.read_clients import ClickHouseReadClient, clickhouse_query_context
from coinx.repositories import homepage_series as homepage
from coinx.repositories.market_read import ClickHouseMarketReadRepository


class Response:
    def __init__(self, text='{"ok":1}\n', status=200, headers=None):
        self.text = text
        self.status_code = status
        self.ok = status < 400
        self.headers = headers or {}


class Session:
    def __init__(self, *responses):
        self.responses = iter(responses)
        self.calls = []

    def post(self, *args, **kwargs):
        self.calls.append(kwargs)
        response = next(self.responses)
        if isinstance(response, Exception):
            raise response
        return response


def client(session):
    return ClickHouseReadClient('http://clickhouse.test:8123', 'coinx', 'user', 'test-secret', session=session)


def events(caplog, event):
    prefix = f'ClickHouse query {event}: '
    return [
        json.loads(record.getMessage()[len(prefix):])
        for record in caplog.records
        if record.name == 'coinx.read_clients' and record.getMessage().startswith(prefix)
    ]


@pytest.mark.parametrize('summary', [None, 'broken json', '[]', '{"read_rows":"100","read_bytes":"2048","memory_usage":"512"}'])
def test_query_logs_optional_server_counters_without_inventing_peak(caplog, summary):
    caplog.set_level(logging.INFO, logger='coinx.read_clients')
    headers = {} if summary is None else {'X-ClickHouse-Summary': summary}
    session = Session(Response(headers=headers))
    assert client(session).query_rows('SELECT 1') == [{'ok': 1}]
    started, = events(caplog, 'started')
    finished, = events(caplog, 'finished')
    assert started['query_id'] == finished['query_id'] == session.calls[0]['params']['query_id']
    assert finished['peak_memory_usage'] is None
    assert finished['result_count'] == 1
    assert finished['duration_ms'] >= 0 and finished['queue_wait_ms'] >= 0
    if summary and summary.startswith('{'):
        assert finished['read_rows'] == '100'
        assert finished['read_bytes'] == '2048'
        assert finished['memory_usage'] == '512'
    else:
        assert finished['read_rows'] is None
    assert 'test-secret' not in caplog.text


def test_homepage_memory_failure_identifies_batch_and_resets_context(monkeypatch, caplog):
    caplog.set_level(logging.INFO, logger='coinx.read_clients')
    error = 'Code: 241. DB::Exception: (total) memory limit exceeded: While executing AggregatingTransform.'
    response = Response(error, 500, {
        'X-ClickHouse-Exception-Code': '241',
        'X-ClickHouse-Query-Id': 'server-query-id',
        'X-ClickHouse-Summary': '{"read_rows":"12345","peak_memory_usage":"1048576"}',
    })
    session = Session(Response('{}\n'), response, Response())
    http_client = client(session)
    repository = ClickHouseMarketReadRepository(http_client, 'coinx')
    monkeypatch.setattr(homepage, 'get_clickhouse_repository', lambda: repository)
    monkeypatch.setattr(homepage, 'CLICKHOUSE_AGGREGATION_SYMBOL_BATCH_SIZE', 1)
    monkeypatch.setattr(homepage, '_has_unreliable_taker_source', lambda exchange: False)
    upper = 1_700_000_000_000
    with pytest.raises(requests.HTTPError) as raised:
        homepage._load_homepage_exchange_maps_clickhouse(
            'binance', ['BTCUSDT', 'ETHUSDT'], upper_bound=upper,
            candidate_latest_override={'BTCUSDT': upper, 'ETHUSDT': upper},
        )

    failed, = events(caplog, 'failed')
    assert failed['query_id'] == session.calls[1]['params']['query_id']
    assert failed['query_id'] == raised.value.clickhouse_query_id
    assert raised.value.response is response
    assert error in failed['error']
    assert failed['stage'] == 'rows_open_interest'
    assert failed['operation'] == 'homepage'
    assert failed['exchange'] == 'binance'
    assert failed['table'] == 'market_open_interest_hist'
    assert failed['batch_index'] == failed['batch_count'] == 2
    assert failed['symbol_count'] == 1 and failed['symbols'] == ['ETHUSDT']
    assert failed['lower_bound'] == upper - homepage.MAX_TIME_INTERVAL_MS
    assert failed['upper_bound'] == upper
    assert failed['exception_code'] == '241'
    assert failed['server_query_id'] == 'server-query-id'
    assert failed['peak_memory_usage'] == '1048576'
    assert len(session.calls) == 2  # No memory-error retry or later table query.
    assert http_client.query_rows('SELECT 1') == [{'ok': 1}]
    assert 'operation' not in events(caplog, 'finished')[-1]
    assert 'test-secret' not in caplog.text


def test_timeout_keeps_query_id_and_original_exception(caplog):
    error = requests.Timeout('read timed out')
    session = Session(error)
    with pytest.raises(requests.Timeout) as raised:
        client(session).query_rows('SELECT 1')
    failed, = events(caplog, 'failed')
    assert raised.value is error
    assert failed['query_id'] == session.calls[0]['params']['query_id']
    assert failed['http_status'] is None
    assert failed['peak_memory_usage'] is None


def test_final_fallback_has_distinct_linked_query_ids(caplog):
    caplog.set_level(logging.INFO, logger='coinx.read_clients')
    session = Session(Response('Code: 181. ILLEGAL_FINAL', 500), Response())
    assert client(session).query_rows('SELECT * FROM coinx.market_tickers FINAL') == [{'ok': 1}]
    first, second = [call['params']['query_id'] for call in session.calls]
    assert first != second
    assert events(caplog, 'finished')[0]['retry_of'] == first
    assert 'FINAL' not in session.calls[1]['params']['query']


def test_parallel_query_contexts_do_not_mix(monkeypatch, caplog):
    import coinx.read_clients as read_clients
    from threading import BoundedSemaphore

    monkeypatch.setattr(read_clients, '_CLICKHOUSE_QUERY_SEMAPHORE', BoundedSemaphore(2))
    caplog.set_level(logging.INFO, logger='coinx.read_clients')
    barrier = Barrier(2)

    class ParallelSession:
        def post(self, *args, **kwargs):
            barrier.wait(timeout=5)
            return Response()

    def run(exchange):
        with clickhouse_query_context(operation='homepage', exchange=exchange):
            with clickhouse_query_context(stage='latest_kline'):
                return client(ParallelSession()).query_rows('SELECT 1')

    with ThreadPoolExecutor(max_workers=2) as executor:
        assert list(executor.map(run, ['binance', 'okx'])) == [[{'ok': 1}], [{'ok': 1}]]
    starts = {record['query_id']: record for record in events(caplog, 'started')}
    finishes = events(caplog, 'finished')
    assert {record['exchange'] for record in finishes} == {'binance', 'okx'}
    for record in finishes:
        assert record['exchange'] == starts[record['query_id']]['exchange']
        assert record['stage'] == 'latest_kline'
