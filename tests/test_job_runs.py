from coinx.models import ScheduledJobRun
from coinx.repositories.job_runs import (
    INTERRUPTED_ERROR_MESSAGE,
    create_job_run,
    delete_expired_job_runs,
    finish_job_run,
    get_job_runs,
    get_latest_job_runtime_metadata,
    recover_interrupted_job_runs,
)


def test_job_run_lifecycle_persists_summary_and_error(db_session):
    success_run_id = create_job_run('market_rank_refresh_job', started_at=1_000, session=db_session)
    finish_job_run(
        success_run_id,
        status='success',
        summary={'saved_count': 123},
        completed_at=1_250,
        duration_ms=250,
        session=db_session,
    )
    failed_run_id = create_job_run('market_rank_refresh_job', started_at=2_000, session=db_session)
    finish_job_run(
        failed_run_id,
        status='error',
        error='upstream timeout',
        completed_at=2_500,
        duration_ms=500,
        session=db_session,
    )

    latest = get_latest_job_runtime_metadata(['market_rank_refresh_job'], session=db_session)
    runs = get_job_runs('market_rank_refresh_job', limit=20, session=db_session)

    assert latest['market_rank_refresh_job']['last_status'] == 'error'
    assert latest['market_rank_refresh_job']['last_error'] == 'upstream timeout'
    assert latest['market_rank_refresh_job']['last_duration_ms'] == 500
    assert [run['id'] for run in runs] == [failed_run_id, success_run_id]
    assert runs[1]['last_summary'] == {'saved_count': 123}


def test_recover_interrupted_runs_marks_them_as_errors(db_session):
    run_id = create_job_run('repair_market_rolling_job', started_at=1_000, session=db_session)

    recovered = recover_interrupted_job_runs(completed_at=2_200, session=db_session)
    run = db_session.get(ScheduledJobRun, run_id)

    assert recovered == 1
    assert run.status == 'error'
    assert run.error_message == INTERRUPTED_ERROR_MESSAGE
    assert run.completed_at == 2_200
    assert run.duration_ms == 1_200


def test_delete_expired_job_runs_keeps_records_within_retention(db_session):
    old_run_id = create_job_run('old-job', started_at=1, session=db_session)
    recent_run_id = create_job_run('recent-job', started_at=100 * 24 * 60 * 60 * 1000, session=db_session)

    deleted = delete_expired_job_runs(
        retention_days=90,
        now=101 * 24 * 60 * 60 * 1000,
        session=db_session,
    )

    assert deleted == 1
    assert db_session.get(ScheduledJobRun, old_run_id) is None
    assert db_session.get(ScheduledJobRun, recent_run_id) is not None
