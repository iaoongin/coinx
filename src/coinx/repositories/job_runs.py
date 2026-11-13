"""Persistence helpers for scheduled job execution history."""

import time

from sqlalchemy import desc, inspect, text

from coinx.config import TASK_RUN_STALE_TIMEOUT_SECONDS
from coinx.database import get_session
from coinx.models import ScheduledJobRun


INTERRUPTED_ERROR_MESSAGE = '服务中断，任务未完成'
STALE_RUN_ERROR_MESSAGE = '任务执行超时，未收到心跳'


def now_ms():
    return int(time.time() * 1000)


def ensure_job_run_schema(session=None):
    own_session = session is None
    db = session or get_session()
    try:
        bind = db.get_bind()
        ScheduledJobRun.__table__.create(bind=bind, checkfirst=True)
        columns = {column['name'] for column in inspect(bind).get_columns(ScheduledJobRun.__tablename__)}
        if 'heartbeat_at' not in columns:
            db.execute(text('ALTER TABLE scheduled_job_runs ADD COLUMN heartbeat_at BIGINT'))
            db.commit()
        for index in ScheduledJobRun.__table__.indexes:
            index.create(bind=bind, checkfirst=True)
    finally:
        if own_session:
            db.close()


def create_job_run(job_id, started_at=None, session=None):
    own_session = session is None
    db = session or get_session()
    try:
        run = ScheduledJobRun(
            job_id=job_id,
            status='running',
            started_at=started_at if started_at is not None else now_ms(),
        )
        run.heartbeat_at = run.started_at
        db.add(run)
        db.commit()
        return run.id
    except Exception:
        db.rollback()
        raise
    finally:
        if own_session:
            db.close()


def update_job_run(run_id, status=None, summary=None, error=None, session=None):
    """Persist an in-flight run snapshot without closing the run."""
    own_session = session is None
    db = session or get_session()
    try:
        run = db.get(ScheduledJobRun, run_id)
        if run is None:
            return False
        if status is not None:
            run.status = status
        if summary is not None:
            run.summary_json = summary
        if error is not None:
            run.error_message = str(error)[:500]
        if status == 'running' or summary is not None:
            run.heartbeat_at = now_ms()
        db.commit()
        return True
    except Exception:
        db.rollback()
        raise
    finally:
        if own_session:
            db.close()


def heartbeat_running_job(run_id, heartbeat_at, session=None):
    own_session = session is None
    db = session or get_session()
    try:
        updated = db.query(ScheduledJobRun).filter(
            ScheduledJobRun.id == run_id,
            ScheduledJobRun.status == 'running',
        ).update({ScheduledJobRun.heartbeat_at: heartbeat_at}, synchronize_session=False)
        db.commit()
        return bool(updated)
    except Exception:
        db.rollback()
        raise
    finally:
        if own_session:
            db.close()


def finish_job_run(run_id, status, summary=None, error=None, completed_at=None, duration_ms=None, session=None):
    own_session = session is None
    db = session or get_session()
    try:
        run = db.get(ScheduledJobRun, run_id)
        if run is None:
            return False
        run.status = status
        run.summary_json = summary
        run.error_message = str(error)[:500] if error else None
        run.completed_at = completed_at if completed_at is not None else now_ms()
        run.duration_ms = round(duration_ms) if duration_ms is not None else None
        run.heartbeat_at = run.completed_at
        db.commit()
        return True
    except Exception:
        db.rollback()
        raise
    finally:
        if own_session:
            db.close()


def _is_stale_run(run, current_ms=None):
    if run.status != 'running':
        return False
    heartbeat_at = run.heartbeat_at or run.started_at
    current_ms = current_ms if current_ms is not None else now_ms()
    timeout_ms = max(1, int(TASK_RUN_STALE_TIMEOUT_SECONDS)) * 1000
    return current_ms - heartbeat_at > timeout_ms


def _serialize_run(run):
    summary = run.summary_json
    error_detail = summary.get('_error_detail') if isinstance(summary, dict) else None
    if isinstance(summary, dict) and '_error_detail' in summary:
        summary = {key: value for key, value in summary.items() if key != '_error_detail'}
    current_ms = now_ms()
    stale = _is_stale_run(run, current_ms=current_ms)
    status = 'timeout' if stale else run.status
    last_error = STALE_RUN_ERROR_MESSAGE if stale and not run.error_message else run.error_message
    duration_ms = run.duration_ms
    if stale and duration_ms is None:
        duration_ms = max(0, current_ms - run.started_at)
    return {
        'id': run.id,
        'job_id': run.job_id,
        'status': status,
        'last_status': status,
        'last_summary': summary,
        'last_error': last_error,
        'last_error_detail': error_detail,
        'last_started_at_ms': run.started_at,
        'last_heartbeat_at_ms': run.heartbeat_at or run.started_at,
        'last_finished_at_ms': run.completed_at,
        'last_duration_ms': duration_ms,
        'running': run.status == 'running' and not stale,
    }


def get_latest_job_runtime_metadata(job_ids, session=None):
    job_ids = list(dict.fromkeys(job_id for job_id in (job_ids or []) if job_id))
    if not job_ids:
        return {}
    own_session = session is None
    db = session or get_session()
    try:
        # Each narrow query is covered by (job_id, started_at, id); load full
        # records by primary key only after the latest IDs have been identified.
        latest_ids = []
        for job_id in job_ids:
            run_id = (
                db.query(ScheduledJobRun.id)
                .filter(ScheduledJobRun.job_id == job_id)
                .order_by(desc(ScheduledJobRun.started_at), desc(ScheduledJobRun.id))
                .limit(1)
                .scalar()
            )
            if run_id is not None:
                latest_ids.append(run_id)
        if not latest_ids:
            return {}
        rows = db.query(ScheduledJobRun).filter(ScheduledJobRun.id.in_(latest_ids)).all()
        return {run.job_id: _serialize_run(run) for run in rows}
    finally:
        if own_session:
            db.close()


def get_job_runs(job_id, limit=20, offset=0, session=None):
    own_session = session is None
    db = session or get_session()
    try:
        rows = (
            db.query(ScheduledJobRun)
            .filter(ScheduledJobRun.job_id == job_id)
            .order_by(desc(ScheduledJobRun.started_at), desc(ScheduledJobRun.id))
            .offset(offset)
            .limit(limit)
            .all()
        )
        return [_serialize_run(run) for run in rows]
    finally:
        if own_session:
            db.close()


def get_job_run_count(job_id, session=None):
    own_session = session is None
    db = session or get_session()
    try:
        return db.query(ScheduledJobRun.id).filter(ScheduledJobRun.job_id == job_id).count()
    finally:
        if own_session:
            db.close()


def recover_interrupted_job_runs(completed_at=None, session=None):
    own_session = session is None
    db = session or get_session()
    try:
        finished_at = completed_at if completed_at is not None else now_ms()
        rows = db.query(ScheduledJobRun).filter(ScheduledJobRun.status == 'running').all()
        for run in rows:
            run.status = 'error'
            run.error_message = INTERRUPTED_ERROR_MESSAGE
            run.completed_at = finished_at
            run.duration_ms = max(0, finished_at - run.started_at)
            run.heartbeat_at = finished_at
        db.commit()
        return len(rows)
    except Exception:
        db.rollback()
        raise
    finally:
        if own_session:
            db.close()


def delete_expired_job_runs(retention_days, now=None, session=None):
    own_session = session is None
    db = session or get_session()
    try:
        cutoff = (now if now is not None else now_ms()) - max(0, int(retention_days)) * 24 * 60 * 60 * 1000
        deleted = db.query(ScheduledJobRun).filter(ScheduledJobRun.started_at < cutoff).delete(
            synchronize_session=False,
        )
        db.commit()
        return deleted
    except Exception:
        db.rollback()
        raise
    finally:
        if own_session:
            db.close()
