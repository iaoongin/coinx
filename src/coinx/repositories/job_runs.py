"""Persistence helpers for scheduled job execution history."""

import time

from sqlalchemy import desc

from coinx.database import get_session
from coinx.models import ScheduledJobRun


INTERRUPTED_ERROR_MESSAGE = '服务中断，任务未完成'


def now_ms():
    return int(time.time() * 1000)


def ensure_job_run_schema(session=None):
    own_session = session is None
    db = session or get_session()
    try:
        ScheduledJobRun.__table__.create(bind=db.get_bind(), checkfirst=True)
        for index in ScheduledJobRun.__table__.indexes:
            index.create(bind=db.get_bind(), checkfirst=True)
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
        db.add(run)
        db.commit()
        return run.id
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
        db.commit()
        return True
    except Exception:
        db.rollback()
        raise
    finally:
        if own_session:
            db.close()


def _serialize_run(run):
    return {
        'id': run.id,
        'job_id': run.job_id,
        'status': run.status,
        'last_status': run.status,
        'last_summary': run.summary_json,
        'last_error': run.error_message,
        'last_started_at_ms': run.started_at,
        'last_finished_at_ms': run.completed_at,
        'last_duration_ms': run.duration_ms,
        'running': run.status == 'running',
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
