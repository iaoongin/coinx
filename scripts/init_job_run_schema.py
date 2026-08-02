"""Create the scheduled job run table for existing CoinX databases."""

from coinx.database import engine
from coinx.models import ScheduledJobRun


if __name__ == '__main__':
    ScheduledJobRun.__table__.create(bind=engine, checkfirst=True)
    print('Scheduled job run table is ready.')
