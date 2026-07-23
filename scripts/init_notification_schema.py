"""Create the notification tables without modifying existing CoinX tables."""

from coinx.database import Base, engine
from coinx.models import (
    AlertEvaluationRun,
    AlertRule,
    AlertRuleChannel,
    AlertState,
    NotificationChannel,
    NotificationDelivery,
)


TABLES = [
    NotificationChannel.__table__,
    AlertRule.__table__,
    AlertRuleChannel.__table__,
    AlertState.__table__,
    NotificationDelivery.__table__,
    AlertEvaluationRun.__table__,
]


if __name__ == '__main__':
    Base.metadata.create_all(bind=engine, tables=TABLES)
    print('Notification tables are ready.')
