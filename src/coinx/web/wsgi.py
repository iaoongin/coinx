from coinx.runtime import start_runtime_services
from coinx.web.app import app


start_runtime_services(with_startup_repair=True, startup_delay_seconds=1)
