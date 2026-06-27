"""StarRocks 连接测试脚本

使用前确保 .env 中设置了:
    DB_TYPE=starrocks
    SR_HOST=localhost
    SR_PORT=9030
    SR_USER=root
    SR_PASSWORD=
    SR_DB=coinx

或通过环境变量直接传递。
"""
import sys
from pathlib import Path

# Add project root to sys.path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

try:
    from coinx.config import DB_TYPE, DATABASE_URI

    print(f"DB_TYPE: {DB_TYPE}")
    print(f"DATABASE_URI: {DATABASE_URI}")

    if DB_TYPE != 'starrocks':
        print("\nWARNING: DB_TYPE is not set to 'starrocks'.")
        print("Set DB_TYPE=starrocks in your .env to connect to StarRocks.")
        print("Continuing with current config anyway...\n")

    from sqlalchemy import create_engine, text

    engine = create_engine(DATABASE_URI)
    print(f"Dialect: {engine.dialect.name}")

    with engine.connect() as connection:
        version = connection.execute(text("SELECT VERSION()")).scalar()
        print(f"Database version: {version}")

        result = connection.execute(text("SELECT 1")).scalar()
        print(f"SELECT 1 result: {result}")

        print("\nStarRocks connection successful!")
except ModuleNotFoundError as e:
    print(f"Module import failed: {e}")
    print("Make sure the starrocks package is installed: pip install starrocks")
except Exception as e:
    print(f"StarRocks connection failed: {e}")
    print("Please check your StarRocks configuration in .env or environment variables.")
