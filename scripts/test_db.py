import sys
import os
from pathlib import Path
from sqlalchemy import text

# Add project root to sys.path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

try:
    from coinx.database import engine
    
    print("Trying to connect to database...")
    print(f"Connection URL: {engine.url}")
    
    with engine.connect() as connection:
        result = connection.execute(text("SELECT 1"))
        print("Database connection successful!")
        print(f"Result: {result.scalar()}")
except ModuleNotFoundError as e:
    print(f"Module import failed: {e}")
except Exception as e:
    print(f"Database connection failed: {e}")
    print("Please check your database configuration in config.py or environment variables.")
