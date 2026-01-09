import sys
import os
import logging

# Add project root to sys.path
project_root = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, str(project_root))
sys.path.insert(0, os.path.join(project_root, 'src'))

# Configure basic logging to stdout
logging.basicConfig(level=logging.INFO, stream=sys.stdout)

try:
    from coinx.coin_manager import update_coins_config
    print("Starting update_coins_config...")
    result = update_coins_config()
    print(f"Result: {result}")
except Exception as e:
    print(f"Exception caught: {e}")
    import traceback
    traceback.print_exc()
