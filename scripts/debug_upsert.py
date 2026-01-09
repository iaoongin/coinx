import sys
import os
import logging
from datetime import datetime
from sqlalchemy.dialects.mysql import insert

# Add project root to sys.path
project_root = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, str(project_root))
sys.path.insert(0, os.path.join(project_root, 'src'))

# Configure logging
logging.basicConfig(level=logging.INFO)

from coinx.utils import logger
from coinx.database import db_session
from coinx.models import Coin
from coinx.coin_manager import load_coins_config_dict

def test_update():
    print("Fetching exchange info...")
    from coinx.collector import get_exchange_info
    all_coins = get_exchange_info()
    
    if not all_coins:
        print("No coins fetched.")
        return

    print(f"Fetched {len(all_coins)} coins. Processing ALL...")
    
    current_config = load_coins_config_dict()
    
    for i, coin_info in enumerate(all_coins):
        symbol = coin_info.get('symbol')
        # print(f"Processing {i+1}/{len(all_coins)}: {symbol}")
        
        is_tracking = False
        if symbol in current_config:
             is_tracking = current_config[symbol]
        
        values = {
            'symbol': symbol,
            'is_tracking': is_tracking,
            'base_asset': coin_info.get('baseAsset'),
            'quote_asset': coin_info.get('quoteAsset'),
            'margin_asset': coin_info.get('marginAsset'),
            'price_precision': coin_info.get('pricePrecision'),
            'quantity_precision': coin_info.get('quantityPrecision'),
            'base_asset_precision': coin_info.get('baseAssetPrecision'),
            'quote_precision': coin_info.get('quotePrecision'),
            'status': coin_info.get('status'),
            'onboard_date': coin_info.get('onboardDate'),
            'delivery_date': coin_info.get('deliveryDate'),
            'contract_type': coin_info.get('contractType'),
            'underlying_type': coin_info.get('underlyingType'),
            'liquidation_fee': coin_info.get('liquidationFee'),
            'maint_margin_percent': coin_info.get('maintMarginPercent'),
            'required_margin_percent': coin_info.get('requiredMarginPercent'),
            'updated_at': datetime.now()
        }
    
        try:
            stmt = insert(Coin).values(**values)
            on_duplicate_key_stmt = stmt.on_duplicate_key_update(**values)
            db_session.execute(on_duplicate_key_stmt)
        except Exception as e:
            print(f"Error processing {symbol}: {e}")
            print(f"Values: {values}")
            db_session.rollback()
            raise
            
    db_session.commit()
    print("All coins processed successfully!")

if __name__ == "__main__":
    test_update()
