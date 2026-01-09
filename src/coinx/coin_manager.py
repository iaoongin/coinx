import sys
import os
import json
from datetime import datetime
from sqlalchemy.dialects.mysql import insert

# 添加项目根目录到路径
project_root = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, project_root)

# 添加src到路径
sys.path.insert(0, os.path.join(project_root, 'src'))

from coinx.utils import logger
from coinx.database import db_session, init_db
from coinx.models import Coin
from coinx.config import DATA_DIR, TIME_INTERVALS

# 币种配置文件路径
COINS_CONFIG_FILE = os.path.join(DATA_DIR, 'coins_config.json')

def load_coins_config():
    """
    从数据库加载币种配置
    :return: 币种配置列表（只包含启用跟踪的币种）
    """
    try:
        # 确保数据库已初始化（可选，如果确定已初始化可移除）
        # init_db()
        
        coins = db_session.query(Coin).filter(Coin.is_tracking == True).all()
        
        if not coins:
            # 如果数据库为空，尝试从文件迁移或创建默认
            if os.path.exists(COINS_CONFIG_FILE):
                migrate_from_file()
                coins = db_session.query(Coin).filter(Coin.is_tracking == True).all()
            else:
                # 默认配置
                default_coins = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT']
                for symbol in default_coins:
                    add_coin(symbol, True)
                return default_coins

        return [coin.symbol for coin in coins]
        
    except Exception as e:
        logger.error(f"加载币种配置失败: {e}")
        # 出错时回退到默认
        return ['BTCUSDT', 'ETHUSDT', 'BNBUSDT']

def load_coins_config_dict():
    """
    从数据库加载币种配置字典（包含跟踪状态）
    :return: 币种配置字典 {symbol: tracked}
    """
    try:
        coins = db_session.query(Coin).all()
        
        if not coins and os.path.exists(COINS_CONFIG_FILE):
            migrate_from_file()
            coins = db_session.query(Coin).all()
            
        return {coin.symbol: coin.is_tracking for coin in coins}
    except Exception as e:
        logger.error(f"加载币种配置字典失败: {e}")
        return {'BTCUSDT': True, 'ETHUSDT': True, 'BNBUSDT': True}

def save_coins_config_dict(coins_dict):
    """
    保存币种配置字典到数据库
    :param coins_dict: 币种配置字典 {symbol: tracked}
    """
    try:
        for symbol, tracked in coins_dict.items():
            # 使用 merge 进行 upsert (insert or update)
            # 注意：Merge 会查询一次，效率可能略低，也可以使用 SQLAlchemy 的 upsert 方言
            
            # MySQL特定的 UPSERT
            stmt = insert(Coin).values(symbol=symbol, is_tracking=tracked)
            on_duplicate_key_stmt = stmt.on_duplicate_key_update(is_tracking=tracked, updated_at=datetime.now())
            
            db_session.execute(on_duplicate_key_stmt)
            
        db_session.commit()
        logger.info(f"币种配置已保存到数据库: {len(coins_dict)} 个币种")
        
    except Exception as e:
        db_session.rollback()
        logger.error(f"保存币种配置失败: {e}")

def migrate_from_file():
    """从旧的JSON文件迁移数据到数据库"""
    try:
        if not os.path.exists(COINS_CONFIG_FILE):
            return
            
        logger.info("开始从文件迁移币种配置到数据库...")
        with open(COINS_CONFIG_FILE, 'r', encoding='utf-8') as f:
            config = json.load(f)
            
        coins_to_add = {}
        
        if isinstance(config, list):
             coins_to_add = {coin: True for coin in config}
        elif isinstance(config, dict) and 'coins' in config:
            coins_data = config['coins']
            if isinstance(coins_data, list):
                coins_to_add = {coin: True for coin in coins_data}
            elif isinstance(coins_data, dict):
                coins_to_add = coins_data
                
        # 保存到数据库
        if coins_to_add:
            save_coins_config_dict(coins_to_add)
            logger.info("迁移完成")
            
    except Exception as e:
        logger.error(f"迁移失败: {e}")

def update_coins_config():
    """
    更新币种配置：从币安获取最新交易对并更新配置
    """
    try:
        logger.info("开始更新币种配置...")
        
        # 从币安获取所有USDT交易对
        from coinx.collector import get_exchange_info
        all_coins = get_exchange_info()
        
        # 加载现有配置
        current_config = load_coins_config_dict()
        
        # 检查是否获取到了币种数据
        if all_coins is None:
            # 网络请求失败
            logger.error("从币安获取交易对列表失败")
            return False
        elif len(all_coins) == 0:
            # 获取到了响应但没有符合条件的交易对
            logger.warning("未找到符合条件的USDT交易对")
            return True
        else:
            # 批量更新：使用 upsert 更新所有信息
            count = 0
            for coin_info in all_coins:
                symbol = coin_info.get('symbol')
                if not symbol:
                    continue
                
                # 确定跟踪状态：如果是新币种，默认不跟踪；如果已存在，保持原有状态
                if symbol not in current_config:
                    is_tracking = False 
                    count += 1
                else:
                    is_tracking = current_config[symbol]

                # 构建数据字典
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
                    # underlyingSubType 是列表，取第一个或拼接
                    'underlying_type': coin_info.get('underlyingType'),
                    'liquidation_fee': coin_info.get('liquidationFee'),
                    'maint_margin_percent': coin_info.get('maintMarginPercent'),
                    'required_margin_percent': coin_info.get('requiredMarginPercent'),
                    'updated_at': datetime.now()
                }
                
                # 执行 Upsert
                stmt = insert(Coin).values(**values)
                on_duplicate_key_stmt = stmt.on_duplicate_key_update(**values)
                db_session.execute(on_duplicate_key_stmt)
            
            db_session.commit()
            
            if count > 0:
                logger.info(f"添加了 {count} 个新币种")
            
            logger.info(f"币种配置更新完成，共更新 {len(all_coins)} 个币种信息")
            return True
    except Exception as e:
        logger.error(f"更新币种配置失败: {e}")
        return False

def get_active_coins(filter_symbols=None):
    """
    获取活跃的币种列表（启用跟踪的币种）
    :param filter_symbols: 可选的筛选币种列表
    :return: 币种列表
    """
    # 加载启用跟踪的币种配置
    tracked_coins = load_coins_config()
    
    # 如果提供了筛选列表，则只返回筛选后的币种
    if filter_symbols:
        filtered_coins = [coin for coin in tracked_coins if coin in filter_symbols]
        return filtered_coins
    
    return tracked_coins

def get_all_coins_list():
    """
    获取所有币种列表（从币安获取）
    :return: 所有币种列表
    """
    try:
        from coinx.collector import get_exchange_info
        all_coins = get_exchange_info()
        if all_coins:
            return [coin['symbol'] for coin in all_coins]
        else:
            return []
    except Exception as e:
        logger.error(f"获取所有币种列表失败: {e}")
        return []

def set_coin_tracking(symbol, tracked):
    """
    设置币种的跟踪状态
    :param symbol: 币种符号
    :param tracked: 是否跟踪（True/False）
    """
    try:
        # 使用upsert逻辑更新单个
        stmt = insert(Coin).values(symbol=symbol, is_tracking=tracked)
        on_duplicate_key_stmt = stmt.on_duplicate_key_update(is_tracking=tracked, updated_at=datetime.now())
        db_session.execute(on_duplicate_key_stmt)
        db_session.commit()
        
        logger.info(f"币种 {symbol} 跟踪状态已更新为: {tracked}")
        return True
    except Exception as e:
        db_session.rollback()
        logger.error(f"更新币种跟踪状态失败: {e}")
        return False

def add_coin(symbol, tracked=True):
    """
    添加币种到配置
    :param symbol: 币种符号
    :param tracked: 是否跟踪（默认True）
    """
    return set_coin_tracking(symbol, tracked)

def remove_coin(symbol):
    """
    从配置中移除币种 (设置为不跟踪，或者物理删除？这里选择物理删除以匹配原语意，或者仅设为不跟踪)
    原逻辑是 del coins_config[symbol]，相当于不再管理。
    在数据库中，我们选择物理删除。
    """
    try:
        db_session.query(Coin).filter(Coin.symbol == symbol).delete()
        db_session.commit()
        logger.info(f"币种 {symbol} 已从配置中移除")
        return True
    except Exception as e:
        db_session.rollback()
        logger.error(f"移除币种失败: {e}")
        return False

# 提供 get_session 兼容旧代码使用requests
import requests
from coinx.config import USE_PROXY, PROXY_URL, HTTPS_PROXY_URL

def get_session():
    """创建带代理配置的requests会话"""
    session = requests.Session()
    
    if USE_PROXY:
        proxies = {
            'http': PROXY_URL,
            'https': HTTPS_PROXY_URL
        }
        session.proxies.update(proxies)
        logger.info(f"使用代理: {PROXY_URL}")
    
    return session

if __name__ == "__main__":
    # 测试功能
    print("测试币种管理功能 (DB模式)...")
    
    # 迁移数据
    # migrate_from_file()
    
    # 更新币种配置
    # update_coins_config()
    
    # 获取活跃币种
    active_coins = get_active_coins()
    print(f"活跃币种数量: {len(active_coins)}")
    print(f"前10个币种: {active_coins[:10]}")
