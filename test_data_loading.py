import sys
import os

# 添加项目根目录到路径
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

from src.binance_api import update_all_data
from src.data_processor import get_all_coins_data
from src.coin_manager import get_active_coins, load_coins_config_dict
from src.utils import logger

def test_data_loading():
    print("测试数据加载流程...")
    
    # 1. 检查币种配置
    print("\n1. 检查币种配置:")
    config_dict = load_coins_config_dict()
    print(f"币种配置字典: {config_dict}")
    
    active_coins = get_active_coins()
    print(f"活跃币种: {active_coins}")
    
    # 2. 更新数据
    print("\n2. 更新数据:")
    try:
        updated_data = update_all_data(active_coins)
        print(f"更新后的数据: {updated_data}")
    except Exception as e:
        print(f"更新数据时出错: {e}")
        import traceback
        traceback.print_exc()
    
    # 3. 获取处理后的数据
    print("\n3. 获取处理后的数据:")
    try:
        processed_data = get_all_coins_data(active_coins)
        print(f"处理后的数据: {processed_data}")
    except Exception as e:
        print(f"处理数据时出错: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_data_loading()