import os
import yaml
from pathlib import Path

# 获取项目根目录
ROOT_DIR = Path(__file__).resolve().parent.parent.parent
DEFAULT_CONFIG_FILE = ROOT_DIR / "application.yml"

# 全局配置字典
_CONFIG = {}

def deep_merge(target, source):
    """深度合并两个字典"""
    for key, value in source.items():
        if isinstance(value, dict) and key in target and isinstance(target[key], dict):
            deep_merge(target[key], value)
        else:
            target[key] = value

def load_config():
    """加载YAML配置文件，支持多环境"""
    global _CONFIG
    _CONFIG = {}
    
    # 1. 加载默认配置 (application.yml)
    if DEFAULT_CONFIG_FILE.exists():
        try:
            with open(DEFAULT_CONFIG_FILE, 'r', encoding='utf-8') as f:
                default_config = yaml.safe_load(f) or {}
                deep_merge(_CONFIG, default_config)
            print(f"默认配置已加载: {DEFAULT_CONFIG_FILE}")
        except Exception as e:
            print(f"加载默认配置失败: {e}")
    else:
        print(f"默认配置文件未找到: {DEFAULT_CONFIG_FILE}")

    # 2. 获取当前环境 (COINX_ENV > profiles.active in default config > "dev")
    env = os.getenv('COINX_ENV')
    if not env:
        # 尝试从默认配置中读取 profiles.active
        env = _CONFIG.get('profiles', {}).get('active', 'dev')
    
    print(f"当前环境: {env}")

    # 3. 加载环境特定配置 (application-{env}.yml)
    if env:
        env_config_file = ROOT_DIR / f"application-{env}.yml"
        if env_config_file.exists():
            try:
                with open(env_config_file, 'r', encoding='utf-8') as f:
                    env_config = yaml.safe_load(f) or {}
                    # 深度合并，环境配置覆盖默认配置
                    deep_merge(_CONFIG, env_config)
                print(f"环境配置已加载: {env_config_file}")
            except Exception as e:
                print(f"加载环境配置失败: {e}")
        else:
            print(f"环境配置文件未找到: {env_config_file}")

# 初始化加载
load_config()

def get_conf(env_key, yaml_path, default=None, type_func=None):
    """
    获取配置项，优先级: 环境变量 > YAML配置 > 默认值
    :param env_key: 环境变量名
    :param yaml_path: YAML配置路径 (e.g. "database.host")
    :param default: 默认值
    :param type_func: 类型转换函数
    :return: 配置值
    """
    # 1. 尝试从环境变量获取
    val = os.getenv(env_key)
    if val is not None:
        if type_func:
            try:
                if type_func == bool:
                    return val.strip().lower() in ('1', 'true', 'yes', 'y', 'on')
                return type_func(val)
            except:
                pass
        return val

    # 2. 尝试从YAML配置获取
    keys = yaml_path.split('.')
    curr = _CONFIG
    try:
        for k in keys:
            if isinstance(curr, dict) and k in curr:
                curr = curr[k]
            else:
                raise KeyError
        
        # 找到值后进行类型转换
        if type_func and curr is not None:
             # 如果配置已经是正确类型，就不需要转换 (例如列表)
            if isinstance(curr, type_func):
                return curr
            # 特殊处理bool类型，防止yaml解析为bool后这里又处理出错（尽管yaml.safe_load已经处理了bool）
            # 这里主要处理如果yaml里是字符串的情况
            if type_func == bool and isinstance(curr, str):
                 return curr.strip().lower() in ('1', 'true', 'yes', 'y', 'on')
            if type_func != bool:
                return type_func(curr)
                
        return curr
    except (KeyError, TypeError):
        pass

    # 3. 返回默认值
    return default

# ================= 配置项定义 =================

# 数据存储路径
DATA_DIR = os.path.join(str(ROOT_DIR), 'data')
LOGS_DIR = os.path.join(str(ROOT_DIR), 'logs')

# 确保目录存在
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

# 币安API端点
BINANCE_BASE_URL = get_conf('BINANCE_BASE_URL', 'binance.base_url', 'https://proxy.yffjglcms.com/fapi.binance.com')

# 定时任务间隔（秒）
UPDATE_INTERVAL = get_conf('UPDATE_INTERVAL', 'app.update_interval', 300, int)

# 支持的时间间隔
_DEFAULT_INTERVALS = ['5m', '15m', '30m', '1h', '2h', '4h', '6h', '12h', '1d']
TIME_INTERVALS = get_conf('TIME_INTERVALS', 'app.time_intervals', _DEFAULT_INTERVALS, list)

# 代理配置
PROXY_HOST = get_conf('PROXY_HOST', 'proxy.host', '127.0.0.1')
PROXY_PORT = get_conf('PROXY_PORT', 'proxy.port', 7897, int)
PROXY_URL = f'http://{PROXY_HOST}:{PROXY_PORT}'
USE_PROXY = get_conf('USE_PROXY', 'proxy.enabled', False, bool)

# HTTPS代理配置
HTTPS_PROXY_URL = f'http://{PROXY_HOST}:{PROXY_PORT}'

# 数据库配置
DB_HOST = get_conf('DB_HOST', 'database.host', 'localhost')
DB_PORT = get_conf('DB_PORT', 'database.port', 3306, int)
DB_USER = get_conf('DB_USER', 'database.username', 'root')
DB_PASSWORD = get_conf('DB_PASSWORD', 'database.password', '')
DB_NAME = get_conf('DB_NAME', 'database.name', 'coinx')
DB_CHARSET = get_conf('DB_CHARSET', 'database.charset', 'utf8mb4')

DATABASE_URI = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset={DB_CHARSET}"

# Web服务配置 (仅供参考，实际在app.py或run脚本中使用)
WEB_HOST = get_conf('WEB_HOST', 'server.host', '0.0.0.0')
WEB_PORT = get_conf('WEB_PORT', 'server.port', 5000, int)
WEB_DEBUG = get_conf('WEB_DEBUG', 'server.debug', False, bool)
