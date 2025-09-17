import json

# 读取数据文件
with open('d:/CODE/test/coinx/data/coins_data.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# 获取最新的数据条目
latest_entry = data[-1]
print(f"时间戳: {latest_entry['timestamp']}")

# 查找BTCUSDT数据
btc_data = None
for coin in latest_entry['data']:
    if coin['symbol'] == 'BTCUSDT':
        btc_data = coin
        break

if btc_data:
    print(f"BTCUSDT 当前数据:")
    print(f"  持仓量: {btc_data['current']['openInterest']}")
    print(f"  持仓价值: {btc_data['current']['openInterestValue']}")
    print(f"  时间: {btc_data['current']['time']}")
    
    print(f"\nBTCUSDT 历史数据:")
    for interval_data in btc_data['intervals']:
        print(f"  {interval_data['interval']}:")
        print(f"    持仓量: {interval_data['openInterest']}")
        print(f"    持仓价值: {interval_data['openInterestValue']}")
        print(f"    时间: {interval_data['timestamp']}")
else:
    print("未找到BTCUSDT数据")