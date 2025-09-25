from web.app import app
import json

# 测试过滤功能
with app.test_client() as c:
    response = c.get('/api/coins?symbol=BTC')
    print('过滤BTC的API响应状态码:', response.status_code)
    data = response.get_json()
    print('过滤BTC的API响应数据:', str(data)[:200])