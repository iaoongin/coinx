from web.app import app
import json

# 测试API
with app.test_client() as c:
    response = c.get('/api/coins')
    print('API响应状态码:', response.status_code)
    data = response.get_json()
    print('API响应数据:', str(data)[:200])