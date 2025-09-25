# 项目更新日志

## 2025-09-25

### 1. 代理配置增强
- 修改了 [src/config.py](file:///d:/CODE/test/coinx/src/config.py) 文件，支持通过环境变量控制代理：
  ```python
  # 是否使用代理 - 可通过环境变量 USE_PROXY 配置，默认为 True
  USE_PROXY = os.getenv('USE_PROXY', 'True').lower() == 'true'
  ```
- 支持的配置方式：
  - 环境变量：`USE_PROXY=True` 或 `USE_PROXY=False`
  - 配置文件：修改默认值

### 2. 币安API错误处理增强
- 增强了 [src/coin_manager.py](file:///d:/CODE/test/coinx/src/coin_manager.py) 中的错误处理机制：
  - 添加响应内容类型检查
  - 增加专门的JSON解析错误处理
  - 改进网络请求异常处理
  - 增加超时时间（从10秒到15秒）
- 修复了潜在的空指针异常问题

### 3. API调用修复
- 修复了 [web/templates/index.html](file:///d:/CODE/test/coinx/web/templates/index.html) 中的API调用问题：
  - 将默认的GET请求改为明确指定的POST请求
  - 与后端API定义保持一致
  - 统一了两个页面中相同功能的实现方式

### 4. 测试文件整理
- 将测试相关的文件移至 [tests](file:///d:/CODE/test/coinx/tests) 目录下，保持项目结构整洁