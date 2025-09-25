# 币种数据监控系统

这是一个从币安交易所获取币种数据并在Web页面上展示的系统。

## 项目结构

```
coinx/
├── src/                 # 核心源代码
│   ├── __init__.py
│   ├── binance_api.py   # 币安API接口
│   ├── config.py        # 配置文件
│   ├── data_processor.py # 数据处理模块
│   ├── scheduler.py     # 定时任务调度器
│   └── utils.py         # 工具函数
├── web/                 # Web应用
│   ├── __init__.py
│   ├── app.py           # Flask应用入口
│   ├── static/          # 静态资源
│   │   ├── script.js
│   │   └── style.css
│   └── templates/       # 模板文件
│       └── index.html
├── tests/               # 单元测试
│   ├── __init__.py
│   ├── conftest.py      # 测试配置
│   ├── test_binance_api.py  # 币安API测试
│   ├── test_data_processor.py  # 数据处理测试
│   ├── test_utils.py    # 工具函数测试
│   └── test_web_api.py  # Web接口测试
├── data/                # 数据存储目录
├── logs/                # 日志目录
├── main.py              # 主程序入口
├── system_test.py       # 系统测试脚本
├── run_tests.py         # 测试运行脚本
├── requirements.txt     # 依赖包列表
├── .gitignore           # Git忽略文件
└── README.md            # 项目说明
```

## 功能说明

1. **数据获取**: 从币安交易所获取币种持仓量数据
2. **数据持久化**: 将数据保存到本地JSON文件
3. **定时更新**: 自动定时更新数据
4. **数据计算**: 计算不同时间间隔的持仓量变化比例
5. **Web展示**: 在网页上展示数据和变化比例
6. **币种过滤**: 支持按币种名称过滤显示
7. **币种配置管理**: 管理需要跟踪的币种列表

## 重要说明

本系统**不使用任何模拟数据**，所有数据均来自币安交易所的真实API。如果网络连接异常或API不可用，系统将记录错误日志但不会生成虚假数据。

## 安装依赖

```bash
pip install -r requirements.txt
```

## 启动系统

### 方式一：交互式启动

```bash
python main.py
```

然后根据提示选择启动模式：
- 1: 仅启动Web服务
- 2: 仅启动数据更新服务
- 3: 同时启动Web服务和数据更新服务

### 方式二：直接启动Web服务

```bash
python -m web.app
```

访问地址：http://localhost:5000

### 方式三：启动定时数据更新服务

```bash
python -m src.scheduler
```

## 使用说明

1. **查看数据**: 访问Web页面即可查看币种持仓量数据和变化比例
2. **手动更新**: 点击"手动更新数据"按钮可立即更新数据
3. **币种过滤**: 在输入框中输入币种名称（如BTC）进行过滤
4. **自动更新**: 系统默认每5分钟自动更新一次数据

## 币种配置管理

系统提供币种配置管理功能，用户可以：

1. **查看所有币种**：显示币安交易所所有USDT交易对
2. **管理跟踪列表**：通过穿梭框界面添加或移除需要跟踪的币种
3. **从币安更新**：获取最新的币种列表并更新本地配置
4. **保存配置**：保存当前的币种跟踪状态

### 使用方法

1. 访问币种配置页面：http://localhost:5000/coins-config
2. 点击"从币安更新"按钮获取最新币种列表
3. 在左侧列表中选择需要跟踪的币种，点击">>"按钮添加到跟踪列表
4. 在右侧列表中选择不需要跟踪的币种，点击"<<"按钮移除
5. 配置会自动保存，无需手动点击保存按钮

### "从币安更新"功能说明

"从币安更新"功能用于从币安交易所获取最新的USDT交易对列表，并更新本地币种配置。更新配置后会自动触发数据更新，拉取最新的币种数据。

#### 使用步骤

1. 确保代理服务正在运行（如果在中国大陆）
2. 在应用界面中点击"币种配置管理"进入配置页面
3. 点击"从币安更新"按钮
4. 等待更新完成（可能需要几秒钟）
5. 查看币种列表是否已更新
6. 数据更新完成后，可在主页查看最新的币种数据

#### 常见问题

##### 1. 点击"从币安更新"后没有反应
- 检查代理是否正确配置并运行
- 查看浏览器控制台是否有错误信息
- 检查网络连接是否正常

##### 2. 数据没有更新
- 确认币安API返回的是"TRADING"状态的交易对
- 检查配置文件是否正确保存
- 查看日志文件了解详细信息

##### 3. 更新后数据未显示
- 等待数据更新完成（后台异步更新）
- 刷新页面查看最新数据
- 检查 [data/coins_data.json](file:///d:/CODE/test/coinx/data/coins_data.json) 文件是否包含最新数据

#### 验证更新

可以通过以下方式验证更新是否成功：

1. 检查 [data/coins_config.json](file:///d:/CODE/test/coinx/data/coins_config.json) 文件中的币种数量
2. 查看 [data/coins_data.json](file:///d:/CODE/test/coinx/data/coins_data.json) 文件中的最新数据
3. 查看日志文件中的更新记录

#### 注意事项

- 新添加的币种默认设置为"不跟踪"状态
- 原有的币种跟踪状态会被保留
- 更新操作不会删除本地配置中已有的币种
- 数据更新是异步进行的，更新完成后会自动保存到本地文件

## 数据说明

- **当前持仓量**: 币种当前的持仓量数值
- **变化比例**: 当前持仓量相对于不同时间间隔前的持仓量变化百分比
  - 正数表示增加
  - 负数表示减少

## 配置说明

在 `src/config.py` 中可以修改以下配置：

- `UPDATE_INTERVAL`: 数据更新间隔（秒）
- `TIME_INTERVALS`: 支持的时间间隔列表

## 代理支持

系统支持通过代理连接币安API，默认配置使用本地7897端口代理：
- 代理地址: `127.0.0.1:7897`
- 可在 `src/config.py` 中修改代理配置

系统支持通过环境变量控制代理：
```python
# 是否使用代理 - 可通过环境变量 USE_PROXY 配置，默认为 True
USE_PROXY = os.getenv('USE_PROXY', 'True').lower() == 'true'
```

支持的配置方式：
- 环境变量：`USE_PROXY=True` 或 `USE_PROXY=False`
- 配置文件：修改默认值

## 服务管理

系统提供了完善的服务管理脚本：

### 启动服务
```bash
python start_app.py start
```

### 停止服务
```bash
python start_app.py stop
```

### 重启服务
```bash
python start_app.py restart
```

### 查看服务状态
```bash
python start_app.py status
```

### 自动监控和重启服务
```bash
python monitor_service.py
```

监控脚本会自动检测服务状态，如果服务停止会自动重启。

## 单元测试

本项目包含完整的单元测试，确保代码质量和功能可靠性。

### 运行所有测试

```bash
# 运行测试并生成覆盖率报告
python run_tests.py

# 仅运行测试（不生成覆盖率报告）
python run_tests.py --no-cov
```

### 测试框架

- 使用 `pytest` 作为测试框架
- 使用 `unittest.mock` 进行模拟测试
- 使用 `pytest-cov` 生成测试覆盖率报告

### 测试内容

1. **数据获取模块测试** (`tests/test_binance_api.py`)
   - 测试API调用成功和失败的情况
   - 测试网络异常时的处理机制
   - 测试数据更新功能
   - **不使用模拟数据**，所有测试基于真实API响应的模拟

2. **数据处理模块测试** (`tests/test_data_processor.py`)
   - 测试币种数据处理逻辑
   - 测试变化比例计算
   - 测试边界条件处理
   - 测试异常数据处理（如None值、空数据等）

3. **工具函数测试** (`tests/test_utils.py`)
   - 测试数据保存和加载功能
   - 测试变化比例计算函数
   - 测试异常处理机制

4. **Web接口测试** (`tests/test_web_api.py`)
   - 测试API接口的正确性
   - 测试页面访问功能
   - 测试错误处理

### 测试覆盖率

运行测试后，会在 `htmlcov/` 目录生成详细的覆盖率报告，可以通过打开 `htmlcov/index.html` 查看。

## 系统测试

运行系统测试脚本验证各功能模块：

```bash
python system_test.py
```

## 日志查看

系统日志保存在 `logs/app.log` 文件中，可查看数据获取和处理的详细信息。

## 数据存储

所有币种数据保存在 `data/coins_data.json` 文件中，包含历史记录。

## 项目更新日志

### 2025-09-25

#### 1. 代理配置增强
- 修改了 [src/config.py](file:///d:/CODE/test/coinx/src/config.py) 文件，支持通过环境变量控制代理：
  ```python
  # 是否使用代理 - 可通过环境变量 USE_PROXY 配置，默认为 True
  USE_PROXY = os.getenv('USE_PROXY', 'True').lower() == 'true'
  ```
- 支持的配置方式：
  - 环境变量：`USE_PROXY=True` 或 `USE_PROXY=False`
  - 配置文件：修改默认值

#### 2. 币安API错误处理增强
- 增强了 [src/coin_manager.py](file:///d:/CODE/test/coinx/src/coin_manager.py) 中的错误处理机制：
  - 添加响应内容类型检查
  - 增加专门的JSON解析错误处理
  - 改进网络请求异常处理
  - 增加超时时间（从10秒到15秒）
- 修复了潜在的空指针异常问题

#### 3. API调用修复
- 修复了 [web/templates/index.html](file:///d:/CODE/test/coinx/web/templates/index.html) 中的API调用问题：
  - 将默认的GET请求改为明确指定的POST请求
  - 与后端API定义保持一致
  - 统一了两个页面中相同功能的实现方式

#### 4. 币种状态筛选修复
- 修复了 [src/coin_manager.py](file:///d:/CODE/test/coinx/src/coin_manager.py) 中币种状态筛选问题：
  - 将错误的 `TRACING` 状态改为正确的 `TRADING` 状态
  - 确保能正确获取币安交易所中的活跃交易对

#### 5. 测试文件整理
- 将测试相关的文件移至 [tests](file:///d:/CODE/test/coinx/tests) 目录下，保持项目结构整洁
- 添加了币种更新功能测试脚本 [tests/test_update_coins.py](file:///d:/CODE/test/coinx/tests/test_update_coins.py)

#### 6. "从币安更新"功能增强
- 修复了 [web/app.py](file:///d:/CODE/test/coinx/web/app.py) 中的函数命名冲突问题
- 增强了"从币安更新"功能，使其不仅更新币种配置，还会自动触发数据更新
- 优化了数据更新流程，使用异步线程执行数据更新，避免阻塞Web请求
- 添加了详细的日志记录，便于跟踪更新过程