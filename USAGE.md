# 币种数据监控系统使用说明

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
├── README.md            # 项目说明
└── USAGE.md             # 使用说明
```

## 功能说明

1. **数据获取**: 从币安交易所获取币种持仓量数据
2. **数据持久化**: 将数据保存到本地JSON文件
3. **定时更新**: 自动定时更新数据
4. **数据计算**: 计算不同时间间隔的持仓量变化比例
5. **Web展示**: 在网页上展示数据和变化比例
6. **币种过滤**: 支持按币种名称过滤显示

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

## 数据说明

- **当前持仓量**: 币种当前的持仓量数值
- **变化比例**: 当前持仓量相对于不同时间间隔前的持仓量变化百分比
  - 正数表示增加
  - 负数表示减少

## 配置说明

在 `src/config.py` 中可以修改以下配置：

- `UPDATE_INTERVAL`: 数据更新间隔（秒）
- `TIME_INTERVALS`: 支持的时间间隔列表

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