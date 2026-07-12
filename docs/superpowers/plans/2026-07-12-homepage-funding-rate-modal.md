# 首页资费走势模态框实施计划

> **执行要求：** 必须使用 `superpowers:subagent-driven-development`（推荐）或 `superpowers:executing-plans`，按任务逐项执行。所有步骤使用复选框跟踪。

**目标：** 用户点击首页资费标签后，打开响应式模态框，展示该币种近 24 小时的资费走势图。

**架构：** 在首页现有 Vue 应用中增加模态框状态、历史数据加载、ECharts 渲染及资源清理逻辑。复用现有资费历史接口并保持与资费页相同的图表语义，不修改后端。

**技术栈：** Flask/Jinja、Vue 3 Composition API、ECharts、pytest、Playwright。

## 全局约束

- 使用 `GET /api/funding-rate/history/<symbol>?hours=24`，不新增 API。
- 模态框只展示走势图，包含结算费率和预测费率两条曲线。
- 支持关闭按钮、遮罩、`Esc`、键盘激活、焦点归还，以及加载、空数据和失败状态。
- 图表跟随窗口尺寸调整；关闭模态框、切换币种或卸载组件时销毁图表实例。

---

### 任务一：首页模态框契约与实现

**文件：**
- 修改：`tests/test_homepage_template.py`
- 修改：`src/coinx/web/templates/index.html`

**接口：**
- 输入：首页币种对象的 `symbol`、`funding_rate` 和 `funding_rate_formatted` 字段。
- 输出：`openFundingModal(coin, event)`、`closeFundingModal()`、`renderFundingChart(container, historyData)` 及模态框响应式状态。

- [ ] **步骤 1：编写失败的模板测试**

```python
def test_homepage_funding_label_opens_24_hour_chart_modal():
    template = Path('src/coinx/web/templates/index.html').read_text(encoding='utf-8')
    assert '@click="openFundingModal(coin, $event)"' in template
    assert 'role="dialog"' in template
    assert "'/api/funding-rate/history/' + encodeURIComponent(symbol) + '?hours=24'" in template
    assert "name: '结算费率'" in template
    assert "name: '预测费率'" in template
```

- [ ] **步骤 2：运行聚焦测试并确认失败**

运行：`python -m pytest tests/test_homepage_template.py -v`

预期：测试因首页尚无模态框契约而失败。

- [ ] **步骤 3：实现最小可用功能**

在首页加入可通过键盘操作的资费标签、对话框结构、固定高度图表容器及响应式样式。加入模态框状态、历史接口请求、ECharts 双折线渲染、窗口尺寸处理、`Esc` 处理、焦点归还和图表销毁逻辑，并从 `setup()` 暴露模板所需的方法。

- [ ] **步骤 4：再次运行聚焦测试**

运行：`python -m pytest tests/test_homepage_template.py -v`

预期：全部通过。

- [ ] **步骤 5：提交任务一**

```bash
git add tests/test_homepage_template.py src/coinx/web/templates/index.html
git commit -m "feat(homepage): add funding rate chart modal"
```

### 任务二：浏览器交互覆盖

**文件：**
- 修改：`tests/playwright/tests/index.spec.js`

**接口：**
- 输入：`.coin-meta-funding`、`[role="dialog"]`、`.funding-modal-close`、`.funding-chart-state` 以及资费历史接口。
- 输出：覆盖打开、请求、图表、空数据、失败和关闭行为的浏览器回归测试。

- [ ] **步骤 1：编写失败的 Playwright 测试**

为首页数据和资费历史数据设置路由桩。验证点击标签后请求包含 `hours=24`，打开正确币种的模态框并渲染非空图表画布；验证关闭按钮、遮罩和 `Esc`；验证空响应和失败响应显示对应文案。

- [ ] **步骤 2：运行浏览器测试并确认失败**

运行：`npx playwright test tests/index.spec.js`

预期：尚未满足的浏览器行为导致测试失败。

- [ ] **步骤 3：按失败结果补齐实现**

保持测试选择器稳定；阻止对话框内部点击触发遮罩关闭；确保 Vue 渲染图表容器后再初始化 ECharts；明确区分空数据与请求失败状态。

- [ ] **步骤 4：运行相关验证**

运行：`python -m pytest tests/test_homepage_template.py tests/test_funding_rate_template.py -v`

预期：全部通过。

运行：`npx playwright test tests/index.spec.js`

预期：全部通过。

- [ ] **步骤 5：提交任务二**

```bash
git add tests/playwright/tests/index.spec.js src/coinx/web/templates/index.html
git commit -m "test(homepage): cover funding chart modal"
```
