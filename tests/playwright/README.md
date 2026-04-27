# Playwright 测试契约规范

这份文档定义 `tests/playwright/` 目录下测试的统一写法。目标不是绑定页面实现细节，而是绑定稳定交互契约。

## 目的

- 页面改版时，优先只影响视觉和布局，不轻易打红测试。
- 测试只关注用户可感知的交互和结果。
- 页面结构、组件库、class 名称可以调整，但稳定契约要保持一致。

## 定位规则

优先级从高到低：

1. `getByRole`
2. `getByLabel`
3. `data-testid`

禁止在页面级 spec 里直接依赖下面这类选择器：

- `.el-menu-item`
- `.nav > a`
- `.header-row`
- `.content > div:nth-child(...)`
- 任何仅表示样式或布局的 class / 结构选择器

## `data-testid` 约定

- 只挂在最终交互节点上，不挂在外层容器。
- 输入框直接定位到 `<input data-testid="...">`。
- 结果区、错误区、状态区如果需要稳定测试点，直接挂在承载文本的最终节点上。
- 不要让测试先点容器，再二次下钻到真正的交互元素。

## 共享 Helper

页面级 spec 不直接散落选择器，统一通过 helper 访问。

当前 helper 位于：

- [`tests/playwright/tests/contracts.js`](/Users/xhx-mbp/Code/project/coinx/tests/playwright/tests/contracts.js)

建议按下面方式使用：

```js
const { visit, button, link, heading, testId } = require('./contracts');
```

helper 里只保留稳定、可复用的交互契约，不要放页面特有的临时实现。

## 页面公开契约

每个页面都要明确“对外公开”的测试契约。

### 导航

- 导航项名称固定
- 导航项 `href` 固定
- 点击后能跳转到对应页面

当前导航契约包括：

- `首页`
- `行情榜`
- `历史序列`
- `对冲计算器`
- `币种配置`

### 对冲计算器

稳定契约包括：

- 页面标题
- 表单字段名
- 结果 banner 文案
- 错误 / 状态提示文案
- 关键结果字段

当前关键 `data-testid` 包括：

- `long-quantity`
- `long-entry-price`
- `short-quantity`
- `short-entry-price`
- `long-status`
- `short-status`
- `result-banner`
- `breakeven-price`
- `long-pnl`
- `short-pnl`
- `net-pnl`
- `constant-net-pnl`

### 首页 / 行情榜 / 币种配置 / 历史序列

每个主页面的 smoke 测试只检查：

- 关键 heading 是否存在
- 关键操作是否存在
- 关键数据区是否可见

不要把 smoke 测试写成布局测试，也不要把它写成视觉回归测试。

## 改版时的最低验收标准

1. 只改 CSS，不应影响测试。
2. 只改 DOM 结构，但不改契约，不应影响测试。
3. 改了契约字段、文案或交互语义时，必须同步更新 helper 和 spec。

## 新增或修改页面时的做法

1. 先定义页面契约。
2. 在模板里为最终交互节点补稳定的 role / label / `data-testid`。
3. 如有多个 spec 复用，先补到 `contracts.js`。
4. 页面级测试只调用 helper，不直接写零散选择器。
5. 再补一个最小 smoke 检查，确认关键元素可见。

## 常见失败原因

- 组件库升级后，DOM 包装层变了，但业务没变。
- 页面加了图标或前缀，导致文本精确匹配失效。
- 结果区改了容器结构，测试还在找旧 class。
- 输入框被包了一层，测试仍按旧层级去点。

## 当前测试入口

在 `tests/playwright/` 下运行：

```bash
npm test
```

也可以直接跑单个文件：

```bash
npx playwright test tests/navigation.spec.js
```

