# Vue 3 CDN 迁移计划

## 概述

将现有的原生 JavaScript + Jinja2 模板的前端项目，迁移到 Vue 3 CDN + Element Plus UI 组件库。

## 技术选型

| 技术 | 选择 | 理由 |
|------|------|------|
| Vue 版本 | Vue 3 CDN | 完整 Vue 能力，无需编译 |
| UI 库 | Element Plus | 社区活跃（27.3k stars），CDN 支持完整，桌面端优秀 |
| 测试框架 | Playwright | 可直接测试本地 HTML，支持 Vue 组件测试 |
| 架构 | 多页面应用（非 SPA） | 保持多页面，Vue 只负责单页内交互 |

## 导航菜单顺序

1. 首页 `/`
2. 行情榜 `/market-rank`
3. 历史序列 `/binance-series`
4. 币种配置 `/coins-config`

---

## 测试方案

### 测试框架配置

**tests/playwright/playwright.config.js**
```javascript
const { defineConfig } = require('@playwright/test');

module.exports = defineConfig({
  testDir: './tests',
  timeout: 30000,
  use: {
    baseURL: 'http://localhost:5000',
    headless: true,
    viewport: { width: 1280, height: 720 },
  },
  webServer: {
    command: 'python src/coinx/main.py',
    port: 5000,
    reuseExistingServer: !process.env.CI,
  },
});
```

### 测试用例清单

#### vue.spec.js - Vue 加载测试
```javascript
test('Vue CDN加载', async ({ page }) => {
  await page.goto('http://localhost:5000/');
  const vueExists = await page.evaluate(() => typeof Vue !== 'undefined');
  expect(vueExists).toBe(true);
});

test('Vue实例挂载', async ({ page }) => {
  await page.goto('http://localhost:5000/');
  await page.waitForSelector('#app');
  const hasContent = await page.evaluate(() => {
    const app = document.getElementById('app');
    return app && app.children.length > 0;
  });
  expect(hasContent).toBe(true);
});

test('Element Plus加载', async ({ page }) => {
  await page.goto('http://localhost:5000/');
  const elExists = await page.evaluate(() => {
    return typeof ElementPlus !== 'undefined' || 
           document.querySelector('.el-menu') !== null;
  });
  expect(elExists).toBe(true);
});

test('响应式数据渲染', async ({ page }) => {
  await page.goto('http://localhost:5000/');
  await page.waitForSelector('.el-table');
  const rowCount = await page.locator('.el-table__body tr').count();
  expect(rowCount).toBeGreaterThan(0);
});
```

#### navigation.spec.js - 导航测试
```javascript
test('导航菜单显示', async ({ page }) => {
  await page.goto('http://localhost:5000/');
  await expect(page.locator('text=首页')).toBeVisible();
  await expect(page.locator('text=行情榜')).toBeVisible();
  await expect(page.locator('text=历史序列')).toBeVisible();
  await expect(page.locator('text=币种配置')).toBeVisible();
});

test('导航菜单高亮', async ({ page }) => {
  await page.goto('http://localhost:5000/');
  await expect(page.locator('.el-menu-item:has-text("首页")')).toHaveClass(/is-active/);
});

test('导航菜单跳转', async ({ page }) => {
  await page.goto('http://localhost:5000/');
  await page.click('text=行情榜');
  await expect(page).toHaveURL(/market-rank/);
});
```

#### index.spec.js - 首页测试
```javascript
test('页面加载', async ({ page }) => {
  await page.goto('http://localhost:5000/');
  await page.waitForSelector('#app');
  await expect(page.locator('text=币种数据监控')).toBeVisible();
});

test('表格数据渲染', async ({ page }) => {
  await page.goto('http://localhost:5000/');
  await page.waitForSelector('.el-table');
  const rows = await page.locator('.el-table tbody tr').count();
  expect(rows).toBeGreaterThan(0);
});

test('弹窗打开', async ({ page }) => {
  await page.goto('http://localhost:5000/');
  await page.click('text=币种配置');
  await expect(page.locator('.el-dialog')).toBeVisible();
});
```

#### market_rank.spec.js - 行情榜测试
```javascript
test('排行类型切换', async ({ page }) => {
  await page.goto('http://localhost:5000/market-rank');
  await page.click('text=涨幅榜');
});

test('表格数据', async ({ page }) => {
  await page.goto('http://localhost:5000/market-rank');
  await page.waitForSelector('.el-table');
});
```

#### coins_config.spec.js - 币种配置测试
```javascript
test('穿梭框左右移动', async ({ page }) => {
  await page.goto('http://localhost:5000/coins-config');
  await page.waitForSelector('.el-transfer');
});

test('搜索功能', async ({ page }) => {
  await page.goto('http://localhost:5000/coins-config');
  await page.fill('input[placeholder="搜索币种..."]', 'BTC');
});
```

#### binance_series.spec.js - 历史序列测试
```javascript
test('表单提交', async ({ page }) => {
  await page.goto('http://localhost:5000/binance-series');
  await page.click('text=执行单条采集');
});
```

#### coin_detail.spec.js - 币种详情测试
```javascript
test('详情页加载', async ({ page }) => {
  await page.goto('http://localhost:5000/coin-detail?symbol=BTCUSDT');
  await expect(page.locator('text=BTCUSDT 合约详情')).toBeVisible();
});
```

### 验收标准

| 检查项 | 标准 |
|--------|------|
| Vue CDN 加载 | Vue 全局对象存在 |
| Vue 实例挂载 | #app 元素有内容 |
| Element Plus 加载 | UI 组件正常渲染 |
| 导航栏 | 4个菜单项显示正确 |
| 导航高亮 | 当前页面对应菜单高亮 |
| 导航跳转 | 点击跳转正确页面 |
| 表格渲染 | 数据正确显示 |
| 弹窗交互 | 打开/关闭正常 |
| 穿梭框 | 左右移动正常 |
| 响应式 | 移动端布局正常 |

---

## 执行计划

### 阶段 1：测试环境配置

| 步骤 | 内容 | 预计时间 |
|------|------|----------|
| 1.1 | 安装 Playwright | 2分钟 |
| 1.2 | 创建 `tests/playwright/playwright.config.js` | 2分钟 |
| 1.3 | 创建测试目录结构 `tests/playwright/tests/` | 1分钟 |
| 1.4 | 编写基础测试用例 | 10分钟 |
| 1.5 | 验证测试可运行 | 3分钟 |

### 阶段 2：创建公共导航组件

| 步骤 | 内容 | 预计时间 |
|------|------|----------|
| 2.1 | 创建 `templates/nav.html` | 5分钟 |
| 2.2 | 使用 Element Plus el-menu | - |

### 阶段 3：替换各页面菜单

| 步骤 | 页面 | 预计时间 |
|------|------|----------|
| 3.1 | index.html | 3分钟 |
| 3.2 | market_rank.html | 3分钟 |
| 3.3 | coins_config.html | 3分钟 |
| 3.4 | binance_series.html | 3分钟 |

### 阶段 4：Vue 迁移

| 步骤 | 页面 | 复杂度 | 预计时间 |
|------|------|--------|----------|
| 4.1 | binance_series.html | 低 | 1小时 |
| 4.2 | coin_detail.html | 中 | 1小时 |
| 4.3 | coins_config.html | 中 | 1-2小时 |
| 4.4 | market_rank.html | 高 | 2-3小时 |
| 4.5 | index.html | 高 | 2-3小时 |

### 阶段 5：E2E 测试验收

| 步骤 | 内容 | 预计时间 |
|------|------|----------|
| 5.1 | 运行所有测试用例 | 5分钟 |
| 5.2 | 修复问题 | - |
| 5.3 | 最终验收 | - |

---

## 改动内容

### 1. 引入 CDN
```html
<!-- Vue 3 -->
<script src="https://unpkg.com/vue@3/dist/vue.global.js"></script>

<!-- Element Plus -->
<link rel="stylesheet" href="https://unpkg.com/element-plus/dist/index.css" />
<script src="https://unpkg.com/element-plus/dist/index.full.js"></script>
```

### 2. HTML 结构改动
| 原写法 | Vue 写法 |
|--------|----------|
| `innerHTML = '<tr>...</tr>'` | `v-for` 循环 |
| `document.getElementById` | `ref()` 响应式变量 |
| `onclick="func()"` | `@click` |
| 手写弹窗 | `<el-dialog>` |
| 手写穿梭框 | `<el-transfer>` |
| 手写表格 | `<el-table>` |

### 3. JavaScript 改动
```javascript
// 原写法
let coinsData = [];
function renderData(data) {
  container.innerHTML = '...';
}

// Vue 写法
const coinsData = ref([]);
// 响应式，模板自动更新
```

### 4. CSS
- 保留 70% 自定义样式
- 替换 30% UI 组件样式

---

## 总计

- 测试配置：约 20 分钟
- 导航组件：约 10 分钟
- 菜单替换：约 15 分钟
- Vue 迁移：约 8-10 小时
- 测试验收：约 30 分钟
