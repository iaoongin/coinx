const { test, expect } = require('./fixtures');
const { button, heading, testId, visit } = require('./contracts');

test.describe('契约 Smoke', () => {
  test('首页关键元素可见', async ({ page }) => {
    await visit(page, '/');
    await expect(heading(page, '币种数据监控', 1)).toBeVisible();
    await expect(button(page, '↻ 刷新')).toBeVisible();
  });

  test('行情榜关键元素可见', async ({ page }) => {
    await visit(page, '/market-rank');
    await expect(heading(page, '行情榜', 1)).toBeVisible();
    await expect(button(page, '跌幅榜')).toBeVisible();
    await expect(button(page, '涨幅榜')).toBeVisible();
  });

  test('币种配置关键元素可见', async ({ page }) => {
    await visit(page, '/coins-config');
    await expect(heading(page, '币种配置管理', 1)).toBeVisible();
    await expect(button(page, '从币安更新币种')).toBeVisible();
  });

  test('历史序列关键元素可见', async ({ page }) => {
    await visit(page, '/binance-series');
    await expect(heading(page, 'Binance 历史序列管理', 1)).toBeVisible();
    await expect(button(page, '执行缺口修补')).toBeVisible();
    await expect(button(page, '执行单条采集')).toBeVisible();
  });

  test('对冲计算器关键元素可见', async ({ page }) => {
    await visit(page, '/hedge-calculator');
    await expect(heading(page, '对冲计算器', 1)).toBeVisible();
    await expect(testId(page, 'long-quantity')).toBeVisible();
    await expect(testId(page, 'short-quantity')).toBeVisible();
    await expect(testId(page, 'result-banner')).toContainText('请输入多单和空单的数量、开仓价格后才能计算平衡价。');
  });
});
