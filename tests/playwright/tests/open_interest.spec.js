const { test, expect } = require('./fixtures');
const { button, heading, visit } = require('./contracts');

test.describe('持仓量概览测试', () => {
  test('页面加载并展示跨交易所累计数据', async ({ page }) => {
    await visit(page, '/open-interest');

    await expect(heading(page, '持仓量概览', 1)).toBeVisible();
    await expect(page.locator('body')).toContainText('BTCUSDT');
    await expect(page.locator('body')).toContainText('binance · bybit');
    await expect(page.locator('body')).toContainText('6.1412%');
  });

  test('筛选控件可以提交查询', async ({ page }) => {
    await visit(page, '/open-interest');

    await page.locator('#oi-keyword').fill('BTC');
    await page.locator('#oi-min-ratio').fill('5');
    await button(page, '查询').click();

    await expect(page.locator('body')).toContainText('BTCUSDT');
    await expect(page.locator('#oi-keyword')).toHaveValue('BTC');
  });

  test('移动端页面保持可横向查看表格', async ({ page }) => {
    await page.setViewportSize({ width: 390, height: 844 });
    await visit(page, '/open-interest');

    await expect(page.locator('.table-wrap')).toBeVisible();
    const overflow = await page.locator('.table-wrap').evaluate((node) => node.scrollWidth > node.clientWidth);
    expect(overflow).toBe(true);
  });
});
