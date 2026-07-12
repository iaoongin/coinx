const { test, expect } = require('./fixtures');
const { button, heading, visit } = require('./contracts');

test.describe('币种详情测试', () => {
  test('详情页加载', async ({ page }) => {
    await visit(page, '/coin-detail?symbol=BTCUSDT');
    await expect(heading(page, '合约详情', 1)).toBeVisible();
  });

  test('详情页渲染了关键指标', async ({ page }) => {
    await visit(page, '/coin-detail?symbol=BTCUSDT');
    await expect(page.locator('body')).toContainText('最新价格');
    await expect(page.locator('body')).toContainText('$69,234.12');
    await expect(page.locator('body')).toContainText('+0.08%');
    await expect(page.locator('body')).toContainText('72.4');
    await expect(page.locator('body')).toContainText('binance');
    await expect(page.locator('body')).toContainText('bybit');
    await expect(page.locator('body')).toContainText('数据完整');
    await expect(button(page, '返回')).toBeVisible();
  });

  test('渲染结构评分与历史趋势并支持切换范围', async ({ page }) => {
    await visit(page, '/coin-detail?symbol=BTCUSDT');
    await expect(page.getByText('市场结构评分', { exact: true })).toBeVisible();
    await expect(page.getByText('趋势', { exact: true }).first()).toBeVisible();
    await expect(page.locator('.chart canvas')).toHaveCount(3);
    const chartWidths = await page.locator('.chart canvas').evaluateAll(canvases => canvases.map(canvas => canvas.getBoundingClientRect().width));
    expect(chartWidths.every(width => width > 300)).toBeTruthy();

    const seriesRequest = page.waitForRequest(request => request.url().includes('/series?range=4h'));
    await page.getByRole('button', { name: '4h', exact: true }).click();
    await seriesRequest;
    await expect(page.locator('.segment.active')).toHaveText('4h');
  });
});
