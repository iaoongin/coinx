const { test, expect } = require('./fixtures');

test.describe('币种详情测试', () => {
  test('详情页加载', async ({ page }) => {
    await page.goto('/coin-detail?symbol=BTCUSDT');
    await page.waitForLoadState('networkidle');
    await expect(page.locator('h1')).toContainText('合约详情');
  });

  test('详情页渲染了关键指标', async ({ page }) => {
    await page.goto('/coin-detail?symbol=BTCUSDT');
    await page.waitForLoadState('networkidle');
    await expect(page.locator('body')).toContainText('最近交易价');
    await expect(page.locator('body')).toContainText('$69234.12000000');
    await expect(page.locator('body')).toContainText('+0.08%');
    await expect(page.locator('body')).toContainText('Binance');
    await expect(page.locator('body')).toContainText('Bybit');
  });
});
