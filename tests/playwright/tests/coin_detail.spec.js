const { test, expect } = require('./fixtures');
const { button, heading, visit } = require('./contracts');

test.describe('币种详情测试', () => {
  test('详情页加载', async ({ page }) => {
    await visit(page, '/coin-detail?symbol=BTCUSDT');
    await expect(heading(page, '合约详情', 1)).toBeVisible();
  });

  test('详情页渲染了关键指标', async ({ page }) => {
    await visit(page, '/coin-detail?symbol=BTCUSDT');
    await expect(page.locator('body')).toContainText('最近交易价');
    await expect(page.locator('body')).toContainText('$69234.12000000');
    await expect(page.locator('body')).toContainText('+0.08%');
    await expect(page.locator('body')).toContainText('Binance');
    await expect(page.locator('body')).toContainText('Bybit');
    await expect(button(page, '← 返回')).toBeVisible();
  });
});
