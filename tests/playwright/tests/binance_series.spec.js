const { test, expect } = require('./fixtures');

test.describe('历史序列测试', () => {
  test('页面加载', async ({ page }) => {
    await page.goto('/binance-series');
    await page.waitForLoadState('networkidle');
    await expect(page.locator('h1')).toContainText('Binance 历史序列');
  });

  test('核心操作区域显示', async ({ page }) => {
    await page.goto('/binance-series');
    await page.waitForLoadState('networkidle');
    await expect(page.getByRole('heading', { level: 2, name: '缺口修补' })).toBeVisible();
    await expect(page.getByRole('heading', { level: 2, name: '手动采集' })).toBeVisible();
    await expect(page.locator('body')).toContainText('执行缺口修补');
    await expect(page.locator('body')).toContainText('执行单条采集');
  });
});
