const { test, expect } = require('./fixtures');

test.describe('币种配置测试', () => {
  test('页面加载', async ({ page }) => {
    await page.goto('/coins-config');
    await page.waitForLoadState('networkidle');
    await expect(page.locator('h1')).toContainText('币种配置');
  });

  test('配置列表显示', async ({ page }) => {
    await page.goto('/coins-config');
    await page.waitForLoadState('networkidle');
    await expect(page.locator('body')).toContainText('从币安更新币种');
    await expect(page.locator('body')).toContainText('所有币种');
    await expect(page.locator('body')).toContainText('跟踪的币种');
    await expect(page.locator('body')).toContainText('SOLUSDT');
    await expect(page.locator('body')).toContainText('BTCUSDT');
    await expect(page.locator('body')).toContainText('ETHUSDT');
  });

  test('更新按钮位于标题行右侧', async ({ page }) => {
    await page.goto('/coins-config');
    await page.waitForLoadState('networkidle');

    const header = page.locator('.header');
    await expect(header.locator('h1')).toContainText('币种配置管理');
    await expect(header.getByRole('button', { name: '从币安更新币种' })).toBeVisible();
  });
});
