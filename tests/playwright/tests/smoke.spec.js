const { test, expect } = require('./fixtures');

test.describe('隔离 Smoke', () => {
  test('首页壳子和导航可以加载', async ({ page }) => {
    await page.goto('/');
    await expect(page.locator('h1')).toContainText('币种数据监控');
    await expect(page.locator('.el-menu')).toContainText('首页');
  });

  test('币种配置页基础元素可加载', async ({ page }) => {
    await page.goto('/coins-config');
    await page.waitForLoadState('networkidle');
    await expect(page.locator('h1')).toContainText('币种配置管理');
    await expect(page.locator('body')).toContainText('从币安更新币种');
  });
});
