const { test, expect } = require('./fixtures');
const { button, heading, visit } = require('./contracts');

test.describe('币种配置测试', () => {
  test('页面加载', async ({ page }) => {
    await visit(page, '/coins-config');
    await expect(heading(page, '币种配置管理', 1)).toBeVisible();
  });

  test('配置列表显示', async ({ page }) => {
    await visit(page, '/coins-config');
    await expect(page.locator('body')).toContainText('从币安更新币种');
    await expect(page.locator('body')).toContainText('所有币种');
    await expect(page.locator('body')).toContainText('跟踪的币种');
    await expect(page.locator('body')).toContainText('SOLUSDT');
    await expect(page.locator('body')).toContainText('BTCUSDT');
    await expect(page.locator('body')).toContainText('ETHUSDT');
  });

  test('更新按钮位于标题行右侧', async ({ page }) => {
    await visit(page, '/coins-config');

    await expect(heading(page, '币种配置管理', 1)).toBeVisible();
    await expect(button(page, '从币安更新币种')).toBeVisible();
  });
});
