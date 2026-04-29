const { test, expect } = require('./fixtures');
const { button, heading, visit } = require('./contracts');

test.describe('行情榜测试', () => {
  test('页面加载', async ({ page }) => {
    await visit(page, '/market-rank');
    await expect(heading(page, '行情榜', 1)).toBeVisible();
  });

  test('排行类型切换', async ({ page }) => {
    await visit(page, '/market-rank');
    await button(page, '涨幅榜').click();
    await expect(page.locator('body')).toContainText('+3.40%');
  });

  test('行情榜渲染了排行数据', async ({ page }) => {
    await visit(page, '/market-rank');
    await expect(page.locator('body')).toContainText('BTCUSDT');
    await expect(page.locator('body')).toContainText('ETHUSDT');
    await expect(page.locator('body')).toContainText('-3.40%');
  });

  test('行情榜刷新按钮会先刷新快照再重新加载数据', async ({ page }) => {
    await visit(page, '/market-rank');
    await expect(page.locator('body')).toContainText('-3.40%');

    await button(page, '↻ 刷新').click();

    await expect(page.locator('body')).toContainText('+3.40%');
  });

  test('行情榜标题与状态操作在同一标题行，且无内嵌币种配置', async ({ page }) => {
    await visit(page, '/market-rank');

    await expect(heading(page, '行情榜', 1)).toBeVisible();
    await expect(page.getByText('最后更新')).toBeVisible();
    await expect(page.getByText('刷新倒计时')).toBeVisible();
    await expect(button(page, '↻ 刷新')).toBeVisible();
    await expect(page.locator('body')).not.toContainText('币种配置管理');
    await expect(page.locator('#coinsConfigModal')).toHaveCount(0);
  });
});
