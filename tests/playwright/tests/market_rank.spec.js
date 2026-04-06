const { test, expect } = require('./fixtures');

test.describe('行情榜测试', () => {
  test('页面加载', async ({ page }) => {
    await page.goto('/market-rank');
    await page.waitForLoadState('networkidle');
    await expect(page.locator('h1')).toContainText('行情榜');
  });

  test('排行类型切换', async ({ page }) => {
    await page.goto('/market-rank');
    await page.waitForLoadState('networkidle');
    await page.locator('button').filter({ hasText: '涨幅榜' }).click();
    await expect(page.locator('body')).toContainText('+3.40%');
  });

  test('行情榜渲染了排行数据', async ({ page }) => {
    await page.goto('/market-rank');
    await page.waitForLoadState('networkidle');
    await expect(page.locator('body')).toContainText('BTCUSDT');
    await expect(page.locator('body')).toContainText('ETHUSDT');
    await expect(page.locator('body')).toContainText('-3.40%');
  });

  test('行情榜标题与状态操作在同一标题行，且无内嵌币种配置', async ({ page }) => {
    await page.goto('/market-rank');
    await page.waitForLoadState('networkidle');

    const header = page.locator('.header-row');
    await expect(header.locator('h1')).toContainText('行情榜');
    await expect(header).toContainText('最后更新时间:');
    await expect(header).toContainText('下次刷新倒计时:');
    await expect(header.getByRole('button', { name: '↻ 刷新' })).toBeVisible();
    await expect(page.locator('body')).not.toContainText('币种配置管理');
    await expect(page.locator('#coinsConfigModal')).toHaveCount(0);
  });
});
