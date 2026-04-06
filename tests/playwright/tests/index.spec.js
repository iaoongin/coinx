const { test, expect } = require('./fixtures');

test.describe('首页测试', () => {
  test('页面加载', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
    const html = await page.content();
    expect(html).toContain('vue@3');
    expect(html).toContain('element-plus');
  });

  test('Vue已加载', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
    const vueLoaded = await page.evaluate(() => typeof Vue !== 'undefined');
    expect(vueLoaded).toBe(true);
  });

  test('首页渲染了币种数据', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
    await expect(page.locator('tbody')).toContainText('BTCUSDT');
    await expect(page.locator('tbody')).toContainText('1.23M');
    await expect(page.locator('tbody')).toContainText('85.43M');
    await expect(page.locator('tbody')).toContainText('120.00K');
  });

  test('首页标题与状态操作在同一标题行', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    const header = page.locator('.header-row');
    await expect(header.locator('h1')).toContainText('币种数据监控');
    await expect(header).toContainText('最后更新时间:');
    await expect(header).toContainText('下次刷新倒计时:');
    await expect(header.getByRole('button', { name: '↻ 刷新' })).toBeVisible();
  });
});
