const { test, expect } = require('./fixtures');
const { button, heading, link, visit } = require('./contracts');

test.describe('首页测试', () => {
  test('页面加载', async ({ page }) => {
    await visit(page, '/');
    await expect(heading(page, '多周期矩阵', 1)).toBeVisible();
    await expect(link(page, '首页')).toBeVisible();
  });

  test('Vue已加载', async ({ page }) => {
    await visit(page, '/');
    const vueLoaded = await page.evaluate(() => typeof Vue !== 'undefined');
    expect(vueLoaded).toBe(true);
  });

  test('首页渲染了币种数据', async ({ page }) => {
    await visit(page, '/');
    await expect(page.locator('body')).toContainText('BTC');
    await expect(page.locator('body')).toContainText('BTC · 持仓量 1.23M');
    await expect(page.locator('body')).toContainText('1.23M');
    await expect(page.locator('body')).toContainText('0.89M(72%)');
    await expect(page.locator('body')).toContainText('0.34M(28%)');
    await expect(page.locator('body')).toContainText('0.12M(剔除)');
  });

  test('首页标题与状态操作在同一标题行', async ({ page }) => {
    await visit(page, '/');
    await expect(heading(page, '多周期矩阵', 1)).toBeVisible();
    await expect(page.getByText('更新时间')).toBeVisible();
    await expect(page.getByText('下次窗口')).toBeVisible();
    await expect(button(page, '刷新')).toBeVisible();
    await expect(page.locator('.coin-meta-line')).toBeVisible();
    await expect(page.locator('body')).not.toContainText('采集：');
  });
});
