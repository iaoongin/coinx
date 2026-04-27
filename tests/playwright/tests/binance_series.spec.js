const { test, expect } = require('./fixtures');
const { button, heading, visit } = require('./contracts');

test.describe('历史序列测试', () => {
  test('页面加载', async ({ page }) => {
    await visit(page, '/binance-series');
    await expect(heading(page, 'Binance 历史序列管理', 1)).toBeVisible();
  });

  test('核心操作区域显示', async ({ page }) => {
    await visit(page, '/binance-series');
    await expect(page.getByRole('heading', { level: 2, name: '缺口修补' })).toBeVisible();
    await expect(page.getByRole('heading', { level: 2, name: '手动采集' })).toBeVisible();
    await expect(page.locator('body')).toContainText('执行缺口修补');
    await expect(page.locator('body')).toContainText('执行单条采集');
    await expect(button(page, '执行缺口修补')).toBeVisible();
    await expect(button(page, '执行单条采集')).toBeVisible();
  });
});
