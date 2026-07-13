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
    await expect(page.locator('body')).toContainText('BTC · 持仓价值 85.43M');
    await expect(page.locator('body')).toContainText('85.43M');
    await expect(page.locator('body')).toContainText('$62.00M(72%)');
    await expect(page.locator('body')).toContainText('$23.43M(28%)');
    await expect(page.locator('body')).toContainText('$8.40M(剔除)');
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
  test('点击资费标签展示 24 小时走势图并支持 Esc 关闭', async ({ page }) => {
    const historyRequest = page.waitForRequest((request) => {
      const url = new URL(request.url());
      return url.pathname === '/api/funding-rate/history/BTCUSDT' && url.searchParams.get('hours') === '24';
    });

    await visit(page, '/');
    await page.locator('.coin-meta-funding').click();
    await historyRequest;

    const dialog = page.getByRole('dialog', { name: /BTCUSDT.*24/ });
    await expect(dialog).toBeVisible();
    await expect(dialog.locator('svg')).toBeVisible();

    await page.keyboard.press('Escape');
    await expect(dialog).toBeHidden();
  });

  test('资费历史为空时展示空状态', async ({ page }) => {
    await page.route('**/api/funding-rate/history/BTCUSDT?hours=24', (route) => route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({ status: 'success', data: [] }),
    }));
    await visit(page, '/');
    await page.locator('.coin-meta-funding').click();
    await expect(page.locator('.funding-chart-state')).toHaveText('暂无历史数据');
  });

  test('资费历史请求失败时展示失败状态', async ({ page }) => {
    await page.route('**/api/funding-rate/history/BTCUSDT?hours=24', (route) => route.fulfill({
      status: 500,
      contentType: 'application/json',
      body: JSON.stringify({ status: 'error' }),
    }));
    await visit(page, '/');
    await page.locator('.coin-meta-funding').click();
    await expect(page.locator('.funding-chart-state')).toHaveText('加载失败，请稍后重试');
  });
});
