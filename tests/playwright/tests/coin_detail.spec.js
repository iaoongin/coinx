const { test, expect } = require('./fixtures');
const { button, heading, visit } = require('./contracts');

test.describe('币种详情测试', () => {
  test('详情页加载', async ({ page }) => {
    await visit(page, '/coin-detail?symbol=BTCUSDT');
    await expect(heading(page, '合约详情', 1)).toBeVisible();
  });

  test('详情页渲染了关键指标', async ({ page }) => {
    await visit(page, '/coin-detail?symbol=BTCUSDT');
    await expect(page.locator('body')).toContainText('最新价格');
    await expect(page.locator('body')).toContainText('$69,234.12');
    await expect(page.locator('body')).toContainText('+0.08%');
    await expect(page.locator('body')).toContainText('72.4');
    await expect(page.locator('body')).toContainText('binance');
    await expect(page.locator('body')).toContainText('bybit');
    await expect(page.locator('body')).toContainText('数据完整');
    await expect(button(page, '返回')).toBeVisible();
  });

  test('多周期变化沿用首页矩阵列序', async ({ page }) => {
    await visit(page, '/coin-detail?symbol=BTCUSDT');
    const matrix = page.locator('.period-matrix');
    await expect(matrix).toBeVisible();
    await expect(matrix).toContainText('窗口');
    await expect(matrix).toContainText('净流入');
    await expect(matrix).toContainText('价格');
    await expect(matrix).toContainText('价格%');
    await expect(matrix).toContainText('量');
    await expect(matrix).toContainText('量%');
    await expect(matrix).toContainText('价值');
    await expect(matrix).toContainText('价值%');
  });

  test('可通过搜索下拉框切换合约并记录最近浏览', async ({ page }) => {
    await visit(page, '/coin-detail?symbol=BTCUSDT');
    await page.getByRole('button', { name: '搜索并切换合约' }).click();
    const picker = page.getByRole('searchbox', { name: '搜索合约' });
    await picker.fill('SOL');
    await page.getByRole('button', { name: 'SOLUSDT', exact: true }).click();
    await expect(page).toHaveURL(/\/coin-detail\?symbol=SOLUSDT$/);
    await expect(page.locator('h1')).toContainText('SOLUSDT');

    await page.getByRole('button', { name: '搜索并切换合约' }).click();
    await expect(page.getByText('最近浏览', { exact: true })).toBeVisible();
    await expect(page.locator('.symbol-picker-menu')).toContainText('SOLUSDT');
  });

  test('移动端布局不产生横向溢出', async ({ page }) => {
    await page.setViewportSize({ width: 390, height: 844 });
    await visit(page, '/coin-detail?symbol=BTCUSDT');
    const dimensions = await page.locator('html').evaluate(element => ({
      clientWidth: element.clientWidth,
      scrollWidth: element.scrollWidth,
    }));
    expect(dimensions.scrollWidth).toBeLessThanOrEqual(dimensions.clientWidth);
    await expect(page.locator('.detail-header')).toHaveCSS('flex-direction', 'column');
    await expect(page.locator('.actions .btn')).toHaveCount(2);
    await expect(page.locator('.chart')).toHaveCSS('height', '260px');
    await page.getByRole('button', { name: '搜索并切换合约' }).click();
    const menuBounds = await page.locator('.symbol-picker-menu').evaluate(element => {
      const rect = element.getBoundingClientRect();
      return { left: rect.left, right: rect.right, viewportWidth: window.innerWidth };
    });
    expect(menuBounds.left).toBeGreaterThanOrEqual(0);
    expect(menuBounds.right).toBeLessThanOrEqual(menuBounds.viewportWidth);
  });

  test('渲染结构评分与历史趋势并支持切换范围', async ({ page }) => {
    await visit(page, '/coin-detail?symbol=BTCUSDT');
    await expect(page.getByText('市场结构评分', { exact: true })).toBeVisible();
    await expect(page.getByText('趋势', { exact: true }).first()).toBeVisible();
    await expect(page.locator('.chart canvas')).toHaveCount(4);
    const chartWidths = await page.locator('.chart canvas').evaluateAll(canvases => canvases.map(canvas => canvas.getBoundingClientRect().width));
    expect(chartWidths.every(width => width > 300)).toBeTruthy();

    const seriesRequest = page.waitForRequest(request => request.url().includes('/series?range=4h'));
    await page.getByRole('button', { name: '4h', exact: true }).click();
    await seriesRequest;
    await expect(page.locator('.segment.active')).toHaveText('4h');
  });
});
