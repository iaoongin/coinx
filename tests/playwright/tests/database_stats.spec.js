const { test, expect } = require('./fixtures');

test.describe('数据存储页面', () => {
  test('展示表注释和空间统计，并支持筛选', async ({ page }) => {
    await page.goto('/database-stats');
    await page.waitForLoadState('networkidle');

    await expect(page.getByRole('heading', { name: '数据存储', exact: true })).toBeVisible();
    await expect(page.getByText('多交易所K线历史数据表', { exact: true })).toBeVisible();
    await expect(page.getByText('2.00 GiB', { exact: true }).first()).toBeVisible();
    await expect(page.locator('.storage-table tbody tr')).toHaveCount(3);

    await page.getByRole('searchbox', { name: '搜索表名或表注释' }).fill('聪明钱');
    await expect(page.locator('.storage-table tbody tr')).toHaveCount(1);
    await expect(page.locator('.storage-table tbody tr')).toContainText('market_futures_smart_money_signal');
  });

  test('所有表格列使用箭头指示排序方向', async ({ page }) => {
    await page.goto('/database-stats');
    await page.waitForLoadState('networkidle');

    await expect(page.getByRole('button', { name: /已用空间/ })).toContainText('↓');
    await expect(page.locator('.sort-button')).not.toContainText('desc');
    await expect(page.locator('.sort-button')).not.toContainText('asc');

    const dataSizeButton = page.getByRole('button', { name: /数据空间/ });
    await dataSizeButton.click();
    await expect(dataSizeButton).toContainText('↓');
    await dataSizeButton.click();
    await expect(dataSizeButton).toContainText('↑');
  });

  test('移动端保持页面可读并允许横向查看明细表', async ({ page }) => {
    await page.setViewportSize({ width: 390, height: 844 });
    await page.goto('/database-stats');
    await page.waitForLoadState('networkidle');

    await expect(page.getByRole('heading', { name: '数据存储', exact: true })).toBeVisible();
    const tableScroll = page.locator('.table-scroll');
    await expect(tableScroll).toBeVisible();
    const metrics = await tableScroll.evaluate((element) => ({
      clientWidth: element.clientWidth,
      scrollWidth: element.scrollWidth,
    }));
    expect(metrics.scrollWidth).toBeGreaterThan(metrics.clientWidth);
  });
});
