const { test, expect } = require('./fixtures');
const { button, heading, visit } = require('./contracts');

test.describe('结构评分页面', () => {
  test('页面加载并展示评分列表', async ({ page }) => {
    await visit(page, '/market-structure-score');
    await expect(heading(page, '合约市场结构评分', 1)).toBeVisible();
    await expect(page.locator('body')).toContainText('BTCUSDT');
    await expect(page.locator('body')).toContainText('强多');
    await expect(button(page, '刷新评分')).toBeVisible();
    await expect(page.locator('select').first()).toHaveValue('100');
  });

  test('可展开查看交易所明细与原始输入', async ({ page }) => {
    await visit(page, '/market-structure-score');

    await button(page, '展开').first().click();

    const expandedRow = page.locator('.expand-row');
    await expect(expandedRow.getByRole('columnheader', { name: '交易所' })).toBeVisible();
    await expect(expandedRow.getByRole('columnheader', { name: 'OI 占比' })).toBeVisible();
    await expect(expandedRow.getByRole('columnheader', { name: '当前价格' })).toBeVisible();
    await expect(expandedRow.getByRole('columnheader', { name: '资金费率' })).toBeVisible();
    await expect(expandedRow.getByRole('columnheader', { name: '平均振幅' })).toBeVisible();
    await expect(page.locator('body')).toContainText('binance');
    await expect(page.locator('body')).toContainText('EMA20');
    await expect(page.locator('body')).toContainText('OI 变化');
    await expect(page.locator('body')).toContainText('主动买卖压力');
    await expect(page.locator('body')).toContainText('成交量放大');
  });

  test('移动端下仍可展开查看交易所表格', async ({ page }) => {
    await page.setViewportSize({ width: 390, height: 844 });
    await visit(page, '/market-structure-score');

    await expect(heading(page, '合约市场结构评分', 1)).toBeVisible();
    await button(page, '展开').first().click();

    const expandedRow = page.locator('.expand-row');
    await expect(expandedRow.getByRole('columnheader', { name: '交易所' })).toBeVisible();
    await expect(expandedRow.getByRole('columnheader', { name: 'OI 持仓价值' })).toBeVisible();
    await expect(page.locator('body')).toContainText('当前价格');
    await expect(button(page, '收起').first()).toBeVisible();
  });
});
