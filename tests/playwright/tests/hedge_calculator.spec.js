const { test, expect } = require('./fixtures');

async function fillInput(page, testId, value) {
  const input = page.locator(`[data-testid="${testId}"] input`);
  await input.fill('');
  if (value !== null) {
    await input.fill(String(value));
  }
}

test.describe('对冲计算器页面', () => {
  test('页面基础内容可见', async ({ page }) => {
    await page.goto('/hedge-calculator');
    await page.waitForLoadState('networkidle');

    await expect(page.locator('h1')).toContainText('对冲计算器');
    await expect(page.locator('body')).toContainText('USDT 本位线性合约');
    await expect(page.locator('[data-testid="result-banner"]')).toContainText('数量、开仓价格');
  });

  test('填入示例后显示正确平衡价', async ({ page }) => {
    await page.goto('/hedge-calculator');
    await page.getByRole('button', { name: '填入示例' }).click();

    await expect(page.locator('[data-testid="breakeven-price"]')).toContainText('85000');
    await expect(page.locator('[data-testid="long-pnl"]')).toContainText('-7500.00 USDT');
    await expect(page.locator('[data-testid="short-pnl"]')).toContainText('7500.00 USDT');
    await expect(page.locator('[data-testid="net-pnl"]')).toContainText('0.00 USDT');
  });

  test('输入数量和开仓价格即可计算', async ({ page }) => {
    await page.goto('/hedge-calculator');

    await fillInput(page, 'long-quantity', 0.5);
    await fillInput(page, 'long-entry-price', 100000);
    await fillInput(page, 'short-quantity', 0.3);
    await fillInput(page, 'short-entry-price', 110000);

    await expect(page.locator('[data-testid="long-status"]')).toContainText('输入有效');
    await expect(page.locator('[data-testid="long-status"]')).toContainText('数量 0.5');
    await expect(page.locator('[data-testid="short-status"]')).toContainText('数量 0.3');
    await expect(page.locator('[data-testid="breakeven-price"]')).toContainText('85000');
  });

  test('非法输入时阻止计算', async ({ page }) => {
    await page.goto('/hedge-calculator');

    await fillInput(page, 'long-quantity', -1);
    await fillInput(page, 'long-entry-price', 100);
    await fillInput(page, 'short-quantity', 2);
    await fillInput(page, 'short-entry-price', 120);

    await expect(page.locator('[data-testid="long-status"]')).toContainText('必须大于 0');
    await expect(page.locator('[data-testid="result-banner"]')).toContainText('存在无效输入');
    await expect(page.locator('[data-testid="breakeven-price"]')).toHaveCount(0);
  });

  test('任意价格都平衡时显示对应提示', async ({ page }) => {
    await page.goto('/hedge-calculator');

    await fillInput(page, 'long-quantity', 1);
    await fillInput(page, 'long-entry-price', 100);
    await fillInput(page, 'short-quantity', 1);
    await fillInput(page, 'short-entry-price', 100);

    await expect(page.locator('[data-testid="result-banner"]')).toContainText('任意市场价格下净未实现盈亏都为 0');
  });

  test('数量相同但开仓价不同则不存在平衡价格', async ({ page }) => {
    await page.goto('/hedge-calculator');

    await fillInput(page, 'long-quantity', 1);
    await fillInput(page, 'long-entry-price', 100);
    await fillInput(page, 'short-quantity', 1);
    await fillInput(page, 'short-entry-price', 120);

    await expect(page.locator('[data-testid="result-banner"]')).toContainText('不存在平衡价格');
    await expect(page.locator('[data-testid="constant-net-pnl"]')).toContainText('20.00 USDT');
  });

  test('输入不足时只显示提示', async ({ page }) => {
    await page.goto('/hedge-calculator');

    await fillInput(page, 'long-quantity', 1);

    await expect(page.locator('[data-testid="long-status"]')).toContainText('需要同时填写数量和开仓价格');
    await expect(page.locator('[data-testid="result-banner"]')).toContainText('数量、开仓价格');
    await expect(page.locator('[data-testid="breakeven-price"]')).toHaveCount(0);
  });
});
