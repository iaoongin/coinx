const { test, expect } = require('./fixtures');
const { NAV_ITEMS, navLinks, visit } = require('./contracts');

const pages = ['/', '/market-rank', '/binance-series', '/hedge-calculator', '/coins-config', '/coin-detail?symbol=BTCUSDT'];

test.describe('导航栏契约', () => {
  test('导航链接文本和 href 稳定', async ({ page }) => {
    await visit(page, '/');

    const items = navLinks(page);
    expect(items).toHaveLength(NAV_ITEMS.length);

    for (const item of items) {
      await expect(item.locator).toBeVisible();
      await expect(item.locator).toHaveAttribute('href', item.href);
      await expect(item.locator).toContainText(item.name);
    }
  });

  test('点击导航可以跳转到对应页面', async ({ page }) => {
    await visit(page, '/');

    for (const item of NAV_ITEMS) {
      await page.getByRole('link', { name: item.name }).click();
      await expect(page).toHaveURL(new RegExp(`${item.href.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}($|\\?)`));
    }
  });

  test('每个主页面都保留同一组导航链接', async ({ page }) => {
    for (const path of pages) {
      await visit(page, path);

      for (const item of NAV_ITEMS) {
        await expect(page.getByRole('link', { name: item.name })).toBeVisible();
      }
    }
  });
});
