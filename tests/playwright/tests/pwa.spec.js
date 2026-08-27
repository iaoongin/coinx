const { test, expect } = require('./fixtures');
const { visit } = require('./contracts');

test.describe('PWA安装能力', () => {
  test('页面声明Manifest并注册Service Worker', async ({ page }) => {
    await visit(page, '/');

    await expect(page.locator('link[rel="manifest"]')).toHaveAttribute(
      'href',
      '/static/manifest.webmanifest'
    );

    await expect.poll(async () => page.evaluate(async () => {
      const registration = await navigator.serviceWorker.getRegistration('/');
      return registration?.active?.scriptURL || '';
    })).toContain('/service-worker.js');
  });

  test('记录首页及运行时更新的URL参数', async ({ page }) => {
    await page.goto('/');
    await page.evaluate(() => {
      window.history.replaceState({}, '', '/?symbol=BTCUSDT');
    });

    await expect.poll(() => page.evaluate(() => (
      localStorage.getItem('coinx.pwa.last-route')
    ))).toBe('/?symbol=BTCUSDT');
  });

  test('PWA启动时恢复上次页面', async ({ page }) => {
    await page.evaluate(() => localStorage.setItem('coinx.pwa.last-route', '/market-rank'));

    await page.goto('/pwa-start');

    await expect(page).toHaveURL(/\/market-rank$/);
  });

  test('上次页面返回404时清除记录并留在首页', async ({ page }) => {
    await page.evaluate(() => localStorage.setItem('coinx.pwa.last-route', '/page-that-does-not-exist'));

    await page.goto('/pwa-start');

    await expect(page).toHaveURL(/\/$/);
    await expect.poll(() => page.evaluate(() => localStorage.getItem('coinx.pwa.last-route'))).toBeNull();
  });
});
