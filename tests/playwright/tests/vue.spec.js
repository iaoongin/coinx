const { test, expect } = require('./fixtures');
const { heading, link, visit } = require('./contracts');

test.describe('Vue 加载测试', () => {
  test('Vue CDN 加载', async ({ page }) => {
    await visit(page, '/');
    const vueExists = await page.evaluate(() => typeof Vue !== 'undefined');
    expect(vueExists).toBe(true);
  });

  test('Vue 实例挂载', async ({ page }) => {
    await visit(page, '/');
    const hasContent = await page.evaluate(() => {
      const app = document.getElementById('app');
      return app && app.children.length > 0;
    });
    expect(hasContent).toBe(true);
  });

  test('首页契约渲染', async ({ page }) => {
    await visit(page, '/');
    await expect(heading(page, '币种数据监控', 1)).toBeVisible();
    await expect(link(page, '首页')).toBeVisible();
  });

  test('响应式数据渲染', async ({ page }) => {
    await visit(page, '/');
    const hasContent = await page.evaluate(() => {
      const app = document.getElementById('app');
      return app && app.innerText.length > 0;
    });
    expect(hasContent).toBe(true);
  });
});
