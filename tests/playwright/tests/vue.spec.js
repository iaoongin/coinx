const { test, expect } = require('./fixtures');

test.describe('Vue 加载测试', () => {
  test('Vue CDN 加载', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
    const vueExists = await page.evaluate(() => typeof Vue !== 'undefined');
    expect(vueExists).toBe(true);
  });

  test('Vue 实例挂载', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(3000);
    const hasContent = await page.evaluate(() => {
      const app = document.getElementById('app');
      return app && app.children.length > 0;
    });
    expect(hasContent).toBe(true);
  });

  test('Element Plus 加载', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(3000);
    const elExists = await page.evaluate(() => {
      return typeof ElementPlus !== 'undefined' || 
             document.querySelector('[class*="el-"]') !== null;
    });
    expect(elExists).toBe(true);
  });

  test('响应式数据渲染', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(5000);
    const hasContent = await page.evaluate(() => {
      const app = document.getElementById('app');
      return app && app.innerText.length > 0;
    });
    expect(hasContent).toBe(true);
  });
});
