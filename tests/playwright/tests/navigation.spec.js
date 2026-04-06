const { test, expect } = require('./fixtures');

test.describe('导航栏测试', () => {
  const pages = ['/', '/market-rank', '/binance-series', '/coins-config', '/coin-detail?symbol=BTCUSDT'];

  test('点击导航菜单可以跳转到对应页面', async ({ page }) => {
    await page.goto('/');
    await page.waitForSelector('.el-menu-item', { timeout: 5000 });

    await page.getByRole('menuitem', { name: '行情榜' }).click();
    await expect(page).toHaveURL(/\/market-rank$/);

    await page.getByRole('menuitem', { name: '历史序列' }).click();
    await expect(page).toHaveURL(/\/binance-series$/);

    await page.getByRole('menuitem', { name: '币种配置' }).click();
    await expect(page).toHaveURL(/\/coins-config$/);

    await page.getByRole('menuitem', { name: '首页' }).click();
    await expect(page).toHaveURL(/\/$/);
  });

  test('首页有导航菜单', async ({ page }) => {
    await page.goto('/');
    await page.waitForSelector('.el-menu-item', { timeout: 5000 });
    const html = await page.content();
    expect(html).toContain('el-menu');
    expect(html).toContain('首页');
  });

  test('行情榜有导航菜单', async ({ page }) => {
    await page.goto('/market-rank');
    await page.waitForSelector('.el-menu-item', { timeout: 5000 });
    const html = await page.content();
    expect(html).toContain('el-menu');
    expect(html).toContain('首页');
  });

test('导航栏宽度一致', async ({ page }) => {
    const pages = ['/', '/market-rank', '/binance-series', '/coins-config'];
    let firstWidth = null;
    
    for (const path of pages) {
      await page.goto(path);
      await page.waitForSelector('.nav-container', { timeout: 5000 });
      
      const navContainer = page.locator('.nav-container');
      const box = await navContainer.boundingBox();
      
      console.log(`页面 ${path}: 导航栏宽度 ${box.width}px`);
      
      if (firstWidth === null) {
        firstWidth = box.width;
      } else {
        expect(Math.abs(box.width - firstWidth)).toBeLessThan(10);
      }
    }
  });

  test('导航菜单项顺序一致', async ({ page }) => {
    const expectedOrder = ['首页', '行情榜', '历史序列', '币种配置'];
    
    for (const path of pages) {
      await page.goto(path);
      await page.waitForSelector('.el-menu-item', { timeout: 5000 });
      
      const menuItems = page.locator('.el-menu > .el-menu-item');
      const count = await menuItems.count();
      expect(count).toBe(4);
      
      for (let i = 0; i < expectedOrder.length; i++) {
        const itemText = await menuItems.nth(i).textContent();
        expect(itemText).toContain(expectedOrder[i]);
      }
    }
  });

  test('导航栏高度固定', async ({ page }) => {
    for (const path of pages) {
      await page.goto(path);
      await page.waitForSelector('.el-menu-item', { timeout: 5000 });
      
      const menu = page.locator('.el-menu');
      const box = await menu.boundingBox();
      expect(box.height).toBe(60);
    }
  });

  test('导航栏与容器间距', async ({ page }) => {
    for (const path of pages) {
      await page.goto(path);
      await page.waitForSelector('.nav-container', { timeout: 5000 });
      await page.waitForSelector('.container, .shell, .config-container', { timeout: 5000 });

      const navMarginBottom = await page.locator('.nav-container').evaluate((el) => parseFloat(getComputedStyle(el).marginBottom));
      const containerMarginTop = await page
        .locator('.container, .shell, .config-container')
        .first()
        .evaluate((el) => parseFloat(getComputedStyle(el).marginTop));

      expect(navMarginBottom).toBe(24);
      expect(containerMarginTop).toBe(0);
    }
  });

  test('导航栏和主容器圆角一致', async ({ page }) => {
    for (const path of pages) {
      await page.goto(path);
      await page.waitForSelector('.nav-container', { timeout: 5000 });
      await page.waitForSelector('.container, .shell, .config-container', { timeout: 5000 });

      const navRadius = await page.locator('.nav-container').evaluate((el) => parseFloat(getComputedStyle(el).borderTopLeftRadius));
      const containerRadius = await page
        .locator('.container, .shell, .config-container')
        .first()
        .evaluate((el) => parseFloat(getComputedStyle(el).borderTopLeftRadius));

      expect(navRadius).toBe(16);
      expect(containerRadius).toBe(16);
    }
  });

  test('导航栏底部间距使用统一值', async ({ page }) => {
    for (const path of pages) {
      await page.goto(path);
      await page.waitForSelector('.nav-container', { timeout: 5000 });

      const marginBottom = await page.locator('.nav-container').evaluate((el) => parseFloat(getComputedStyle(el).marginBottom));
      expect(marginBottom).toBe(24);
    }
  });

});
