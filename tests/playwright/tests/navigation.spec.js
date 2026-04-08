const { test, expect } = require('./fixtures');

async function getNavItems(page) {
  const menuItems = page.locator('.el-menu > .el-menu-item');
  const count = await menuItems.count();
  const items = [];

  for (let i = 0; i < count; i++) {
    const item = menuItems.nth(i);
    items.push({
      text: (await item.textContent()).trim(),
      href: await item.getAttribute('href'),
    });
  }

  return items;
}

test.describe('导航栏测试', () => {
  const pages = ['/', '/market-rank', '/binance-series', '/hedge-calculator', '/coins-config', '/coin-detail?symbol=BTCUSDT'];

  test('点击导航菜单可以跳转到对应页面', async ({ page }) => {
    await page.goto('/');
    await page.waitForSelector('.el-menu-item', { timeout: 5000 });

    const navItems = await getNavItems(page);
    expect(navItems.length).toBeGreaterThan(0);

    for (const item of navItems) {
      await page.getByRole('menuitem', { name: item.text }).click();
      await expect(page).toHaveURL(new RegExp(`${item.href.replace('/', '\\/')}($|\\?)`));
    }
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

test('导航栏宽度与主容器一致', async ({ page }) => {
    for (const path of pages) {
      await page.goto(path);
      await page.waitForSelector('.nav-container', { timeout: 5000 });
      await page.waitForSelector('.container, .shell, .config-container', { timeout: 5000 });
      
      const navContainer = page.locator('.nav-container');
      const mainContainer = page.locator('.container, .shell, .config-container').first();
      const navBox = await navContainer.boundingBox();
      const mainBox = await mainContainer.boundingBox();

      expect(navBox).not.toBeNull();
      expect(mainBox).not.toBeNull();
      expect(Math.abs(navBox.width - mainBox.width)).toBeLessThan(2);
    }
  });

  test('导航菜单项顺序一致', async ({ page }) => {
    await page.goto('/');
    await page.waitForSelector('.el-menu-item', { timeout: 5000 });
    const expectedNavItems = await getNavItems(page);
    
    for (const path of pages) {
      await page.goto(path);
      await page.waitForSelector('.el-menu-item', { timeout: 5000 });
      
      const currentNavItems = await getNavItems(page);
      expect(currentNavItems).toEqual(expectedNavItems);
    }
  });

  test('导航栏固定在页面顶部', async ({ page }) => {
    await page.goto('/');
    await page.waitForSelector('.nav-container', { timeout: 5000 });

    const navContainer = page.locator('.nav-container');
    const initialTop = await navContainer.evaluate((el) => el.getBoundingClientRect().top);
    const position = await navContainer.evaluate((el) => getComputedStyle(el).position);
    expect(position).toBe('sticky');

    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await page.waitForTimeout(100);

    const scrolledTop = await navContainer.evaluate((el) => el.getBoundingClientRect().top);
    expect(Math.abs(initialTop - 20)).toBeLessThan(4);
    expect(Math.abs(scrolledTop - 20)).toBeLessThan(4);
  });

  test('导航栏样式约束使用统一值', async ({ page }) => {
    for (const path of pages) {
      await page.goto(path);
      await page.waitForSelector('.nav-container', { timeout: 5000 });

      const styles = await page.locator('.nav-container').evaluate((el) => {
        const computed = getComputedStyle(el);
        return {
          marginBottom: parseFloat(computed.marginBottom),
          borderTopLeftRadius: parseFloat(computed.borderTopLeftRadius),
          top: parseFloat(computed.top),
        };
      });

      expect(styles.marginBottom).toBe(24);
      expect(styles.borderTopLeftRadius).toBe(16);
      expect(styles.top).toBe(20);
    }
  });

  test('导航栏与主容器间距保持稳定', async ({ page }) => {
    for (const path of pages) {
      await page.goto(path);
      await page.waitForSelector('.nav-container', { timeout: 5000 });
      await page.waitForSelector('.container, .shell, .config-container', { timeout: 5000 });

      const navBox = await page.locator('.nav-container').boundingBox();
      const containerBox = await page.locator('.container, .shell, .config-container').first().boundingBox();
      const marginBottom = await page.locator('.nav-container').evaluate((el) => parseFloat(getComputedStyle(el).marginBottom));
      expect(navBox).not.toBeNull();
      expect(containerBox).not.toBeNull();
      expect(Math.abs(containerBox.y - (navBox.y + navBox.height) - marginBottom)).toBeLessThan(2);
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

  test('导航栏菜单项都可识别为链接', async ({ page }) => {
    await page.goto('/');
    await page.waitForSelector('.el-menu-item', { timeout: 5000 });

    const navItems = await getNavItems(page);
    expect(navItems.length).toBeGreaterThan(0);

    for (const item of navItems) {
      expect(item.text.length).toBeGreaterThan(0);
      expect(item.href).toMatch(/^\//);
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
