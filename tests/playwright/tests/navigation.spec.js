const { test, expect } = require('./fixtures');
const { NAV_ITEMS, navLinks, visit } = require('./contracts');

const pages = ['/', '/legacy-home', '/market-rank', '/market-structure-score', '/hedge-calculator', '/coins-config', '/coin-detail?symbol=BTCUSDT', '/task-jobs', '/funding-rate'];

const NAV_MENU_BY_PATH = {
  '/legacy-home': '市场',
  '/market-rank': '市场',
  '/funding-rate': '市场',
  '/market-structure-score': '分析',
  '/coin-detail': '分析',
  '/hedge-calculator': '分析',
  '/coins-config': '管理',
  '/task-jobs': '管理',
};

async function revealNavItem(page, item) {
  const menuName = NAV_MENU_BY_PATH[item.href];
  if (!menuName) return;

  const trigger = page.locator(`.nav-menu:has(.nav-menu-list[aria-label="${menuName}"]) .nav-menu-trigger`);
  await expect(trigger).toHaveCount(1);
  await trigger.click();
}

async function navToContentGap(page) {
  const metrics = await page.evaluate(() => {
    const nav = document.querySelector('.nav-container');
    if (!nav) {
      return null;
    }

    const pageFrame = nav.closest('.page-frame') || document.body;
    const selectors = [
      '#app > .shell',
      '#app.shell',
      '#app > section.shell',
      '#app > .config-container',
      '#app > .container',
      '#app > .card',
      '#app > .table-wrapper',
      '#app',
    ];

    let content = null;
    for (const selector of selectors) {
      const candidate = pageFrame.querySelector(selector);
      if (!candidate || candidate === nav) continue;
      const style = window.getComputedStyle(candidate);
      const rect = candidate.getBoundingClientRect();
      if (style.display === 'none' || style.visibility === 'hidden') continue;
      if (rect.width === 0 || rect.height === 0) continue;
      content = candidate;
      break;
    }

    if (!content) {
      const siblings = Array.from(nav.parentElement?.children || []);
      content = siblings.find((element) => {
        if (element === nav) return false;
        const style = window.getComputedStyle(element);
        const rect = element.getBoundingClientRect();
        if (style.display === 'none' || style.visibility === 'hidden') return false;
        return rect.width > 0 && rect.height > 0;
      }) || null;
    }

    if (!content) {
      return null;
    }

    const navRect = nav.getBoundingClientRect();
    const contentRect = content.getBoundingClientRect();
    return {
      navTop: Math.round(navRect.top * 100) / 100,
      gap: Math.round((contentRect.top - navRect.bottom) * 100) / 100,
      navBottom: Math.round(navRect.bottom * 100) / 100,
      contentTop: Math.round(contentRect.top * 100) / 100,
      tagName: content.tagName,
      className: content.className || '',
      id: content.id || '',
    };
  });

  expect(metrics).not.toBeNull();
  return metrics;
}

async function scrollPastBackToTopThreshold(page) {
  await page.evaluate(() => {
    const spacer = document.createElement('div');
    spacer.dataset.playwrightScrollSpacer = 'true';
    spacer.style.height = '1000px';
    document.body.appendChild(spacer);
  });
  await page.mouse.wheel(0, 500);
  await expect.poll(() => page.evaluate(() => window.scrollY)).toBeGreaterThan(320);
}

test.describe('导航栏契约', () => {
  test('回到顶端按钮按滚动位置显示并回滚', async ({ page }) => {
    await visit(page, '/legacy-home');

    const backToTop = page.locator('[data-back-to-top]');
    await expect(backToTop).toBeHidden();

    await scrollPastBackToTopThreshold(page);
    await expect(backToTop).toBeVisible();
    await expect(backToTop).toHaveAttribute('aria-label', '回到顶端');
    await expect(backToTop).toHaveAttribute('aria-hidden', 'false');

    await backToTop.focus();
    await expect(backToTop).toBeFocused();
    await backToTop.press('Enter');
    await expect.poll(() => page.evaluate(() => window.scrollY)).toBe(0);
    await expect(backToTop).toBeHidden();
  });

  test('所有主导航业务页面都渲染回到顶端按钮', async ({ page }) => {
    for (const path of pages) {
      await visit(page, path);
      await expect(page.locator('[data-back-to-top]'), `${path} should render back-to-top button`).toHaveCount(1);
    }
  });

  test('回到顶端按钮在移动端保持固定尺寸和边距', async ({ page }) => {
    await page.setViewportSize({ width: 390, height: 844 });
    await visit(page, '/legacy-home');
    await scrollPastBackToTopThreshold(page);
    const backToTop = page.locator('[data-back-to-top]');
    await expect(backToTop).toBeVisible();

    const metrics = await backToTop.evaluate((button) => {
      const rect = button.getBoundingClientRect();
      const style = window.getComputedStyle(button);
      return {
        width: rect.width,
        height: rect.height,
        position: style.position,
        right: style.right,
        bottom: style.bottom,
      };
    });

    expect(metrics.width).toBe(44);
    expect(metrics.height).toBe(44);
    expect(metrics.position).toBe('fixed');
    expect(metrics.right).toBe('20px');
    expect(metrics.bottom).toBe('20px');
  });

  test('导航入口按市场、分析和管理下拉菜单归类', async ({ page }) => {
    await visit(page, '/');

    await expect(page.locator('.nav-menu')).toHaveCount(3);
    await expect(page.locator('.nav-menu-list[aria-label="市场"]')).toContainText('旧首页');
    await expect(page.locator('.nav-menu-list[aria-label="分析"]')).toContainText('结构评分');
    await expect(page.locator('.nav-menu-list[aria-label="管理"]')).toContainText('币种配置');
  });

  test('导航链接文本和 href 稳定', async ({ page }) => {
    await visit(page, '/');

    const items = navLinks(page);
    expect(items).toHaveLength(NAV_ITEMS.length);

    for (const item of items) {
      await expect(item.locator).toHaveCount(1);
      await expect(item.locator).toHaveAttribute('href', item.href);
      await expect(item.locator).toContainText(item.name);
    }
  });

  test('点击导航可以跳转到对应页面', async ({ page }) => {
    await visit(page, '/');

    for (const item of navLinks(page)) {
      await revealNavItem(page, item);
      await expect(item.locator).toBeVisible();
      await item.locator.click();
      await expect(page).toHaveURL(new RegExp(`${item.href.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}($|\\?)`));
    }
  });

  test('每个主页面都保留同一组导航链接', async ({ page }) => {
    for (const path of pages) {
      await visit(page, path);

      for (const item of navLinks(page)) {
        await expect(item.locator).toHaveCount(1);
      }
    }
  });

  test('导航和页面顶部内容间距在主要页面保持一致', async ({ page }) => {
    let baselineNavTop = null;
    let baselineGap = null;

    for (const path of pages.filter((item) => item !== '/coin-detail?symbol=BTCUSDT')) {
      await visit(page, path);

      const metrics = await navToContentGap(page);
      expect(metrics.navTop, `${path} nav top should be non-negative`).toBeGreaterThanOrEqual(0);
      expect(metrics.gap, `${path} gap should be non-negative`).toBeGreaterThanOrEqual(0);

      if (baselineNavTop === null) {
        baselineNavTop = metrics.navTop;
        baselineGap = metrics.gap;
        continue;
      }

      expect(
        Math.abs(metrics.navTop - baselineNavTop),
        `${path} navTop ${metrics.navTop}px should match baseline ${baselineNavTop}px`
      ).toBeLessThanOrEqual(1);

      expect(
        Math.abs(metrics.gap - baselineGap),
        `${path} gap ${metrics.gap}px should match baseline ${baselineGap}px; content=${metrics.tagName}#${metrics.id}.${metrics.className}`
      ).toBeLessThanOrEqual(1);
    }
  });
});
