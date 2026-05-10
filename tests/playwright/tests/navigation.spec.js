const { test, expect } = require('./fixtures');
const { NAV_ITEMS, navLinks, visit } = require('./contracts');

const pages = ['/', '/market-rank', '/market-structure-score', '/binance-series', '/hedge-calculator', '/coins-config', '/coin-detail?symbol=BTCUSDT', '/task-jobs'];

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

    for (const item of navLinks(page)) {
      await item.locator.click();
      await expect(page).toHaveURL(new RegExp(`${item.href.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}($|\\?)`));
    }
  });

  test('每个主页面都保留同一组导航链接', async ({ page }) => {
    for (const path of pages) {
      await visit(page, path);

      for (const item of navLinks(page)) {
        await expect(item.locator).toBeVisible();
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
