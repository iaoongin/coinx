const { test, expect } = require('./fixtures');
const { button, heading, link, visit } = require('./contracts');

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

test.describe('首页测试', () => {
  test('页面加载', async ({ page }) => {
    await visit(page, '/');
    await expect(heading(page, '多周期矩阵', 1)).toBeVisible();
    await expect(link(page, '首页')).toBeVisible();
  });

  test('Vue已加载', async ({ page }) => {
    await visit(page, '/');
    const vueLoaded = await page.evaluate(() => typeof Vue !== 'undefined');
    expect(vueLoaded).toBe(true);
  });

  test('首页渲染了币种数据', async ({ page }) => {
    await visit(page, '/');
    await expect(page.locator('body')).toContainText('BTC');
    await expect(page.locator('body')).toContainText('BTC · 持仓价值 85.43M');
    await expect(page.locator('body')).toContainText('85.43M');
    await expect(page.locator('body')).toContainText('$62.00M(72%)');
    await expect(page.locator('body')).toContainText('$23.43M(28%)');
    await expect(page.locator('body')).toContainText('$8.40M(剔除)');
  });

  test('首页标题与状态操作在同一标题行', async ({ page }) => {
    await visit(page, '/');
    await expect(heading(page, '多周期矩阵', 1)).toBeVisible();
    await expect(page.getByText('更新时间')).toBeVisible();
    await expect(page.getByText('下次窗口')).toBeVisible();
    await expect(button(page, '刷新')).toBeVisible();
    await expect(page.locator('.coin-meta-line')).toBeVisible();
    await expect(page.locator('body')).not.toContainText('采集：');
  });

  test('首页悬浮操作在右下角融合排列', async ({ page }) => {
    await visit(page, '/');

    const configButton = page.getByRole('button', { name: '配置币种' });
    const backToTop = page.locator('[data-back-to-top]');
    await expect(page.locator('body')).toHaveClass(/homepage/);
    await expect(configButton).toBeVisible();
    await expect(backToTop).toBeHidden();

    await scrollPastBackToTopThreshold(page);
    await expect(backToTop).toBeVisible();

    const metrics = await page.evaluate(() => {
      const configRect = document.querySelector('.fab-button').getBoundingClientRect();
      const backRect = document.querySelector('[data-back-to-top]').getBoundingClientRect();
      return {
        configWidth: configRect.width,
        configHeight: configRect.height,
        backWidth: backRect.width,
        backHeight: backRect.height,
        configBackground: window.getComputedStyle(document.querySelector('.fab-button')).backgroundColor,
        backBackground: window.getComputedStyle(document.querySelector('[data-back-to-top]')).backgroundColor,
        configIconWidth: document.querySelector('.fab-button svg').getBoundingClientRect().width,
        configIconHeight: document.querySelector('.fab-button svg').getBoundingClientRect().height,
        backIconWidth: document.querySelector('[data-back-to-top] svg').getBoundingClientRect().width,
        backIconHeight: document.querySelector('[data-back-to-top] svg').getBoundingClientRect().height,
        configRight: configRect.right,
        backRight: backRect.right,
        gap: configRect.top - backRect.bottom,
        configPosition: window.getComputedStyle(document.querySelector('.fab-button')).position,
        backPosition: window.getComputedStyle(document.querySelector('[data-back-to-top]')).position,
      };
    });

    expect(metrics.configWidth).toBe(56);
    expect(metrics.configHeight).toBe(56);
    expect(metrics.backWidth).toBe(56);
    expect(metrics.backHeight).toBe(56);
    expect(metrics.backBackground).toBe(metrics.configBackground);
    expect(metrics.configBackground).toBe('rgb(27, 32, 39)');
    expect(metrics.configIconWidth).toBe(24);
    expect(metrics.configIconHeight).toBe(24);
    expect(metrics.backIconWidth).toBe(24);
    expect(metrics.backIconHeight).toBe(24);
    expect(metrics.configRight).toBeCloseTo(metrics.backRight, 5);
    expect(metrics.gap).toBeCloseTo(12, 5);
    expect(metrics.configPosition).toBe('fixed');
    expect(metrics.backPosition).toBe('fixed');
  });

  test('首页配置按钮仍能打开币种配置弹窗', async ({ page }) => {
    await visit(page, '/');

    await page.getByRole('button', { name: '配置币种' }).click();
    await expect(page.getByRole('dialog', { name: '币种配置' })).toBeVisible();
  });

  test('首页移动端悬浮操作保持右边距和间距', async ({ page }) => {
    await page.setViewportSize({ width: 390, height: 844 });
    await visit(page, '/');
    await scrollPastBackToTopThreshold(page);

    const metrics = await page.evaluate(() => {
      const config = document.querySelector('.fab-button');
      const back = document.querySelector('[data-back-to-top]');
      const configRect = config.getBoundingClientRect();
      const backRect = back.getBoundingClientRect();
      return {
        configWidth: configRect.width,
        configHeight: configRect.height,
        backWidth: backRect.width,
        backHeight: backRect.height,
        configRight: window.getComputedStyle(config).right,
        backRight: window.getComputedStyle(back).right,
        configBottom: window.getComputedStyle(config).bottom,
        backBottom: window.getComputedStyle(back).bottom,
        configIconWidth: config.querySelector('svg').getBoundingClientRect().width,
        configIconHeight: config.querySelector('svg').getBoundingClientRect().height,
        backIconWidth: back.querySelector('svg').getBoundingClientRect().width,
        backIconHeight: back.querySelector('svg').getBoundingClientRect().height,
        gap: configRect.top - backRect.bottom,
      };
    });

    expect(metrics.configWidth).toBe(48);
    expect(metrics.configHeight).toBe(48);
    expect(metrics.backWidth).toBe(48);
    expect(metrics.backHeight).toBe(48);
    expect(metrics.configIconWidth).toBe(20);
    expect(metrics.configIconHeight).toBe(20);
    expect(metrics.backIconWidth).toBe(20);
    expect(metrics.backIconHeight).toBe(20);
    expect(metrics.configRight).toBe('20px');
    expect(metrics.backRight).toBe('20px');
    expect(metrics.configBottom).toBe('20px');
    expect(metrics.backBottom).toBe('80px');
    expect(metrics.gap).toBeCloseTo(12, 5);
  });

  test('搜索条件通过URL参数恢复并支持清除', async ({ page }) => {
    await visit(page, '/?symbol=BTC');

    const input = page.locator('.search-input');
    await expect(input).toHaveValue('BTC');
    await expect(page.locator('.coin-panel')).toHaveCount(1);

    await page.reload();
    await expect(input).toHaveValue('BTC');

    await page.getByRole('button', { name: '清除搜索' }).click();
    await expect(input).toHaveValue('');
    await expect(page).toHaveURL(/\/$/);
    await expect(page.locator('.coin-panel')).toHaveCount(1);
  });

  test('搜索记录可在输入框聚焦时展示并重新使用', async ({ page }) => {
    await page.evaluate(() => localStorage.removeItem('coinx.homepage.search-history'));
    await visit(page, '/');

    const input = page.locator('.search-input');
    await input.fill('BTC');
    await input.press('Enter');
    await expect(page).toHaveURL(/symbol=BTC/);

    await input.fill('');
    await input.focus();
    await expect(page.getByRole('listbox', { name: '最近搜索' })).toBeVisible();
    await expect(page.getByRole('option', { name: 'BTC' })).toBeVisible();

    await page.getByRole('option', { name: 'BTC' }).click();
    await expect(input).toHaveValue('BTC');
    await expect(page).toHaveURL(/symbol=BTC/);
  });

  test('点击资费标签展示 24 小时走势图并支持 Esc 关闭', async ({ page }) => {
    const historyRequest = page.waitForRequest((request) => {
      const url = new URL(request.url());
      return url.pathname === '/api/funding-rate/history/BTCUSDT' && url.searchParams.get('hours') === '24';
    });

    await visit(page, '/');
    await page.locator('.coin-meta-funding').click();
    await historyRequest;

    const dialog = page.getByRole('dialog', { name: /BTCUSDT.*24/ });
    await expect(dialog).toBeVisible();
    await expect(dialog.locator('svg')).toBeVisible();

    await page.keyboard.press('Escape');
    await expect(dialog).toBeHidden();
  });

  test('资费历史为空时展示空状态', async ({ page }) => {
    await page.route('**/api/funding-rate/history/BTCUSDT?hours=24', (route) => route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({ status: 'success', data: [] }),
    }));
    await visit(page, '/');
    await page.locator('.coin-meta-funding').click();
    await expect(page.locator('.funding-chart-state')).toHaveText('暂无历史数据');
  });

  test('资费历史请求失败时展示失败状态', async ({ page }) => {
    await page.route('**/api/funding-rate/history/BTCUSDT?hours=24', (route) => route.fulfill({
      status: 500,
      contentType: 'application/json',
      body: JSON.stringify({ status: 'error' }),
    }));
    await visit(page, '/');
    await page.locator('.coin-meta-funding').click();
    await expect(page.locator('.funding-chart-state')).toHaveText('加载失败，请稍后重试');
  });
});
