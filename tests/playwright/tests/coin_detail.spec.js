const { test, expect } = require('./fixtures');
const { button, heading, visit } = require('./contracts');

test.describe('币种详情测试', () => {
  test('详情页加载', async ({ page }) => {
    await visit(page, '/coin-detail?symbol=BTCUSDT');
    await expect(heading(page, '合约详情', 1)).toBeVisible();
  });

  test('详情页渲染了关键指标', async ({ page }) => {
    await visit(page, '/coin-detail?symbol=BTCUSDT');
    await expect(page.locator('body')).toContainText('最新价格');
    await expect(page.locator('body')).toContainText('$69,234.12');
    await expect(page.locator('body')).toContainText('+0.08%');
    await expect(page.locator('body')).toContainText('72.4');
    await expect(page.locator('body')).toContainText('binance');
    await expect(page.locator('body')).toContainText('bybit');
    await expect(button(page, '返回')).toBeVisible();
  });

  test('多周期变化沿用首页矩阵列序', async ({ page }) => {
    await visit(page, '/coin-detail?symbol=BTCUSDT');
    const matrix = page.locator('.matrix');
    await expect(matrix).toBeVisible();
    await expect(matrix).toContainText('窗口');
    await expect(matrix).toContainText('净流入');
    await expect(matrix).toContainText('价格');
    await expect(matrix).toContainText('价格%');
    await expect(matrix).toContainText('量');
    await expect(matrix).toContainText('量%');
    await expect(matrix).toContainText('价值');
    await expect(matrix).toContainText('价值%');
    const configuredIntervals = await page.evaluate(() => window.COINX_TIME_INTERVALS);
    await expect(matrix.locator('.window-cell')).toHaveCount(1 + configuredIntervals.length);
    await expect(matrix.locator('.taker-tag')).toHaveCount(2);
  });

  test('首页与详情页使用同一矩阵展示契约', async ({ page }) => {
    const readMatrixContract = async () => {
      const matrix = page.locator('.matrix');
      expect(await matrix.count()).toBe(1);
      return matrix.evaluate(element => ({
      minWidth: getComputedStyle(element).minWidth,
      columnDefinitions: [
        getComputedStyle(element).getPropertyValue('--matrix-col-window').trim(),
        getComputedStyle(element).getPropertyValue('--matrix-col').trim(),
      ],
      headings: [...element.querySelectorAll(':scope > .matrix-head')].map(cell => (
        cell.querySelector('.matrix-head-title')?.textContent.trim() || cell.textContent.trim()
      )),
      windows: [...element.querySelectorAll(':scope > .window-cell')].map(cell => cell.textContent.trim()),
      metricStyles: (() => {
        const metric = element.querySelector('.metric');
        const raw = element.querySelector('.raw-value');
        return [metric, raw].map(cell => {
          const style = getComputedStyle(cell);
          return {
            minHeight: style.minHeight,
            padding: style.padding,
            fontFamily: style.fontFamily,
            fontSize: style.fontSize,
          };
        });
      })(),
      }));
    };

    await visit(page, '/');
    const homepageContract = await readMatrixContract();
    await visit(page, '/coin-detail?symbol=BTCUSDT');
    const detailContract = await readMatrixContract();

    expect(detailContract.minWidth).toBe(homepageContract.minWidth);
    expect(detailContract.columnDefinitions).toEqual(homepageContract.columnDefinitions);
    expect(detailContract.headings).toEqual(homepageContract.headings);
    expect(detailContract.windows).toEqual(homepageContract.windows);
    expect(detailContract.metricStyles).toEqual(homepageContract.metricStyles);
    const configuredIntervals = await page.evaluate(() => window.COINX_TIME_INTERVALS);
    expect(homepageContract.windows).toEqual(['窗口', ...configuredIntervals]);
  });

  test('可通过搜索下拉框切换合约并记录最近浏览', async ({ page }) => {
    await visit(page, '/coin-detail?symbol=BTCUSDT');
    await page.getByRole('button', { name: '搜索并切换合约' }).click();
    const picker = page.getByRole('searchbox', { name: '搜索合约' });
    await picker.fill('SOL');
    await page.getByRole('button', { name: 'SOLUSDT', exact: true }).click();
    await expect(page).toHaveURL(/\/coin-detail\?symbol=SOLUSDT$/);
    await expect(page.locator('h1')).toContainText('SOLUSDT');

    await page.getByRole('button', { name: '搜索并切换合约' }).click();
    await expect(page.getByText('最近浏览', { exact: true })).toBeVisible();
    await expect(page.locator('.symbol-picker-menu')).toContainText('SOLUSDT');
  });

  test('移动端布局不产生横向溢出', async ({ page }) => {
    await page.setViewportSize({ width: 390, height: 844 });
    await visit(page, '/coin-detail?symbol=BTCUSDT');
    const dimensions = await page.locator('html').evaluate(element => ({
      clientWidth: element.clientWidth,
      scrollWidth: element.scrollWidth,
    }));
    expect(dimensions.scrollWidth).toBeLessThanOrEqual(dimensions.clientWidth);
    const matrixDimensions = await page.locator('.matrix').evaluate(element => ({
      clientWidth: element.parentElement.clientWidth,
      scrollWidth: element.scrollWidth,
    }));
    expect(matrixDimensions.scrollWidth).toBeGreaterThan(matrixDimensions.clientWidth);
    await expect(page.locator('.detail-header')).toHaveCSS('flex-direction', 'column');
    await expect(page.locator('.actions .btn')).toHaveCount(1);
    const chartHeights = await page.locator('.chart').evaluateAll(elements => elements.map(element => getComputedStyle(element).height));
    expect(chartHeights).toEqual(['260px', '260px', '260px', '260px']);
    await page.getByRole('button', { name: '搜索并切换合约' }).click();
    const menuBounds = await page.locator('.symbol-picker-menu').evaluate(element => {
      const rect = element.getBoundingClientRect();
      return { left: rect.left, right: rect.right, viewportWidth: window.innerWidth };
    });
    expect(menuBounds.left).toBeGreaterThanOrEqual(0);
    expect(menuBounds.right).toBeLessThanOrEqual(menuBounds.viewportWidth);
  });

  test('渲染交易机会与历史趋势并支持切换范围', async ({ page }) => {
    const opportunityRequest = page.waitForRequest(request => request.url().includes('/trade-opportunity'));
    await visit(page, '/coin-detail?symbol=BTCUSDT');
    await opportunityRequest;
    await expect(page.getByRole('heading', { name: '交易机会', exact: true })).toBeVisible();
    await expect(page.locator('.opportunity-plan').getByText('入场', { exact: true })).toBeVisible();
    await expect(page.getByText('目标1', { exact: true })).toBeVisible();
    await expect(page.getByText('目标2', { exact: true })).toBeVisible();
    await expect(page.getByText('目标3', { exact: true })).toBeVisible();
    await expect(page.locator('.chart canvas')).toHaveCount(4);
    const chartWidths = await page.locator('.chart canvas').evaluateAll(canvases => canvases.map(canvas => canvas.getBoundingClientRect().width));
    expect(chartWidths.every(width => width > 300)).toBeTruthy();

    const seriesRequest = page.waitForRequest(request => request.url().includes('/series?range=4h'));
    await page.getByRole('button', { name: '4h', exact: true }).click();
    await seriesRequest;
    await expect(page.locator('.segment.active')).toHaveText('4h');
  });
});
