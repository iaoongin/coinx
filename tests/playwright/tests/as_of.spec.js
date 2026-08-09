const { test, expect } = require('./fixtures');
const { visit } = require('./contracts');

test.describe('数据时点回放', () => {
  test('透传上海时间对应的 as_of_ms', async ({ page }) => {
    const requestPromise = page.waitForRequest((request) => {
      const url = new URL(request.url());
      return url.pathname === '/api/coins'
        && url.searchParams.get('as_of_ms') === '1711526400000';
    });

    await visit(page, '/?as_of_ms=1711526400000');
    await requestPromise;
    await expect(page.locator('[data-as-of-input]')).toHaveValue('2024-03-27T08:00');
    await expect(page.locator('[data-as-of-status]')).toContainText('回放至');
  });
});
