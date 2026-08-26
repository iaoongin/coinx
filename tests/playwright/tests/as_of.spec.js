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
    await expect(page.locator('[data-as-of-input]')).toHaveValue('2024-03-27 16:00');
    await expect(page.locator('[data-as-of-status]')).toContainText('回放至');
  });

  test('点击数据时点打开日期时间弹窗', async ({ page }) => {
    await visit(page, '/');

    await page.locator('[data-as-of-input]').click();

    await expect(page.locator('.flatpickr-calendar.open')).toBeVisible();
    await expect(page.locator('.flatpickr-time')).toBeVisible();
  });

  test('选择时间后应用新的回放时点', async ({ page }) => {
    await visit(page, '/?as_of_ms=1711526400000');
    await page.locator('[data-as-of-input]').click();
    await page.locator('.flatpickr-day[aria-label="三月 28, 2024"]').click();
    await page.locator('.flatpickr-hour').fill('12');
    await page.locator('.flatpickr-minute').fill('05');

    await expect(page.locator('[data-as-of-status]')).toContainText('待应用 2024-03-28 12:05');
    await page.getByRole('button', { name: '应用' }).click();
    await expect(page).toHaveURL(/as_of_ms=1711598700000/);
  });
});
