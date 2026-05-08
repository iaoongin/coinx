const NAV_ITEMS = [
  { name: '首页', href: '/' },
  { name: '行情榜', href: '/market-rank' },
  { name: '结构评分', href: '/market-structure-score' },
  { name: '历史序列', href: '/binance-series' },
  { name: '对冲计算器', href: '/hedge-calculator' },
  { name: '币种配置', href: '/coins-config' },
];

function escapeRegExp(text) {
  return text.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

async function visit(page, path) {
  await page.goto(path);
  await page.waitForLoadState('networkidle');
}

function heading(page, name, level) {
  const pattern = new RegExp(escapeRegExp(name));
  return level ? page.getByRole('heading', { name: pattern, level }) : page.getByRole('heading', { name: pattern });
}

function button(page, name) {
  return page.getByRole('button', { name: new RegExp(escapeRegExp(name)) });
}

function link(page, name) {
  if (name === '首页') {
    return page.locator('a[href="/"]');
  }
  return page.getByRole('link', { name: new RegExp(`^${escapeRegExp(name)}$`) });
}

function testId(page, id) {
  return page.getByTestId(id);
}

function navLinks(page) {
  return NAV_ITEMS.map((item) => ({
    ...item,
    locator: page.locator(`a[href="${item.href}"]`),
  }));
}

module.exports = {
  NAV_ITEMS,
  visit,
  heading,
  button,
  link,
  testId,
  navLinks,
};
