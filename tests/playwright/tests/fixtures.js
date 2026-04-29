const { test: base, expect } = require('@playwright/test');

const mockHomepageCoins = [
  {
    symbol: 'BTCUSDT',
    current_open_interest: 1234567.89,
    current_open_interest_formatted: '1.23M',
    current_open_interest_value: 85432123.45,
    current_open_interest_value_formatted: '85.43M',
    current_price: 69234.12,
    current_price_formatted: '69,234.12',
    price_change: 2.34,
    price_change_percent: 2.34,
    price_change_formatted: '+2.34%',
    net_inflow: {
      '5m': 120000,
      '15m': 220000,
      '30m': -180000,
      '1h': 310000,
      '4h': 650000,
      '6h': 720000,
      '12h': 980000,
    },
    changes: [
      {
        interval: '15m',
        ratio: 1.2,
        value_ratio: 1.6,
        open_interest: 1250000,
        open_interest_formatted: '1.25M',
        open_interest_value: 86000000,
        open_interest_value_formatted: '86.00M',
        price_change: 0.5,
        price_change_percent: 0.5,
        price_change_formatted: '+0.50%',
        current_price: 68900.5,
        current_price_formatted: '68,900.50',
      },
      {
        interval: '30m',
        ratio: -0.8,
        value_ratio: -0.3,
        open_interest: 1220000,
        open_interest_formatted: '1.22M',
        open_interest_value: 85000000,
        open_interest_value_formatted: '85.00M',
        price_change: -0.2,
        price_change_percent: -0.2,
        price_change_formatted: '-0.20%',
        current_price: 68880.2,
        current_price_formatted: '68,880.20',
      },
      {
        interval: '1h',
        ratio: 2.1,
        value_ratio: 2.9,
        open_interest: 1280000,
        open_interest_formatted: '1.28M',
        open_interest_value: 87900000,
        open_interest_value_formatted: '87.90M',
        price_change: 1.4,
        price_change_percent: 1.4,
        price_change_formatted: '+1.40%',
        current_price: 69100.0,
        current_price_formatted: '69,100.00',
      },
      {
        interval: '4h',
        ratio: 4.5,
        value_ratio: 5.1,
        open_interest: 1310000,
        open_interest_formatted: '1.31M',
        open_interest_value: 90100000,
        open_interest_value_formatted: '90.10M',
        price_change: 3.2,
        price_change_percent: 3.2,
        price_change_formatted: '+3.20%',
        current_price: 69450.0,
        current_price_formatted: '69,450.00',
      },
      {
        interval: '6h',
        ratio: 5.2,
        value_ratio: 6.0,
        open_interest: 1325000,
        open_interest_formatted: '1.33M',
        open_interest_value: 91000000,
        open_interest_value_formatted: '91.00M',
        price_change: 3.8,
        price_change_percent: 3.8,
        price_change_formatted: '+3.80%',
        current_price: 69510.0,
        current_price_formatted: '69,510.00',
      },
      {
        interval: '12h',
        ratio: 7.8,
        value_ratio: 8.6,
        open_interest: 1360000,
        open_interest_formatted: '1.36M',
        open_interest_value: 94000000,
        open_interest_value_formatted: '94.00M',
        price_change: 4.4,
        price_change_percent: 4.4,
        price_change_formatted: '+4.40%',
        current_price: 69750.0,
        current_price_formatted: '69,750.00',
      },
    ],
  },
];

const mockMarketRankBase = [
  {
    symbol: 'BTCUSDT',
    rank_index: 1,
    price: 69234.12,
    price_change_percent: -3.4,
    volume: 123456.78,
    quote_volume: 85432123.45,
  },
  {
    symbol: 'ETHUSDT',
    rank_index: 2,
    price: 3521.45,
    price_change_percent: -1.2,
    volume: 98765.43,
    quote_volume: 43000000.12,
  },
];

const mockMarketRankRefreshed = [
  {
    symbol: 'BTCUSDT',
    rank_index: 1,
    price: 69888.88,
    price_change_percent: 3.4,
    volume: 223456.78,
    quote_volume: 95432123.45,
  },
  {
    symbol: 'ETHUSDT',
    rank_index: 2,
    price: 3621.45,
    price_change_percent: 1.2,
    volume: 198765.43,
    quote_volume: 53000000.12,
  },
];

const mockBinanceSeriesConfig = {
  collect: {
    limit: 30,
    series_types: ['klines', 'open_interest_hist', 'top_long_short_position_ratio'],
    periods: ['5m', '15m', '1h'],
  },
  repair: {
    enabled: true,
    interval: 300,
    period: '5m',
    bootstrap_days: 7,
    klines_page_limit: 1000,
    futures_page_limit: 500,
    sleep_ms: 0,
  },
};

const mockCoinDetail = {
  symbol: 'BTCUSDT',
  latest_price: 69234.12,
  funding_rate: {
    lastFundingRate: 0.0008,
  },
  ticker_data: {
    priceChangePercent: 2.34,
  },
  open_interest_data: {
    openInterestValue: 85432123.45,
  },
  exchange_distribution: {
    binance: { value: 40000000, percentage: 46.8 },
    bybit: { value: 25000000, percentage: 29.3 },
  },
  net_inflow_data: {
    '5m': 120000,
    '1h': 340000,
    '4h': -120000,
  },
};

function jsonResponse(data, status = 200) {
  return {
    status,
    contentType: 'application/json',
    body: JSON.stringify(data),
  };
}

const test = base.extend({
  context: async ({ context }, use) => {
    const coinsConfig = {
      BTCUSDT: true,
      ETHUSDT: true,
      SOLUSDT: false,
    };
    let marketRankSnapshot = mockMarketRankBase;
    let marketRankSnapshotTime = '2026-04-07T01:00:00';

    await context.route('**/api/**', async (route) => {
      const request = route.request();
      const url = new URL(request.url());
      const { pathname, searchParams } = url;

      if (pathname === '/api/update') {
        await route.fulfill(jsonResponse({ status: 'success', message: 'homepage series refresh triggered' }));
        return;
      }

      if (pathname === '/api/coins') {
        await route.fulfill(
          jsonResponse({
            status: 'success',
            message: 'homepage data loaded',
            data: mockHomepageCoins,
            cache_update_time: '2026-04-07T01:00:00',
          })
        );
        return;
      }

      if (pathname === '/api/market-rank') {
        const direction = searchParams.get('direction');
        const data =
          direction === 'up'
            ? marketRankSnapshot.map((item) => ({ ...item, price_change_percent: Math.abs(item.price_change_percent) }))
            : marketRankSnapshot;
        await route.fulfill(
          jsonResponse({
            status: 'success',
            message: 'market rank loaded',
            data,
            snapshot_time: marketRankSnapshotTime,
          })
        );
        return;
      }

      if (pathname === '/api/market-rank/refresh' && request.method() === 'POST') {
        marketRankSnapshot = mockMarketRankRefreshed;
        marketRankSnapshotTime = '2026-04-07T01:05:00';
        await route.fulfill(
          jsonResponse({
            status: 'success',
            message: 'market rank snapshot refreshed',
            data: {
              status: 'success',
              message: 'market rank snapshot refreshed',
              saved_count: marketRankSnapshot.length,
              snapshot_time: marketRankSnapshotTime,
            },
          })
        );
        return;
      }

      if (pathname === '/api/coins-config' && request.method() === 'GET') {
        await route.fulfill(
          jsonResponse({
            status: 'success',
            message: '获取币种配置成功',
            data: coinsConfig,
          })
        );
        return;
      }

      if (pathname === '/api/coins-config/track' && request.method() === 'POST') {
        const payload = request.postDataJSON();
        coinsConfig[payload.symbol] = Boolean(payload.tracked);
        await route.fulfill(jsonResponse({ status: 'success', message: '跟踪状态设置成功' }));
        return;
      }

      if (pathname === '/api/coins-config/update-from-binance' && request.method() === 'POST') {
        coinsConfig.XRPUSDT = false;
        await route.fulfill(jsonResponse({ status: 'success', message: '币种配置更新成功' }));
        return;
      }

      if (pathname === '/api/binance-series/config') {
        await route.fulfill(
          jsonResponse({
            status: 'success',
            message: 'binance series config loaded',
            data: mockBinanceSeriesConfig,
          })
        );
        return;
      }

      if (pathname === '/api/binance-series/repair-tracked' && request.method() === 'POST') {
        await route.fulfill(
          jsonResponse({
            status: 'success',
            message: 'repair completed',
            data: {
              success_count: 3,
              failure_count: 0,
              skipped_count: 0,
            },
          })
        );
        return;
      }

      if (pathname === '/api/binance-series/collect' && request.method() === 'POST') {
        const payload = request.postDataJSON();
        await route.fulfill(
          jsonResponse({
            status: 'success',
            message: 'collect completed',
            data: {
              affected: 1,
              series_type: payload.series_type,
              symbol: payload.symbol,
              period: payload.period,
            },
          })
        );
        return;
      }

      if (pathname === '/api/binance-series/batch-collect' && request.method() === 'POST') {
        const payload = request.postDataJSON();
        await route.fulfill(
          jsonResponse({
            status: 'success',
            message: 'batch collect completed',
            data: {
              success_count: payload.symbols.length,
              failure_count: 0,
              limit: payload.limit,
            },
          })
        );
        return;
      }

      if (pathname.startsWith('/api/coin-detail/')) {
        const symbol = pathname.split('/').pop();
        await route.fulfill(
          jsonResponse({
            status: 'success',
            message: 'coin detail loaded',
            data: {
              ...mockCoinDetail,
              symbol,
            },
          })
        );
        return;
      }

      await route.continue();
    });

    await use(context);
  },
  page: async ({ page }, use) => {
    await page.goto('/login');
    await page.locator('#username').fill('admin');
    await page.locator('#password').fill('playwright-test-password');
    await page.getByRole('button', { name: '登录' }).click();
    await expect(page).toHaveURL(/\/$/);

    await use(page);
  },
});

module.exports = {
  test,
  expect,
};
