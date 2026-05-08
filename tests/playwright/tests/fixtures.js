const { test: base, expect } = require('@playwright/test');

const mockHomepageCoins = [
  {
    symbol: 'BTCUSDT',
    included_exchanges: ['binance', 'bybit'],
    missing_exchanges: ['okx'],
    status: 'partial',
    current_open_interest: 1234567.89,
    current_open_interest_formatted: '1.23M',
    current_open_interest_value: 85432123.45,
    current_open_interest_value_formatted: '85.43M',
    current_price: 69234.12,
    current_price_formatted: '69,234.12',
    price_change: 2.34,
    price_change_percent: 2.34,
    price_change_formatted: '+2.34%',
    exchange_statuses: [
      {
        exchange: 'binance',
        status: 'included',
        open_interest: 890000,
        open_interest_formatted: '0.89M',
        open_interest_value: 62000000,
        open_interest_value_formatted: '$62.00M',
        share_percent: 72.0,
        quantity_share_percent: 72.0,
      },
      {
        exchange: 'bybit',
        status: 'included',
        open_interest: 344567.89,
        open_interest_formatted: '0.34M',
        open_interest_value: 23432123.45,
        open_interest_value_formatted: '$23.43M',
        share_percent: 28.0,
        quantity_share_percent: 28.0,
      },
      {
        exchange: 'okx',
        status: 'excluded',
        open_interest: 120000,
        open_interest_formatted: '0.12M',
        open_interest_value: 8400000,
        open_interest_value_formatted: '$8.40M',
        share_percent: null,
        quantity_share_percent: null,
      },
    ],
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

const mockMarketStructureScores = [
  {
    symbol: 'BTCUSDT',
    rank_index: 1,
    total_score: 72.4,
    trend_score: 30,
    momentum_score: 25,
    position_score: 25,
    sentiment_score: 5,
    risk_score: -5,
    trend_direction: '多头趋势',
    momentum_direction: '多',
    position_structure: '多头开仓推动',
    sentiment_state: '多头共识增强',
    risk_level: '低',
    trade_signal: '强多',
    operation_advice: '只找回踩做多，不追高',
    current_time: 1711526400000,
    current_price: 69234.12,
    current_open_interest_value: 85432123.45,
    included_exchanges: ['binance', 'okx', 'bybit'],
    missing_exchanges: [],
    exchange_scores: [
      {
        exchange: 'binance',
        weight: 0.52,
        weight_percent: 52,
        total_score: 74.1,
        weighted_total_score: 38.53,
        open_interest_value: 40000000,
        open_interest: 1200000,
        current_price: 69220.11,
        ema20: 68888.1,
        ema60: 68444.1,
        atr: 123.45,
        volume_ratio: 1.34,
        taker_net_pressure_ratio: 0.18,
        open_interest_change_ratio: 0.06,
        trend_direction: '多头趋势',
        momentum_direction: '多',
        position_structure: '多头开仓推动',
        funding_rate: 0.0009,
      },
      {
        exchange: 'okx',
        weight: 0.31,
        weight_percent: 31,
        total_score: 68.3,
        weighted_total_score: 21.17,
        open_interest_value: 25000000,
        open_interest: 800000,
        current_price: 69250.45,
        ema20: 68899.3,
        ema60: 68451.7,
        atr: 121.1,
        volume_ratio: 1.12,
        taker_net_pressure_ratio: 0.12,
        open_interest_change_ratio: 0.04,
        trend_direction: '多头趋势',
        momentum_direction: '多',
        position_structure: '蓄势增仓',
        funding_rate: 0.0007,
      },
      {
        exchange: 'bybit',
        weight: 0.17,
        weight_percent: 17,
        total_score: 61.8,
        weighted_total_score: 10.51,
        open_interest_value: 18432123.45,
        open_interest: 500000,
        current_price: 69240.27,
        ema20: 68895.8,
        ema60: 68460,
        atr: 119.88,
        volume_ratio: 1.05,
        taker_net_pressure_ratio: 0.09,
        open_interest_change_ratio: 0.03,
        trend_direction: '震荡',
        momentum_direction: '弱',
        position_structure: '蓄势增仓',
        funding_rate: 0.0005,
      },
    ],
    funding_rate: 0.0008,
    binance_context: {
      top_long_short_position_ratio: { current: { long_short_ratio: 1.96 }, previous: { long_short_ratio: 1.83 } },
      top_long_short_account_ratio: { current: { long_short_ratio: 1.48 }, previous: { long_short_ratio: 1.37 } },
      global_long_short_account_ratio: { current: { long_short_ratio: 0.88 }, previous: { long_short_ratio: 0.95 } },
    },
    top_long_short_ratio: 1.96,
    global_long_short_ratio: 0.88,
    top_long_short_ratio_delta: 0.13,
    global_long_short_ratio_delta: -0.07,
    exchange_open_interest: [
      { exchange: 'binance', open_interest: 1200000, open_interest_value: 40000000, share_percent: 52, quantity_share_percent: 55, score: 74.1, weighted_score: 38.53 },
      { exchange: 'okx', open_interest: 800000, open_interest_value: 25000000, share_percent: 31, quantity_share_percent: 29, score: 68.3, weighted_score: 21.17 },
      { exchange: 'bybit', open_interest: 500000, open_interest_value: 18432123.45, share_percent: 17, quantity_share_percent: 16, score: 61.8, weighted_score: 10.51 },
    ],
    raw_inputs: {},
  },
  {
    symbol: 'ETHUSDT',
    rank_index: 2,
    total_score: 38.8,
    trend_score: 0,
    momentum_score: 25,
    position_score: 10,
    sentiment_score: 5,
    risk_score: -1,
    trend_direction: '震荡',
    momentum_direction: '多',
    position_structure: '蓄势增仓',
    sentiment_state: '多头共识增强',
    risk_level: '低',
    trade_signal: '偏多',
    operation_advice: '轻仓做多，或等待回踩确认',
    current_time: 1711526400000,
    current_price: 3521.45,
    current_open_interest_value: 43000000,
    included_exchanges: ['binance', 'okx'],
    missing_exchanges: ['bybit'],
    exchange_scores: [
      { exchange: 'binance', weight: 0.65, weight_percent: 65, total_score: 40.0, weighted_total_score: 26.0, open_interest_value: 28000000, open_interest: 800000, current_price: 3522.12, ema20: 3510.0, ema60: 3500.0, atr: 55.2, volume_ratio: 1.21, taker_net_pressure_ratio: 0.14, open_interest_change_ratio: 0.02, trend_direction: '震荡', momentum_direction: '多', position_structure: '蓄势增仓', funding_rate: 0.0004 },
      { exchange: 'okx', weight: 0.35, weight_percent: 35, total_score: 36.5, weighted_total_score: 12.8, open_interest_value: 15000000, open_interest: 420000, current_price: 3519.9, ema20: 3508.5, ema60: 3498.8, atr: 54.8, volume_ratio: 1.08, taker_net_pressure_ratio: 0.11, open_interest_change_ratio: 0.02, trend_direction: '震荡', momentum_direction: '弱', position_structure: '蓄势增仓', funding_rate: 0.0002 },
    ],
    funding_rate: 0.0003,
    binance_context: {
      top_long_short_position_ratio: { current: { long_short_ratio: 1.58 }, previous: { long_short_ratio: 1.50 } },
      top_long_short_account_ratio: { current: { long_short_ratio: 1.25 }, previous: { long_short_ratio: 1.18 } },
      global_long_short_account_ratio: { current: { long_short_ratio: 1.02 }, previous: { long_short_ratio: 0.97 } },
    },
    top_long_short_ratio: 1.58,
    global_long_short_ratio: 1.02,
    top_long_short_ratio_delta: 0.08,
    global_long_short_ratio_delta: 0.05,
    exchange_open_interest: [
      { exchange: 'binance', open_interest: 800000, open_interest_value: 28000000, share_percent: 65, quantity_share_percent: 66, score: 40.0, weighted_score: 26.0 },
      { exchange: 'okx', open_interest: 420000, open_interest_value: 15000000, share_percent: 35, quantity_share_percent: 34, score: 36.5, weighted_score: 12.8 },
    ],
    raw_inputs: {},
  },
];

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

      if (pathname === '/api/market-structure-score') {
        await route.fulfill(
          jsonResponse({
            status: 'success',
            message: 'market structure score loaded',
            data: mockMarketStructureScores,
            cache_update_time: 1711526400000,
            summary: {
              total_symbols: mockMarketStructureScores.length,
              complete_symbols: 1,
              partial_symbols: 1,
              empty_symbols: 0,
              strong_long_count: 1,
              long_count: 1,
              neutral_count: 0,
              short_count: 0,
              strong_short_count: 0,
              high_risk_count: 0,
            },
          })
        );
        return;
      }

      if (pathname === '/api/market-structure-score/refresh' && request.method() === 'POST') {
        await route.fulfill(
          jsonResponse({
            status: 'success',
            message: 'market structure score refresh completed',
            data: {
              status: 'success',
              message: 'market structure score refresh completed',
              success_count: 3,
              failure_count: 0,
              skipped_count: 0,
            },
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
    await page.locator('#password').fill('admin123');
    await page.getByRole('button', { name: '登录' }).click();
    await expect(page).toHaveURL(/\/$/);

    await use(page);
  },
});

module.exports = {
  test,
  expect,
};
