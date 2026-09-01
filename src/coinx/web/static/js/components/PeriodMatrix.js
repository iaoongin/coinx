(function registerPeriodMatrix(global) {
  const { computed } = global.Vue;
  const intervals = ['5m', '15m', '30m', '1h', '4h', '12h', '24h', '48h', '72h', '168h'];

  const normalizeChanges = (coin) => {
    if (!Array.isArray(coin.changes)) return coin.changes || {};
    return coin.changes.reduce((acc, item) => {
      acc[item.interval] = item;
      return acc;
    }, {});
  };

  const formatExchangeName = (exchange) => String(exchange || '--').toUpperCase();

  const formatTakerRejectReason = (item) => {
    const reasons = Array.isArray(item?.taker_reason) ? item.taker_reason : [];
    if (reasons.length === 0) return '';
    const first = reasons[0];
    const pct = first?.details?.health_pct;
    if (first.reason === 'missing_taker_history') return '无Taker数据';
    if (typeof pct === 'number') return `数据完整度${pct}%`;
    return '数据不完整';
  };

  const formatTakerStatusText = (item) => {
    const status = String(item?.taker_status || '').toLowerCase();
    if (status === 'available') return '净流入可用';
    if (status === 'missing') {
      const reason = formatTakerRejectReason(item);
      return reason ? `净流入不可用: ${reason}` : '净流入不可用';
    }
    if (status === 'unreliable') return '净流入不可用: 数据源不可靠';
    if (status === 'excluded') return '净流入不可用: 交易所未纳入聚合';
    if (status === 'unsupported') return '净流入不可用: 交易所不支持';
    if (status === 'unknown') return '净流入状态未知';
    return status ? `净流入状态: ${status}` : '';
  };

  const formatChange = (value) => {
    if (typeof value !== 'number' || Number.isNaN(value)) return 'N/A';
    return `${value > 0 ? '+' : ''}${value.toFixed(2)}%`;
  };

  const formatNetInflow = (value) => {
    if (typeof value !== 'number' || Number.isNaN(value)) return 'N/A';
    const abs = Math.abs(value);
    if (abs >= 1e9) return `$${(value / 1e9).toFixed(2)}B`;
    if (abs >= 1e6) return `$${(value / 1e6).toFixed(2)}M`;
    if (abs >= 1e3) return `$${(value / 1e3).toFixed(2)}K`;
    return `$${value.toFixed(2)}`;
  };

  const valueClass = (value) => {
    if (typeof value !== 'number') return 'neutral';
    return value > 0 ? 'positive' : value < 0 ? 'negative' : 'neutral';
  };

  global.PeriodMatrix = {
    props: {
      coin: { type: Object, default: () => ({}) },
    },
    setup(props) {
      const coin = computed(() => props.coin || {});
      const matrixRows = computed(() => {
        const current = coin.value;
        const changes = normalizeChanges(current);
        return intervals.map((interval) => ({
          interval,
          ...(changes[interval] || {}),
          net_inflow: current.net_inflow?.[interval],
          net_inflow_value: current.net_inflow_value?.[interval],
          net_inflow_value_formatted: current.net_inflow_value_formatted?.[interval],
        }));
      });

      const allTakerExchanges = (current) => (current.exchange_statuses || [])
        .filter((item) => item && item.exchange)
        .map((item) => ({ ...item, label: formatExchangeName(item.exchange) }));

      const takerExchangeBadgeClass = (item) => {
        const exchange = String(item?.exchange || '').toLowerCase();
        const exchangeClass = exchange ? `taker-${exchange}` : '';
        const inactiveStatuses = new Set(['missing', 'excluded', 'unsupported', 'unreliable', 'cooldown', 'error', 'unavailable']);
        const inactive = (
          item?.supported === false
          || item?.available === false
          || item?.included === false
          || item?.enabled === false
          || item?.supports_taker === false
          || inactiveStatuses.has(String(item?.taker_status || '').toLowerCase())
        );
        return inactive ? ['taker-tag-excluded', exchangeClass] : [exchangeClass];
      };

      const takerExchangeBadgeTitle = (item) => [
        formatExchangeName(item?.exchange),
        formatTakerStatusText(item),
      ].filter(Boolean).join(' · ');

      return {
        coin,
        matrixRows,
        allTakerExchanges,
        takerExchangeBadgeClass,
        takerExchangeBadgeTitle,
        valueClass,
        formatChange,
        formatNetInflow,
      };
    },
    template: `
      <div class="matrix-wrap">
        <div class="matrix">
          <div class="cell matrix-head window-cell">窗口</div>
          <div class="cell matrix-head matrix-head-net-inflow">
            <div class="matrix-head-title">净流入</div>
            <div v-if="allTakerExchanges(coin).length" class="taker-exchanges-line">
              <span
                v-for="item in allTakerExchanges(coin)"
                :key="coin.symbol + '-taker-' + item.exchange + '-' + (item.status || 'unknown')"
                class="taker-tag"
                :class="takerExchangeBadgeClass(item)"
                :title="takerExchangeBadgeTitle(item)"
              >{{ item.label }}</span>
            </div>
          </div>
          <div class="cell matrix-head">价格</div>
          <div class="cell matrix-head">价格%</div>
          <div class="cell matrix-head">量</div>
          <div class="cell matrix-head">量%</div>
          <div class="cell matrix-head">价值</div>
          <div class="cell matrix-head">价值%</div>

          <template v-for="row in matrixRows" :key="coin.symbol + row.interval">
            <div class="cell window-cell">{{ row.interval }}</div>
            <div class="cell metric" :class="valueClass(row.net_inflow_value ?? row.net_inflow)">
              {{ row.net_inflow_value_formatted || formatNetInflow(row.net_inflow_value ?? row.net_inflow) }}
            </div>
            <div class="cell raw-value">{{ row.current_price_formatted || 'N/A' }}</div>
            <div class="cell metric" :class="valueClass(row.price_change_percent)">
              {{ formatChange(row.price_change_percent) }}
            </div>
            <div class="cell raw-value">{{ row.open_interest_formatted || 'N/A' }}</div>
            <div class="cell metric" :class="valueClass(row.ratio)">
              {{ formatChange(row.ratio) }}
            </div>
            <div class="cell raw-value">{{ row.open_interest_value_formatted || 'N/A' }}</div>
            <div class="cell metric" :class="valueClass(row.value_ratio)">
              {{ formatChange(row.value_ratio) }}
            </div>
          </template>
        </div>
      </div>
    `,
  };
})(window);
