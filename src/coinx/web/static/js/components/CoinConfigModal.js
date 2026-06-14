// CoinConfigModal.js - 币种配置弹窗组件
// 依赖: Vue 3 (vue.global.js), dark-theme.css

// 注入组件样式
(function() {
  if (document.getElementById('coin-config-modal-styles')) return;
  const style = document.createElement('style');
  style.id = 'coin-config-modal-styles';
  style.textContent = `
    .coin-modal-mask {
      position: fixed;
      inset: 0;
      background: rgba(0, 0, 0, 0.5);
      backdrop-filter: blur(4px);
      z-index: 9000;
      display: flex;
      align-items: center;
      justify-content: center;
      animation: coinModalFadeIn 0.2s ease;
    }

    .coin-modal {
      background: var(--bg-card, #141414);
      border: 1px solid var(--border-default, rgba(212,175,55,0.12));
      border-radius: var(--card-radius-lg, 16px);
      box-shadow: var(--shadow-card, 0 4px 24px rgba(0,0,0,0.4));
      width: 480px;
      max-width: 90vw;
      max-height: 70vh;
      display: flex;
      flex-direction: column;
      animation: coinModalSlideUp 0.3s ease;
      position: relative;
    }

    @keyframes coinModalFadeIn {
      from { opacity: 0; }
      to { opacity: 1; }
    }

    @keyframes coinModalSlideUp {
      from { opacity: 0; transform: translateY(20px); }
      to { opacity: 1; transform: translateY(0); }
    }

    .coin-modal-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 16px 20px;
      border-bottom: 1px solid var(--border-subtle, rgba(212,175,55,0.06));
      flex-shrink: 0;
    }

    .coin-modal-title {
      margin: 0;
      font-size: 18px;
      font-weight: 600;
      color: var(--text-primary, #fff);
    }

    .coin-modal-close {
      background: none;
      border: none;
      color: var(--text-muted, #888);
      cursor: pointer;
      padding: 4px;
      border-radius: 6px;
      display: flex;
      align-items: center;
      justify-content: center;
      transition: all 0.15s ease;
    }

    .coin-modal-close:hover {
      background: var(--bg-hover, #242424);
      color: var(--text-primary, #fff);
    }

    .coin-modal-body {
      flex: 1;
      overflow: hidden;
      padding: 16px 20px;
      display: flex;
      flex-direction: column;
    }

    .coin-modal-loading,
    .coin-modal-error {
      padding: 40px 20px;
      text-align: center;
      color: var(--text-muted, #888);
      font-size: 14px;
    }

    .coin-modal-retry {
      margin-top: 12px;
      padding: 6px 16px;
      background: var(--bg-elevated, #1a1a1a);
      border: 1px solid var(--border-default, rgba(212,175,55,0.12));
      border-radius: 6px;
      color: var(--text-primary, #fff);
      cursor: pointer;
      font-size: 13px;
    }

    .coin-transfer {
      display: grid;
      grid-template-columns: 1fr 44px 1fr;
      gap: 10px;
      align-items: start;
      flex: 1;
      min-height: 0;
    }

    .coin-transfer-panel {
      background: var(--bg-secondary, #0f0f0f);
      border: 1px solid var(--border-default, rgba(212,175,55,0.12));
      border-radius: var(--card-radius, 12px);
      display: flex;
      flex-direction: column;
      height: calc(70vh - 140px);
      max-height: 420px;
    }

    .coin-transfer-header {
      padding: 10px 14px;
      border-bottom: 1px solid var(--border-default, rgba(212,175,55,0.12));
      font-weight: 600;
      color: var(--text-primary, #fff);
      font-size: 13px;
      flex-shrink: 0;
    }

    .coin-transfer-search {
      padding: 10px;
      border-bottom: 1px solid var(--border-default, rgba(212,175,55,0.12));
      flex-shrink: 0;
    }

    .coin-transfer-search input {
      width: 100%;
      padding: 7px 10px;
      background: var(--bg-card, #141414);
      border: 1px solid var(--border-default, rgba(212,175,55,0.12));
      border-radius: 6px;
      color: var(--text-primary, #fff);
      font-size: 13px;
      outline: none;
      box-sizing: border-box;
    }

    .coin-transfer-search input:focus {
      border-color: var(--gold-primary, #d4af37);
    }

    .coin-transfer-list {
      flex: 1;
      overflow-y: auto;
      padding: 6px;
    }

    .coin-transfer-item {
      padding: 7px 10px;
      border-radius: 6px;
      cursor: pointer;
      font-size: 13px;
      color: var(--text-secondary, #c4c4c4);
      transition: all 0.15s ease;
      display: flex;
      align-items: center;
      gap: 8px;
    }

    .coin-transfer-item:hover {
      background: var(--bg-hover, #242424);
      color: var(--text-primary, #fff);
    }

    .coin-transfer-item input[type="checkbox"] {
      accent-color: var(--gold-primary, #d4af37);
    }

    .coin-transfer-empty {
      padding: 20px;
      text-align: center;
      color: var(--text-muted, #888);
      font-size: 13px;
    }

    .coin-transfer-actions {
      display: flex;
      flex-direction: column;
      justify-content: center;
      gap: 8px;
      padding-top: 60px;
    }

    .coin-transfer-btn {
      width: 36px;
      height: 36px;
      border-radius: 8px;
      background: var(--bg-elevated, #1a1a1a);
      border: 1px solid var(--border-default, rgba(212,175,55,0.12));
      color: var(--text-secondary, #c4c4c4);
      cursor: pointer;
      display: flex;
      align-items: center;
      justify-content: center;
      font-size: 18px;
      font-weight: 700;
      transition: all 0.15s ease;
    }

    .coin-transfer-btn:hover:not(:disabled) {
      background: var(--bg-hover, #242424);
      color: var(--text-primary, #fff);
      border-color: var(--border-strong, rgba(212,175,55,0.2));
    }

    .coin-transfer-btn:disabled {
      opacity: 0.4;
      cursor: not-allowed;
    }

    .coin-modal-toast {
      position: absolute;
      top: -48px;
      left: 50%;
      transform: translateX(-50%);
      padding: 10px 20px;
      border-radius: 8px;
      font-size: 13px;
      animation: coinModalFadeIn 0.2s ease;
      white-space: nowrap;
    }

    .coin-modal-toast-success {
      background: var(--positive, #34d399);
      color: white;
    }

    .coin-modal-toast-error {
      background: var(--negative, #f87171);
      color: white;
    }

    @media (max-width: 768px) {
      .coin-transfer {
        grid-template-columns: 1fr;
      }
      .coin-transfer-actions {
        flex-direction: row;
        justify-content: center;
        padding-top: 0;
      }
      .coin-transfer-panel {
        height: 240px;
      }
    }
  `;
  document.head.appendChild(style);
})();

const CoinConfigModal = {
  name: 'CoinConfigModal',
  props: {
    visible: { type: Boolean, default: false }
  },
  emits: ['update:tracked', 'close'],
  template: `
    <teleport to="body">
      <div v-if="visible" class="coin-modal-mask" @click.self="$emit('close')">
        <div class="coin-modal" @keydown.escape="$emit('close')" ref="modalRef" role="dialog" aria-modal="true" aria-labelledby="coin-modal-title" tabindex="-1">
          <div class="coin-modal-header">
            <h2 class="coin-modal-title" id="coin-modal-title">币种配置</h2>
            <button class="coin-modal-close" @click="$emit('close')" aria-label="关闭">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="18" height="18">
                <line x1="18" y1="6" x2="6" y2="18"></line>
                <line x1="6" y1="6" x2="18" y2="18"></line>
              </svg>
            </button>
          </div>

          <div v-if="loading" class="coin-modal-loading">加载中...</div>
          <div v-else-if="loadError" class="coin-modal-error">
            <span>加载失败</span>
            <button class="coin-modal-retry" @click="loadCoinsConfig">重试</button>
          </div>
          <div v-else class="coin-modal-body">
            <div class="coin-transfer">
              <div class="coin-transfer-panel">
                <div class="coin-transfer-header">未跟踪的币种 ({{ filteredUntracked.length }})</div>
                <div class="coin-transfer-search">
                  <input type="text" v-model="searchLeft" placeholder="搜索币种..." ref="searchRef">
                </div>
                <div class="coin-transfer-list">
                  <label class="coin-transfer-item" v-for="coin in filteredUntracked" :key="coin">
                    <input type="checkbox" :value="coin" v-model="leftChecked">
                    <span>{{ coin }}</span>
                  </label>
                  <div v-if="filteredUntracked.length === 0" class="coin-transfer-empty">暂无币种</div>
                </div>
              </div>

              <div class="coin-transfer-actions">
                <button class="coin-transfer-btn" @click="moveToRight" :disabled="leftChecked.length === 0" title="添加跟踪">›</button>
                <button class="coin-transfer-btn" @click="moveToLeft" :disabled="rightChecked.length === 0" title="移除跟踪">‹</button>
              </div>

              <div class="coin-transfer-panel">
                <div class="coin-transfer-header">跟踪的币种 ({{ filteredTracked.length }})</div>
                <div class="coin-transfer-search">
                  <input type="text" v-model="searchRight" placeholder="搜索币种...">
                </div>
                <div class="coin-transfer-list">
                  <label class="coin-transfer-item" v-for="coin in filteredTracked" :key="coin">
                    <input type="checkbox" :value="coin" v-model="rightChecked">
                    <span>{{ coin }}</span>
                  </label>
                  <div v-if="filteredTracked.length === 0" class="coin-transfer-empty">暂无跟踪币种</div>
                </div>
              </div>
            </div>
          </div>

          <div class="coin-modal-toast" v-if="toast.show" :class="'coin-modal-toast-' + toast.type">
            {{ toast.message }}
          </div>
        </div>
      </div>
    </teleport>
  `,
  setup(props, { emit }) {
    const { ref, computed, watch, nextTick } = Vue;

    const allCoins = ref([]);
    const trackedCoins = ref([]);
    const leftChecked = ref([]);
    const rightChecked = ref([]);
    const searchLeft = ref('');
    const searchRight = ref('');
    const loading = ref(false);
    const loadError = ref(false);
    const modalRef = ref(null);
    const searchRef = ref(null);
    const toast = ref({ show: false, message: '', type: 'success' });

    const showToast = (message, type = 'success') => {
      toast.value = { show: true, message, type };
      setTimeout(() => { toast.value.show = false; }, 2000);
    };

    const filteredUntracked = computed(() => {
      const untracked = allCoins.value.filter(c => !trackedCoins.value.includes(c));
      if (!searchLeft.value) return untracked;
      const s = searchLeft.value.toLowerCase();
      return untracked.filter(c => c.toLowerCase().includes(s));
    });

    const filteredTracked = computed(() => {
      if (!searchRight.value) return trackedCoins.value;
      const s = searchRight.value.toLowerCase();
      return trackedCoins.value.filter(c => c.toLowerCase().includes(s));
    });

    const loadCoinsConfig = async () => {
      loading.value = true;
      loadError.value = false;
      try {
        const response = await fetch('/api/coins-config');
        const data = await response.json();
        if (data.status === 'success') {
          const coinsConfig = data.data;
          allCoins.value = Object.keys(coinsConfig).sort();
          trackedCoins.value = Object.entries(coinsConfig)
            .filter(([, isTracked]) => isTracked)
            .map(([symbol]) => symbol)
            .sort();
        } else {
          loadError.value = true;
        }
      } catch (error) {
        console.error('获取币种配置失败:', error);
        loadError.value = true;
      } finally {
        loading.value = false;
      }
    };

    const updateCoinsTracking = async (updates) => {
      try {
        const results = await Promise.all(
          updates.map(update =>
            fetch('/api/coins-config/track', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify(update)
            }).then(res => res.json())
          )
        );
        const failed = results.filter(r => r.status !== 'success');
        if (failed.length > 0) {
          showToast(`${failed.length} 个更新失败`, 'error');
        } else {
          showToast('更新成功');
        }
        await loadCoinsConfig();
        emit('update:tracked', [...trackedCoins.value]);
      } catch (error) {
        console.error('更新失败:', error);
        showToast('更新失败', 'error');
      }
    };

    const moveToRight = async () => {
      const symbols = [...leftChecked.value];
      const updates = symbols.map(symbol => ({ symbol, tracked: true }));
      leftChecked.value = [];
      await updateCoinsTracking(updates);
    };

    const moveToLeft = async () => {
      const symbols = [...rightChecked.value];
      const updates = symbols.map(symbol => ({ symbol, tracked: false }));
      rightChecked.value = [];
      await updateCoinsTracking(updates);
    };

    watch(() => props.visible, (val) => {
      if (val) {
        document.body.style.overflow = 'hidden';
        loadCoinsConfig();
        nextTick(() => {
          if (modalRef.value) modalRef.value.focus();
          if (searchRef.value) searchRef.value.focus();
        });
      } else {
        document.body.style.overflow = '';
        searchLeft.value = '';
        searchRight.value = '';
        leftChecked.value = [];
        rightChecked.value = [];
      }
    });

    return {
      allCoins, trackedCoins,
      leftChecked, rightChecked,
      searchLeft, searchRight,
      loading, loadError,
      filteredUntracked, filteredTracked,
      moveToRight, moveToLeft,
      loadCoinsConfig,
      modalRef, searchRef,
      toast
    };
  }
};
