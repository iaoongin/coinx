(() => {
  const LAST_ROUTE_STORAGE_KEY = 'coinx.pwa.last-route';
  const PWA_LAUNCH_PATH = '/pwa-start';

  const setStartStatus = (message) => {
    const status = document.querySelector('[data-pwa-start-status]');
    if (status) status.textContent = message;
  };

  const currentRoute = () => `${window.location.pathname}${window.location.search}${window.location.hash}`;

  const isRestorableRoute = (value) => {
    try {
      const target = new URL(value, window.location.origin);
      return target.origin === window.location.origin
        && !target.pathname.startsWith('/api/')
        && !target.pathname.startsWith('/static/')
        && target.pathname !== '/service-worker.js'
        && target.pathname !== PWA_LAUNCH_PATH
        && target.pathname !== '/login';
    } catch (error) {
      return false;
    }
  };

  const rememberCurrentRoute = () => {
    const route = currentRoute();
    if (!isRestorableRoute(route)) return;
    try {
      window.localStorage.setItem(LAST_ROUTE_STORAGE_KEY, route);
    } catch (error) {
      console.warn('CoinX PWA 无法保存上次页面:', error);
    }
  };

  const watchRouteChanges = () => {
    ['pushState', 'replaceState'].forEach((method) => {
      const original = window.history[method];
      window.history[method] = function wrappedHistoryMethod(...args) {
        const result = original.apply(this, args);
        rememberCurrentRoute();
        return result;
      };
    });
    window.addEventListener('popstate', rememberCurrentRoute);
    window.addEventListener('hashchange', rememberCurrentRoute);
  };

  const restoreLastRoute = async () => {
    let route = '';
    try {
      route = window.localStorage.getItem(LAST_ROUTE_STORAGE_KEY) || '';
    } catch (error) {
      return false;
    }
    if (!isRestorableRoute(route)) return false;

    const target = new URL(route, window.location.origin);
    setStartStatus('正在检查上次页面...');
    try {
      const response = await fetch(target.href, {
        method: 'HEAD',
        credentials: 'same-origin',
        cache: 'no-store',
      });
      const finalPath = new URL(response.url).pathname;
      if (response.status === 404 || finalPath === '/login' || !response.ok) {
        if (response.status === 404) {
          window.localStorage.removeItem(LAST_ROUTE_STORAGE_KEY);
          setStartStatus('上次页面已失效，正在打开首页...');
        }
        return false;
      }
    } catch (error) {
      return false;
    }

    window.location.replace(target.href);
    return true;
  };

  const bootstrapPwaRoute = async () => {
    if (window.location.pathname === PWA_LAUNCH_PATH) {
      if (await restoreLastRoute()) return;
      setStartStatus('正在打开首页...');
      window.location.replace('/');
      return;
    }
    rememberCurrentRoute();
  };

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', bootstrapPwaRoute, { once: true });
  } else {
    bootstrapPwaRoute();
  }
  watchRouteChanges();

  if (!('serviceWorker' in navigator)) return;

  window.addEventListener('load', () => {
    navigator.serviceWorker.register('/service-worker.js', { scope: '/' })
      .catch((error) => console.warn('CoinX PWA 注册失败:', error));
  });
})();
