(() => {
  if (window.__coinxApiClientInstalled) return;
  window.__coinxApiClientInstalled = true;

  const nativeFetch = window.fetch.bind(window);
  const DEFAULT_TIMEOUT_MS = 30000;
  const AUTH_REFRESH_TIMEOUT_MS = 10000;
  const RETRYABLE_STATUS_CODES = new Set([408, 425, 429, 500, 502, 503, 504]);
  const SAFE_AUTH_RETRY_METHODS = new Set(['GET', 'HEAD', 'OPTIONS']);
  let refreshPromise = null;
  let toastTimer = null;
  let lastToastKey = '';
  let lastToastAt = 0;

  const isObservedUrl = (url) => (
    url.origin === window.location.origin
    && (url.pathname.startsWith('/api/') || url.pathname.startsWith('/auth/'))
  );

  const requestUrl = (input) => {
    try {
      return new URL(typeof input === 'string' ? input : input.url, window.location.href);
    } catch (_) {
      return new URL(window.location.href);
    }
  };

  const requestMethod = (input, options) => String(
    options.method || (typeof Request !== 'undefined' && input instanceof Request ? input.method : 'GET')
  ).toUpperCase();

  const messageForStatus = (status) => ({
    401: '登录状态已失效，请重新登录',
    408: '请求超时，请稍后重试',
    425: '请求暂时无法处理，请稍后重试',
    429: '请求过于频繁，请稍后重试',
    500: '服务内部错误，请稍后重试',
    502: '网关暂时不可用，请稍后重试',
    503: '服务暂时不可用，请稍后重试',
    504: '请求超时，请稍后重试',
  }[status] || null);

  const errorMessage = (payload, status) => {
    if (status && messageForStatus(status)) return messageForStatus(status);
    if (payload && typeof payload === 'object') {
      return payload.message || payload.error || payload.detail || '请求失败，请稍后重试';
    }
    return '请求失败，请稍后重试';
  };

  class CoinxApiError extends Error {
    constructor({ kind, status = null, code = null, message, retryable = false, url = '', payload = null, cause = null }) {
      super(message);
      this.name = 'CoinxApiError';
      this.kind = kind;
      this.status = status;
      this.code = code;
      this.retryable = retryable;
      this.url = url;
      this.payload = payload;
      this.cause = cause;
    }
  }

  const buildError = ({ kind, status = null, code = null, payload = null, url, cause = null, message = null }) => new CoinxApiError({
    kind,
    status,
    code: code || (payload && typeof payload === 'object' ? (payload.code || payload.error_code || null) : null),
    message: message || errorMessage(payload, status),
    retryable: kind === 'timeout' || kind === 'network' || RETRYABLE_STATUS_CODES.has(status),
    url,
    payload,
    cause,
  });

  const displayMessage = (error) => {
    if (error instanceof CoinxApiError) return error.message;
    if (error?.name === 'AbortError') return '请求超时，请稍后重试';
    return error?.message || '请求失败，请稍后重试';
  };

  const emitError = (error, suppressToast = false) => {
    const detail = error instanceof CoinxApiError
      ? error
      : buildError({ kind: 'network', url: '', cause: error, message: displayMessage(error) });
    window.dispatchEvent(new CustomEvent('coinx:api-error', { detail }));

    if (suppressToast || detail.status === 401) return;
    const now = Date.now();
    const key = `${detail.kind}:${detail.status || ''}:${detail.code || ''}:${detail.url || ''}`;
    if (key === lastToastKey && now - lastToastAt < 1500) return;
    lastToastKey = key;
    lastToastAt = now;

    const showToast = () => {
      let toast = document.querySelector('[data-coinx-api-toast]');
      if (!toast) {
        toast = document.createElement('div');
        toast.dataset.coinxApiToast = 'true';
        toast.setAttribute('role', 'alert');
        toast.setAttribute('aria-live', 'assertive');
        toast.style.cssText = [
          'position:fixed',
          'z-index:3000',
          'right:20px',
          'bottom:20px',
          'max-width:min(420px, calc(100vw - 40px))',
          'padding:11px 14px',
          'border:1px solid rgba(239,101,101,.65)',
          'border-radius:6px',
          'background:var(--bg-elevated, #151c28)',
          'color:var(--negative, #ff8d8d)',
          'box-shadow:0 12px 30px rgba(0,0,0,.28)',
          'font:500 13px/1.45 "Noto Sans SC", sans-serif',
        ].join(';');
        document.body.appendChild(toast);
      }
      toast.textContent = displayMessage(detail);
      toast.hidden = false;
      clearTimeout(toastTimer);
      toastTimer = setTimeout(() => {
        toast.hidden = true;
      }, 5000);
    };

    if (document.body) showToast();
    else document.addEventListener('DOMContentLoaded', showToast, { once: true });
  };

  const parseResponsePayload = async (response) => {
    try {
      const text = await response.clone().text();
      if (!text.trim()) return null;
      try {
        return JSON.parse(text);
      } catch (_) {
        return { raw: text.slice(0, 500) };
      }
    } catch (_) {
      return null;
    }
  };

  const observeResponse = async (response, url, suppressToast) => {
    if (response.ok) return response;
    const payload = await parseResponsePayload(response);
    const error = buildError({
      kind: 'http',
      status: response.status,
      payload,
      url,
      message: errorMessage(payload, response.status),
    });
    emitError(error, suppressToast);
    return response;
  };

  const refreshAccessToken = () => {
    if (refreshPromise) return refreshPromise;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), AUTH_REFRESH_TIMEOUT_MS);
    refreshPromise = nativeFetch('/auth/refresh', {
      method: 'POST',
      credentials: 'include',
      signal: controller.signal,
    })
      .then((response) => response.ok)
      .catch(() => false)
      .finally(() => {
        clearTimeout(timer);
        refreshPromise = null;
      });
    return refreshPromise;
  };

  const scheduleLogin = () => {
    window.setTimeout(() => {
      if (window.location.pathname !== '/login') window.location.assign('/login');
    }, 0);
  };

  const createRequestSignal = (originalSignal, timeoutMs) => {
    if (!(timeoutMs > 0)) return { signal: originalSignal, cleanup: () => {}, didTimeout: () => false };
    const controller = new AbortController();
    let timedOut = false;
    let timer = null;
    const abortFromCaller = () => controller.abort(originalSignal.reason);
    if (originalSignal) {
      if (originalSignal.aborted) controller.abort(originalSignal.reason);
      else originalSignal.addEventListener('abort', abortFromCaller, { once: true });
    }
    timer = setTimeout(() => {
      timedOut = true;
      controller.abort();
    }, timeoutMs);
    return {
      signal: controller.signal,
      cleanup: () => {
        clearTimeout(timer);
        originalSignal?.removeEventListener('abort', abortFromCaller);
      },
      didTimeout: () => timedOut,
    };
  };

  const wrappedFetch = async (input, options = {}) => {
    const requestOptions = { ...options };
    const timeoutMs = Number.isFinite(Number(requestOptions.coinxTimeoutMs))
      ? Number(requestOptions.coinxTimeoutMs)
      : DEFAULT_TIMEOUT_MS;
    const suppressToast = requestOptions.coinxSuppressGlobalError === true;
    const retryAuth = requestOptions.coinxRetryAuth !== false;
    delete requestOptions.coinxTimeoutMs;
    delete requestOptions.coinxSuppressGlobalError;
    delete requestOptions.coinxRetryAuth;

    const url = requestUrl(input);
    if (!isObservedUrl(url)) return nativeFetch(input, requestOptions);

    const method = requestMethod(input, requestOptions);
    const requestSignal = createRequestSignal(requestOptions.signal, timeoutMs);
    requestOptions.signal = requestSignal.signal;
    try {
      let response = await nativeFetch(input, requestOptions);
      if (
        response.status === 401
        && retryAuth
        && SAFE_AUTH_RETRY_METHODS.has(method)
        && url.pathname.startsWith('/api/')
      ) {
        if (await refreshAccessToken()) {
          response = await nativeFetch(input, requestOptions);
        }
      }
      if (response.status === 401 && url.pathname.startsWith('/api/')) {
        scheduleLogin();
      }
      return await observeResponse(response, url.href, suppressToast);
    } catch (error) {
      const normalized = buildError({
        kind: requestSignal.didTimeout() || error?.name === 'AbortError' ? 'timeout' : 'network',
        url: url.href,
        cause: error,
        message: requestSignal.didTimeout() || error?.name === 'AbortError'
          ? '请求超时，请稍后重试'
          : '网络连接失败，请检查网络后重试',
      });
      emitError(normalized, suppressToast);
      throw error;
    } finally {
      requestSignal.cleanup();
    }
  };

  const requestJson = async (input, options = {}) => {
    const url = requestUrl(input).href;
    const suppressToast = options.coinxSuppressGlobalError === true;
    let response;
    try {
      response = await window.fetch(input, options);
    } catch (error) {
      throw buildError({
        kind: error?.name === 'AbortError' ? 'timeout' : 'network',
        url,
        cause: error,
        message: error?.name === 'AbortError' ? '请求超时，请稍后重试' : '网络连接失败，请检查网络后重试',
      });
    }

    const payload = await parseResponsePayload(response);
    if (!response.ok) {
      throw buildError({ kind: 'http', status: response.status, payload, url });
    }
    if (!payload || typeof payload !== 'object') {
      const error = buildError({
        kind: 'application',
        code: 'invalid_json',
        url,
        message: '服务返回了无法识别的数据，请稍后重试',
      });
      emitError(error, suppressToast);
      throw error;
    }
    if (payload.status === 'error') {
      const error = buildError({ kind: 'application', payload, url });
      emitError(error, suppressToast);
      throw error;
    }
    return payload;
  };

  window.fetch = wrappedFetch;
  window.CoinxApi = Object.freeze({
    requestJson,
    refreshToken: refreshAccessToken,
    message: displayMessage,
    Error: CoinxApiError,
  });
})();
