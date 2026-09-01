(() => {
  const STORAGE_KEY = 'coinx.as_of_ms';
  const SHANGHAI_OFFSET_MS = 8 * 60 * 60 * 1000;
  const PATCHED_FLAG = '__coinxAsOfFetchPatched';

  const pad = (value) => String(value).padStart(2, '0');

  function parseInputValue(value) {
    const match = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})$/.exec(value || '');
    if (!match) return null;

    const [, year, month, day, hour, minute] = match.map(Number);
    const timestamp = Date.UTC(year, month - 1, day, hour, minute) - SHANGHAI_OFFSET_MS;
    const normalized = new Date(timestamp + SHANGHAI_OFFSET_MS);
    if (normalized.getUTCFullYear() !== year
      || normalized.getUTCMonth() !== month - 1
      || normalized.getUTCDate() !== day
      || normalized.getUTCHours() !== hour
      || normalized.getUTCMinutes() !== minute) {
      return null;
    }
    return Number.isSafeInteger(timestamp) && timestamp > 0 ? timestamp : null;
  }

  function formatInputValue(timestamp) {
    const date = new Date(Number(timestamp) + SHANGHAI_OFFSET_MS);
    if (Number.isNaN(date.getTime())) return '';
    return `${date.getUTCFullYear()}-${pad(date.getUTCMonth() + 1)}-${pad(date.getUTCDate())}`
      + `T${pad(date.getUTCHours())}:${pad(date.getUTCMinutes())}`;
  }

  function formatPickerValue(timestamp) {
    return formatInputValue(timestamp).replace('T', ' ');
  }

  function pickerDateToTimestamp(date) {
    if (!(date instanceof Date) || Number.isNaN(date.getTime())) return null;
    const timestamp = Date.UTC(
      date.getFullYear(),
      date.getMonth(),
      date.getDate(),
      date.getHours(),
      date.getMinutes(),
    ) - SHANGHAI_OFFSET_MS;
    return readTimestamp(timestamp);
  }

  function readTimestamp(value) {
    const timestamp = Number(value);
    return Number.isSafeInteger(timestamp) && timestamp > 0 ? timestamp : null;
  }

  function getActiveTimestamp() {
    const queryParam = new URL(window.location.href).searchParams.get('as_of_ms');
    const queryValue = readTimestamp(queryParam);
    if (queryParam !== null) {
      if (queryValue === null) window.localStorage.removeItem(STORAGE_KEY);
      else window.localStorage.setItem(STORAGE_KEY, String(queryValue));
      return queryValue;
    }
    return readTimestamp(window.localStorage.getItem(STORAGE_KEY));
  }

  function setTimestamp(timestamp) {
    if (timestamp === null) window.localStorage.removeItem(STORAGE_KEY);
    else window.localStorage.setItem(STORAGE_KEY, String(timestamp));
  }

  function updateCurrentUrl(timestamp) {
    const url = new URL(window.location.href);
    if (timestamp === null) url.searchParams.delete('as_of_ms');
    else url.searchParams.set('as_of_ms', String(timestamp));
    window.location.assign(url.toString());
  }

  function shouldAttach(url, method) {
    if (method !== 'GET' || url.origin !== window.location.origin || !url.pathname.startsWith('/api/')) {
      return false;
    }
    return url.pathname !== '/api/update'
      && !url.pathname.endsWith('/refresh')
      && !url.pathname.startsWith('/api/coins-config')
      && !url.pathname.startsWith('/api/task-jobs')
      && !url.pathname.startsWith('/api/health');
  }

  function patchFetch() {
    if (window[PATCHED_FLAG] || typeof window.fetch !== 'function') return;
    const nativeFetch = window.fetch.bind(window);
    window[PATCHED_FLAG] = true;

    window.fetch = (input, init = {}) => {
      const sourceUrl = typeof input === 'string' ? input : input?.url;
      if (!sourceUrl) return nativeFetch(input, init);

      const url = new URL(sourceUrl, window.location.href);
      const method = String(init.method || input?.method || 'GET').toUpperCase();
      const timestamp = getActiveTimestamp();
      if (timestamp !== null && shouldAttach(url, method) && !url.searchParams.has('as_of_ms')) {
        url.searchParams.set('as_of_ms', String(timestamp));
        if (typeof input === 'string') input = url.toString();
        else input = new Request(url.toString(), input);
      }
      return nativeFetch(input, init);
    };
  }

  function decorateNavigationLinks(timestamp) {
    if (timestamp === null) return;
    document.querySelectorAll('.nav-container a[href]').forEach((link) => {
      const url = new URL(link.href, window.location.href);
      if (url.origin !== window.location.origin || url.pathname === '/login') return;
      url.searchParams.set('as_of_ms', String(timestamp));
      link.href = url.toString();
    });
  }

  function initControl() {
    const control = document.querySelector('[data-as-of-control]');
    if (!control) return;

    const input = control.querySelector('[data-as-of-input]');
    const status = control.querySelector('[data-as-of-status]');
    const applyButton = control.querySelector('[data-as-of-apply]');
    const clearButton = control.querySelector('[data-as-of-clear]');
    const timestamp = getActiveTimestamp();
    let pendingTimestamp = timestamp;

    const updateStatus = (message, active = pendingTimestamp !== null) => {
      status.textContent = message;
      status.classList.toggle('is-active', active);
    };
    updateStatus(timestamp === null ? '实时数据' : `回放至 ${formatPickerValue(timestamp)}`);

    if (typeof window.flatpickr === 'function') {
      const syncPickerTimestamp = (selectedDates, dateString, instance) => {
        if (!selectedDates.length) {
          pendingTimestamp = null;
        } else {
          const selectedDate = new Date(selectedDates[0].getTime());
          const hour = Number(instance?.hourElement?.value);
          const minute = Number(instance?.minuteElement?.value);
          if (Number.isInteger(hour) && Number.isInteger(minute)) {
            selectedDate.setHours(hour, minute, 0, 0);
          }
          pendingTimestamp = pickerDateToTimestamp(selectedDate);
        }
        updateStatus(
          pendingTimestamp === null
            ? '请选择数据时点'
            : `待应用 ${formatPickerValue(pendingTimestamp)}`,
          pendingTimestamp !== null,
        );
      };
      window.flatpickr(input, {
        allowInput: false,
        dateFormat: 'Y-m-d H:i',
        defaultDate: timestamp === null ? null : formatPickerValue(timestamp),
        disableMobile: true,
        enableTime: true,
        locale: window.flatpickr.l10ns.zh,
        minuteIncrement: 5,
        onChange: syncPickerTimestamp,
        onClose: syncPickerTimestamp,
        onValueUpdate: syncPickerTimestamp,
        onReady: (selectedDates, dateString, instance) => {
          const syncTimeControls = () => syncPickerTimestamp(
            instance.selectedDates,
            input.value,
            instance,
          );
          [instance.hourElement, instance.minuteElement].forEach((element) => {
            if (!element) return;
            element.addEventListener('change', syncTimeControls);
            element.addEventListener('input', syncTimeControls);
          });
        },
        time_24hr: true,
      });
    } else {
      input.readOnly = false;
      input.type = 'datetime-local';
      input.step = '300';
      if (timestamp !== null) input.value = formatInputValue(timestamp);
    }

    applyButton.addEventListener('click', () => {
      const nextTimestamp = typeof window.flatpickr === 'function'
        ? pendingTimestamp
        : parseInputValue(input.value);
      if (nextTimestamp === null) {
        updateStatus('请输入有效时间', false);
        input.focus();
        return;
      }
      setTimestamp(nextTimestamp);
      updateCurrentUrl(nextTimestamp);
    });

    clearButton.addEventListener('click', () => {
      pendingTimestamp = null;
      if (input._flatpickr) input._flatpickr.clear();
      setTimestamp(null);
      updateCurrentUrl(null);
    });

    decorateNavigationLinks(timestamp);
  }

  patchFetch();
  document.addEventListener('DOMContentLoaded', initControl, { once: true });
})();
