// 页面加载完成后获取数据
document.addEventListener("DOMContentLoaded", function () {
  loadCoinsData();
  // 每5分钟刷新一次数据
  setInterval(loadCoinsData, 300000);
});

// 缓存所有币种数据
let allCoinsData = [];
let isDataLoading = false;
let countdownInterval;
let lastUpdateTime = 0;
let isWaitingForUpdate = false; // 添加标志来跟踪是否正在等待更新

// 获取币种数据
function loadCoinsData() {
  // 如果已经在加载数据，则跳过
  if (isDataLoading) {
    return;
  }

  isDataLoading = true;
  showLoading(true);

  // 先显示缓存数据（如果有的话）
  if (allCoinsData.length > 0) {
    renderCoinsTable(allCoinsData);
  }

  // 直接获取数据，不主动触发更新（更新由后台定时任务处理）
  fetch("/api/coins")
    .then((response) => response.json())
    .then((result) => {
      if (result.status === "success") {
        // 更新缓存
        allCoinsData = result.data;
        // 显示最新数据
        renderCoinsTable(result.data);
        
        // 更新最后更新时间和倒计时
        if (result.cache_update_time) {
          updateLastUpdateTime(result.cache_update_time);
        }
      } else {
        throw new Error(result.message);
      }
      isDataLoading = false;
      isWaitingForUpdate = false; // 数据加载完成，重置等待标志
      showLoading(false);
    })
    .catch((error) => {
      console.error("Error:", error);
      // 即使更新失败也显示缓存数据
      if (allCoinsData.length > 0) {
        renderCoinsTable(allCoinsData);
      } else {
        showMessage("数据加载失败", "error");
      }
      isDataLoading = false;
      isWaitingForUpdate = false; // 数据加载完成，重置等待标志
      showLoading(false);
    });
}

// 更新最后更新时间和倒计时
function updateLastUpdateTime(cacheUpdateTime) {
  const lastUpdateElement = document.getElementById("last-update-time");
  const countdownElement = document.getElementById("countdown");

  // 更新最后更新时间
  const updateTime = new Date(cacheUpdateTime);
  lastUpdateElement.textContent = updateTime.toLocaleString("zh-CN");
  lastUpdateTime = cacheUpdateTime;

  // 开始倒计时（基于缓存更新时间计算下次刷新时间）
  updateCountdownBasedOnCache();
}

// 基于缓存更新时间计算倒计时
function updateCountdownBasedOnCache() {
  const countdownElement = document.getElementById("countdown");

  // 如果没有更新时间，不显示倒计时
  if (!lastUpdateTime) {
    countdownElement.textContent = "--";
    return;
  }

  // 计算下次刷新时间（下一个5分钟时间点）
  const now = new Date();
  const lastUpdate = new Date(lastUpdateTime);
  
  // 计算基于最后更新时间的下一个5分钟刷新点
  const nextRefreshTime = new Date(lastUpdate);
  nextRefreshTime.setMinutes(Math.floor(lastUpdate.getMinutes() / 5) * 5);
  nextRefreshTime.setSeconds(0);
  nextRefreshTime.setMilliseconds(0);
  // 加上5分钟得到下次刷新时间
  nextRefreshTime.setTime(nextRefreshTime.getTime() + 5 * 60 * 1000);

  // 如果计算出的时间已经过去，则重新计算
  if (nextRefreshTime <= now) {
    const minutesSinceLastUpdate = Math.floor((now - lastUpdate) / (60 * 1000));
    const cycles = Math.ceil(minutesSinceLastUpdate / 5);
    nextRefreshTime.setTime(lastUpdate.getTime() + cycles * 5 * 60 * 1000);
  }

  // 更新倒计时
  function updateCountdown() {
    const now = new Date();
    const timeDiff = nextRefreshTime - now;

    if (timeDiff <= 0) {
      // 如果已经在等待更新，则不重复触发
      if (isWaitingForUpdate) {
        return;
      }
      
      // 设置等待标志
      isWaitingForUpdate = true;
      
      // 时间到了，清除当前的倒计时
      if (countdownInterval) {
        clearInterval(countdownInterval);
        countdownInterval = null;
      }
      // 显示加载中状态
      countdownElement.textContent = "加载中...";
      
      // 获取最新数据
      loadCoinsData();
      
      // 设置一个最大等待时间，避免无限等待
      setTimeout(() => {
        if (isWaitingForUpdate) {
          isWaitingForUpdate = false;
          // 重新启动倒计时
          updateCountdownBasedOnCache();
        }
      }, 30000); // 最多等待30秒
      
      return;
    }

    // 计算剩余时间
    const seconds = Math.floor(timeDiff / 1000);
    const minutes = Math.floor(seconds / 60);
    const secs = seconds % 60;
    countdownElement.textContent = `${minutes}:${secs.toString().padStart(2, "0")}`;
  }

  // 立即更新一次
  updateCountdown();

  // 每秒更新倒计时
  if (countdownInterval) {
    clearInterval(countdownInterval);
  }
  countdownInterval = setInterval(updateCountdown, 1000);
}

// 渲染币种表格
function renderCoinsTable(coinsData) {
  const tableBody = document.getElementById("coinsTableBody");
  tableBody.innerHTML = "";

  coinsData.forEach((coin) => {
    const row = document.createElement("tr");

    // 币种
    const symbolCell = document.createElement("td");
    symbolCell.textContent = coin.symbol;
    row.appendChild(symbolCell);

    // 当前持仓量
    const currentCell = document.createElement("td");
    currentCell.textContent = coin.current_open_interest
      ? formatNumber(coin.current_open_interest)
      : "N/A";
    row.appendChild(currentCell);

    // 各时间间隔的变化比例
    const intervals = ["5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h"];

    intervals.forEach((interval) => {
      if (interval === "5m") return; // 跳过5m自身

      const cell = document.createElement("td");
      const change = coin.changes[interval];

      if (change !== null && change !== undefined) {
        cell.textContent = change.toFixed(2);
        if (change > 0) {
          cell.classList.add("positive");
        } else if (change < 0) {
          cell.classList.add("negative");
        }
      } else {
        cell.textContent = "N/A";
      }

      row.appendChild(cell);
    });

    tableBody.appendChild(row);
  });
}

// 格式化数字
function formatNumber(num) {
  if (num >= 1000000) {
    return (num / 1000000).toFixed(2) + "M";
  } else if (num >= 1000) {
    return (num / 1000).toFixed(2) + "K";
  } else {
    return num.toFixed(2);
  }
}

// 筛选币种（前端筛选）
function filterCoins() {
  const symbolFilter = document
    .getElementById("symbolFilter")
    .value.toUpperCase();

  // 如果没有筛选条件，显示所有数据
  if (!symbolFilter) {
    renderCoinsTable(allCoinsData);
    return;
  }

  // 根据筛选条件过滤数据
  const filteredData = allCoinsData.filter((coin) =>
    coin.symbol.includes(symbolFilter)
  );

  // 显示筛选后的数据
  renderCoinsTable(filteredData);
}

// 手动更新数据函数
function updateData() {
  showMessage("正在更新数据...", "success");

  // 强制更新数据
  isDataLoading = true;
  showLoading(true);

  // 手动触发更新
  fetch("/api/update")
    .then((response) => response.json())
    .then((updateResult) => {
      if (updateResult.status === "success") {
        // 更新成功后再获取数据
        return fetch("/api/coins");
      } else {
        throw new Error(updateResult.message);
      }
    })
    .then((response) => response.json())
    .then((result) => {
      if (result.status === "success") {
        // 更新缓存
        allCoinsData = result.data;
        // 显示最新数据
        renderCoinsTable(result.data);
        showMessage("数据更新成功", "success");
        
        // 更新最后更新时间和倒计时
        if (result.cache_update_time) {
          updateLastUpdateTime(result.cache_update_time);
        }
      } else {
        throw new Error(result.message);
      }
      isDataLoading = false;
      showLoading(false);
    })
    .catch((error) => {
      console.error("Error:", error);
      showMessage("数据更新失败: " + error.message, "error");
      isDataLoading = false;
      showLoading(false);
    });
}

// 显示/隐藏加载状态
function showLoading(show) {
  const loadingElement = document.getElementById("loading");
  loadingElement.style.display = show ? "block" : "none";
}

// 显示消息
function showMessage(message, type) {
  const messageElement = document.getElementById("message");
  messageElement.textContent = message;
  messageElement.className = "message " + type;

  // 3秒后自动隐藏消息
  setTimeout(() => {
    messageElement.textContent = "";
    messageElement.className = "message";
  }, 3000);
}