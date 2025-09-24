// 页面加载完成后获取数据
document.addEventListener('DOMContentLoaded', function() {
    loadCoinsData();
});

// 定时刷新数据（每5分钟）
setInterval(loadCoinsData, 300000);

// 缓存所有币种数据
let allCoinsData = [];
let isDataLoading = false;

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
    
    // 在后台更新数据
    fetch('/api/update')
        .then(response => response.json())
        .then(updateResult => {
            if (updateResult.status === 'success') {
                // 更新成功后再获取数据
                return fetch('/api/coins');
            } else {
                throw new Error(updateResult.message);
            }
        })
        .then(response => response.json())
        .then(result => {
            if (result.status === 'success') {
                // 更新缓存
                allCoinsData = result.data;
                // 显示最新数据
                renderCoinsTable(result.data);
            } else {
                throw new Error(result.message);
            }
            isDataLoading = false;
            showLoading(false);
        })
        .catch(error => {
            console.error('Error:', error);
            // 即使更新失败也显示缓存数据
            if (allCoinsData.length > 0) {
                renderCoinsTable(allCoinsData);
            } else {
                showMessage('数据加载失败', 'error');
            }
            isDataLoading = false;
            showLoading(false);
        });
}

// 渲染币种表格
function renderCoinsTable(coinsData) {
    const tableBody = document.getElementById('coinsTableBody');
    tableBody.innerHTML = '';
    
    coinsData.forEach(coin => {
        const row = document.createElement('tr');
        
        // 币种
        const symbolCell = document.createElement('td');
        symbolCell.textContent = coin.symbol;
        row.appendChild(symbolCell);
        
        // 当前持仓量
        const currentCell = document.createElement('td');
        currentCell.textContent = coin.current_open_interest ? formatNumber(coin.current_open_interest) : 'N/A';
        row.appendChild(currentCell);
        
        // 各时间间隔的变化比例
        const intervals = ['5m', '15m', '30m', '1h', '2h', '4h', '6h', '12h'];
        
        intervals.forEach(interval => {
            if (interval === '5m') return; // 跳过5m自身
            
            const cell = document.createElement('td');
            const change = coin.changes[interval];
            
            if (change !== null && change !== undefined) {
                cell.textContent = change.toFixed(2);
                if (change > 0) {
                    cell.classList.add('positive');
                } else if (change < 0) {
                    cell.classList.add('negative');
                }
            } else {
                cell.textContent = 'N/A';
            }
            
            row.appendChild(cell);
        });
        
        tableBody.appendChild(row);
    });
}

// 格式化数字
function formatNumber(num) {
    if (num >= 1000000) {
        return (num / 1000000).toFixed(2) + 'M';
    } else if (num >= 1000) {
        return (num / 1000).toFixed(2) + 'K';
    } else {
        return num.toFixed(2);
    }
}

// 筛选币种（前端筛选）
function filterCoins() {
    const symbolFilter = document.getElementById('symbolFilter').value.toUpperCase();
    
    // 如果没有筛选条件，显示所有数据
    if (!symbolFilter) {
        renderCoinsTable(allCoinsData);
        return;
    }
    
    // 根据筛选条件过滤数据
    const filteredData = allCoinsData.filter(coin => 
        coin.symbol.includes(symbolFilter)
    );
    
    // 显示筛选后的数据
    renderCoinsTable(filteredData);
}

// 手动更新数据函数
function updateData() {
    showMessage('正在更新数据...', 'success');
    
    // 强制更新数据
    isDataLoading = true;
    showLoading(true);
    
    fetch('/api/update')
        .then(response => response.json())
        .then(updateResult => {
            if (updateResult.status === 'success') {
                // 更新成功后再获取数据
                return fetch('/api/coins');
            } else {
                throw new Error(updateResult.message);
            }
        })
        .then(response => response.json())
        .then(result => {
            if (result.status === 'success') {
                // 更新缓存
                allCoinsData = result.data;
                // 显示最新数据
                renderCoinsTable(result.data);
                showMessage('数据更新成功', 'success');
            } else {
                throw new Error(result.message);
            }
            isDataLoading = false;
            showLoading(false);
        })
        .catch(error => {
            console.error('Error:', error);
            showMessage('数据更新失败: ' + error.message, 'error');
            isDataLoading = false;
            showLoading(false);
        });
}

// 显示/隐藏加载状态
function showLoading(show) {
    const loadingElement = document.getElementById('loading');
    loadingElement.style.display = show ? 'block' : 'none';
}

// 显示消息
function showMessage(message, type) {
    const messageElement = document.getElementById('message');
    messageElement.textContent = message;
    messageElement.className = 'message ' + type;
    
    // 3秒后自动隐藏消息
    setTimeout(() => {
        messageElement.textContent = '';
        messageElement.className = 'message';
    }, 3000);
}