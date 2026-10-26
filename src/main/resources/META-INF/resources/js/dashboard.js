const API_BASE = '';
const MCP_API = '/api';
let autoRefreshEnabled = false;

// Domain mode: load server context from localStorage (synchronous, before page scripts run)
var __domainCtx = null;
try { __domainCtx = JSON.parse(localStorage.getItem('jboss_domain_ctx')); } catch(e) {}
var __isDomainMode = false;
var __domainServers = [];

function _appendServerCtx(url) {
    if (!__domainCtx) return url;
    var sep = url.includes('?') ? '&' : '?';
    return url + sep + 'host=' + encodeURIComponent(__domainCtx.host)
        + '&server=' + encodeURIComponent(__domainCtx.server);
}
let autoRefreshInterval = 30;
let refreshTimer = null;

function healthClass(health) {
    if (!health) return '';
    const h = health.toUpperCase();
    if (h === 'HEALTHY') return 'healthy';
    if (h === 'WARNING') return 'warning';
    if (h === 'CRITICAL') return 'critical';
    if (h === 'MODERATE') return 'moderate';
    if (h === 'DEGRADED') return 'warning';
    return '';
}

function healthBadge(health) {
    const cls = healthClass(health);
    return `<span class="badge badge-${cls || 'unknown'}">${health || 'UNKNOWN'}</span>`;
}

function formatBytes(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function formatNumber(num) {
    if (num === null || num === undefined) return '-';
    return num.toLocaleString();
}

function formatMs(ms) {
    if (ms < 1000) return ms + ' ms';
    if (ms < 60000) return (ms / 1000).toFixed(1) + ' s';
    if (ms < 3600000) return (ms / 60000).toFixed(1) + ' min';
    return (ms / 3600000).toFixed(1) + ' h';
}

function showLoading(elementId) {
    const el = document.getElementById(elementId);
    if (el) {
        el.innerHTML = `
            <div class="skeleton skeleton-line"></div>
            <div class="skeleton skeleton-line medium"></div>
            <div class="skeleton skeleton-line short"></div>
        `;
    }
}

function showError(elementId, message) {
    const el = document.getElementById(elementId);
    if (el) {
        el.innerHTML = `<div style="color: var(--critical); font-size: 13px;">Error: ${escapeHtml(message)}</div>`;
    }
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function showToast(message, type = 'info') {
    let container = document.querySelector('.toast-container');
    if (!container) {
        container = document.createElement('div');
        container.className = 'toast-container';
        document.body.appendChild(container);
    }
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    toast.textContent = message;
    container.appendChild(toast);
    setTimeout(() => toast.remove(), 5000);
}

async function fetchAPI(endpoint) {
    const resp = await fetch(_appendServerCtx(`${API_BASE}${endpoint}`));
    if (!resp.ok) {
        const body = await resp.text().catch(() => '');
        if (resp.status === 429) {
            throw new Error(body || 'AI rate limit reached. Please wait a few seconds and try again.');
        }
        throw new Error(body || `HTTP ${resp.status}: ${resp.statusText}`);
    }
    return resp.text();
}

async function fetchData(endpoint) {
    var url = `${MCP_API}${endpoint}`;
    if (!endpoint.startsWith('/domain/')) url = _appendServerCtx(url);
    const resp = await fetch(url);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}: ${resp.statusText}`);
    return resp.json();
}

function setupAutoRefresh(refreshFn) {
    const toggle = document.getElementById('autoRefreshToggle');
    const intervalInput = document.getElementById('refreshInterval');

    if (toggle) {
        toggle.addEventListener('change', (e) => {
            autoRefreshEnabled = e.target.checked;
            if (autoRefreshEnabled) {
                startAutoRefresh(refreshFn);
            } else {
                stopAutoRefresh();
            }
        });

        toggle.checked = true;
        autoRefreshEnabled = true;
        startAutoRefresh(refreshFn);
    }

    if (intervalInput) {
        intervalInput.addEventListener('change', (e) => {
            autoRefreshInterval = parseInt(e.target.value) || 30;
            if (autoRefreshEnabled) {
                stopAutoRefresh();
                startAutoRefresh(refreshFn);
            }
        });
    }
}

function startAutoRefresh(refreshFn) {
    stopAutoRefresh();
    refreshTimer = setInterval(refreshFn, autoRefreshInterval * 1000);
    showToast(`Auto-refresh every ${autoRefreshInterval}s`, 'success');
}

function stopAutoRefresh() {
    if (refreshTimer) {
        clearInterval(refreshTimer);
        refreshTimer = null;
    }
}

function createProgressBar(percent, thresholds = { warning: 60, critical: 85 }) {
    let cls = 'healthy';
    if (percent >= thresholds.critical) cls = 'critical';
    else if (percent >= thresholds.warning) cls = 'warning';

    return `
        <div class="progress-bar">
            <div class="fill ${cls}" style="width: ${Math.min(percent, 100)}%"></div>
        </div>
    `;
}

function createMetricRow(label, value, unit = '') {
    return `
        <div class="metric-row">
            <span class="metric-label">${label}</span>
            <span class="metric-value">${value}${unit ? `<span class="stat-unit">${unit}</span>` : ''}</span>
        </div>
    `;
}

function setActiveNav() {
    const currentPage = window.location.pathname.split('/').pop() || 'index.html';
    document.querySelectorAll('.nav-link').forEach(link => {
        const href = link.getAttribute('href');
        if (href === currentPage || (currentPage === '' && href === 'index.html')) {
            link.classList.add('active');
        }
    });
}

function renderSidebar() {
    return `
    <div class="sidebar">
        <div class="sidebar-header">
            <div class="logo">JB</div>
            <div>
                <h1>JBoss Monitor</h1>
                <div class="subtitle">AI-Powered Monitoring Tool</div>
            </div>
        </div>

        <div class="nav-section">Analysis</div>
        <a href="ai-analyst.html" class="nav-link"><span class="icon">&#9733;</span> AI Analyst</a>
        <a href="rca.html" class="nav-link"><span class="icon">&#128270;</span> Root Cause Analysis</a>

        <div class="nav-section">Overview</div>
        <a href="index.html" class="nav-link"><span class="icon">&#9632;</span> Dashboard</a>
        <a href="server-status.html" class="nav-link"><span class="icon">&#9881;</span> Server Status</a>
        <a href="domain.html" class="nav-link"><span class="icon">&#127760;</span> Domain Topology</a>

        <div class="nav-section">Resources</div>
        <a href="memory.html" class="nav-link"><span class="icon">&#9618;</span> Memory &amp; GC</a>
        <a href="threads.html" class="nav-link"><span class="icon">&#9776;</span> Threads</a>

        <div class="nav-section">Data</div>
        <a href="datasources.html" class="nav-link"><span class="icon">&#9707;</span> Datasources</a>
        <a href="jboss-datasource.html" class="nav-link"><span class="icon">&#128269;</span> JBoss DB Query</a>
        <a href="transactions.html" class="nav-link"><span class="icon">&#8644;</span> Transactions</a>

        <div class="nav-section">Applications</div>
        <a href="deployments.html" class="nav-link"><span class="icon">&#9744;</span> Deployments</a>
        <a href="web-stats.html" class="nav-link"><span class="icon">&#9729;</span> Web &amp; Sessions</a>
        <a href="api-data.html" class="nav-link"><span class="icon">&#9881;</span> API Data</a>

        <div class="nav-section">Logs</div>
        <a href="server-logs.html" class="nav-link"><span class="icon">&#9998;</span> Server Logs</a>

        <div class="sidebar-footer">
            <div class="server-status">
                <span class="status-dot" id="globalStatusDot"></span>
                <span id="globalStatusText">Initializing...</span>
            </div>
        </div>
    </div>
    `;
}

function renderPageHeader(title, description) {
    return `
    <div class="page-header">
        <div>
            <h2>${title}</h2>
            <div class="page-desc">${description}</div>
        </div>
        <div class="header-actions">
            <span id="modeBadge" style="display:none;padding:4px 10px;border-radius:4px;font-size:11px;font-weight:600;font-family:var(--font-mono);letter-spacing:0.5px;"></span>
            <div id="domainServerSelector" style="display:none;">
                <select id="serverSelect" onchange="onDomainServerSelected(this.value)"
                    style="background:var(--bg-primary);color:var(--text-primary);border:1px solid var(--border-color);
                           border-radius:4px;padding:5px 10px;font-size:12px;font-family:var(--font-mono);
                           min-width:180px;cursor:pointer;">
                    <option value="">Standalone</option>
                </select>
            </div>
            <div class="auto-refresh-toggle">
                <input type="checkbox" id="darkModeToggle" checked onchange="toggleDarkMode(this.checked)">
                <label for="darkModeToggle">Dark Mode</label>
            </div>
            <div class="auto-refresh-toggle">
                <input type="checkbox" id="autoRefreshToggle">
                <label for="autoRefreshToggle">Auto</label>
                <input type="number" class="refresh-interval" id="refreshInterval" value="30" min="5" max="300">
                <span>s</span>
            </div>
            <button class="btn" onclick="location.reload()">&#8635; Refresh</button>
        </div>
    </div>
    `;
}

function onDomainServerSelected(val) {
    if (!val) {
        __domainCtx = null;
        localStorage.removeItem('jboss_domain_ctx');
    } else {
        var parts = val.split('/');
        __domainCtx = { host: parts[0], server: parts[1] };
        localStorage.setItem('jboss_domain_ctx', JSON.stringify(__domainCtx));
    }
    location.reload();
}

function _setModeBadge(mode) {
    var badge = document.getElementById('modeBadge');
    if (!badge) return;
    badge.style.display = 'inline-block';
    if (mode === 'DOMAIN') {
        badge.textContent = 'DOMAIN';
        badge.style.background = 'rgba(59,130,246,0.15)';
        badge.style.color = '#60a5fa';
        badge.style.border = '1px solid rgba(59,130,246,0.3)';
    } else {
        badge.textContent = 'STANDALONE';
        badge.style.background = 'rgba(34,197,94,0.15)';
        badge.style.color = '#4ade80';
        badge.style.border = '1px solid rgba(34,197,94,0.3)';
    }
}

function _initDomainSelector() {
    fetch(MCP_API + '/domain/topology')
        .then(function(r) { return r.json(); })
        .then(function(data) {
            if (data.error || data.mode !== 'DOMAIN') {
                __isDomainMode = false;
                _setModeBadge('STANDALONE');
                if (__domainCtx) {
                    __domainCtx = null;
                    localStorage.removeItem('jboss_domain_ctx');
                    location.reload();
                }
                return;
            }
            __isDomainMode = true;
            _setModeBadge('DOMAIN');
            var hosts = data.hosts || [];
            var servers = [];
            hosts.forEach(function(h) {
                (h.servers || []).forEach(function(s) {
                    servers.push({
                        host: h.name,
                        server: s.name,
                        group: s.group || '',
                        status: s.status || s.server_state || 'STOPPED',
                        label: h.name + ' / ' + s.name
                    });
                });
            });
            __domainServers = servers;

            var sel = document.getElementById('serverSelect');
            var container = document.getElementById('domainServerSelector');
            if (!sel || !container) return;

            container.style.display = 'block';
            var currentVal = __domainCtx ? __domainCtx.host + '/' + __domainCtx.server : '';
            var html = '<option value=""' + (!currentVal ? ' selected' : '') + '>All Servers</option>';

            var hostMap = {};
            servers.forEach(function(s) {
                if (!hostMap[s.host]) hostMap[s.host] = [];
                hostMap[s.host].push(s);
            });

            Object.keys(hostMap).forEach(function(hostName) {
                html += '<optgroup label="' + hostName + '">';
                hostMap[hostName].forEach(function(s) {
                    var val = s.host + '/' + s.server;
                    var selected = val === currentVal ? ' selected' : '';
                    var statusIcon = s.status.toUpperCase() === 'STARTED' ? '● ' : '○ ';
                    html += '<option value="' + val + '"' + selected + '>'
                        + statusIcon + s.server
                        + (s.group ? ' (' + s.group + ')' : '')
                        + '</option>';
                });
                html += '</optgroup>';
            });

            sel.innerHTML = html;
        })
        .catch(function() {
            __isDomainMode = false;
            _setModeBadge('STANDALONE');
        });
}

var TABLE_PAGE_SIZE = 20;

function paginateTable(tbodyId, rows, pageSize) {
    pageSize = pageSize || TABLE_PAGE_SIZE;
    var tbody = document.getElementById(tbodyId);
    if (!tbody) return;
    var btnId = tbodyId + '_paginate';
    var existing = document.getElementById(btnId);
    if (existing) existing.remove();

    if (rows.length <= pageSize) {
        tbody.innerHTML = rows.join('');
        return;
    }

    tbody._allRows = rows;
    tbody._pageSize = pageSize;
    tbody._visibleCount = pageSize;
    tbody.innerHTML = rows.slice(0, pageSize).join('');

    var wrapper = document.createElement('div');
    wrapper.id = btnId;
    wrapper.style.cssText = 'text-align:center;padding:12px 0;display:flex;gap:10px;justify-content:center;';

    var btnMore = document.createElement('button');
    btnMore.className = 'btn';
    btnMore.style.cssText = 'font-size:12px;padding:6px 20px;';

    var btnLess = document.createElement('button');
    btnLess.className = 'btn';
    btnLess.style.cssText = 'font-size:12px;padding:6px 20px;display:none;';

    function updateButtons() {
        var remaining = tbody._allRows.length - tbody._visibleCount;
        if (remaining > 0) {
            btnMore.textContent = 'Show More (' + remaining + ' remaining)';
            btnMore.style.display = '';
        } else {
            btnMore.style.display = 'none';
        }
        if (tbody._visibleCount > tbody._pageSize) {
            btnLess.textContent = 'Show Less';
            btnLess.style.display = '';
        } else {
            btnLess.style.display = 'none';
        }
    }

    btnMore.onclick = function() {
        tbody._visibleCount = Math.min(tbody._visibleCount + tbody._pageSize, tbody._allRows.length);
        tbody.innerHTML = tbody._allRows.slice(0, tbody._visibleCount).join('');
        updateButtons();
    };
    btnLess.onclick = function() {
        tbody._visibleCount = Math.max(tbody._visibleCount - tbody._pageSize, tbody._pageSize);
        tbody.innerHTML = tbody._allRows.slice(0, tbody._visibleCount).join('');
        updateButtons();
    };

    updateButtons();
    wrapper.appendChild(btnMore);
    wrapper.appendChild(btnLess);

    var insertAfter = tbody.closest('.table-scroll') || tbody.closest('table');
    if (insertAfter && insertAfter.parentElement) {
        insertAfter.parentElement.insertBefore(wrapper, insertAfter.nextSibling);
    } else {
        (tbody.closest('.card-body') || tbody.parentElement).appendChild(wrapper);
    }
}

function toggleDarkMode(isDark) {
    if (isDark) {
        document.body.classList.remove('light-mode');
        localStorage.setItem('jboss_theme', 'dark');
    } else {
        document.body.classList.add('light-mode');
        localStorage.setItem('jboss_theme', 'light');
    }
}

function _initTheme() {
    var saved = localStorage.getItem('jboss_theme');
    if (saved === 'light') {
        document.body.classList.add('light-mode');
        var toggle = document.getElementById('darkModeToggle');
        if (toggle) toggle.checked = false;
    }
}

// Reflect the live JBoss connection state in the sidebar footer (replaces "Initializing...").
function _updateGlobalStatus() {
    var textEl = document.getElementById('globalStatusText');
    var dotEl = document.getElementById('globalStatusDot');
    if (!textEl || !dotEl) return;
    fetch(_appendServerCtx(MCP_API + '/server-status'))
        .then(function(r) { return r.json(); })
        .then(function(data) {
            if (data && !data.error) {
                textEl.textContent = 'Connected';
                var product = (data.product_name && data.product_name !== 'unknown')
                    ? data.product_name : 'JBoss EAP';
                var version = data.product_version && data.product_version !== 'unknown'
                    ? data.product_version : (data.release_version || '');
                var state = data.server_state || data.running_mode || 'running';
                textEl.title = (product + ' ' + version).trim() + ' — ' + state;
                dotEl.className = 'status-dot';
            } else {
                textEl.textContent = 'Disconnected';
                textEl.title = data && data.error ? String(data.error) : 'Cannot reach JBoss';
                dotEl.className = 'status-dot critical';
            }
        })
        .catch(function() {
            textEl.textContent = 'Disconnected';
            textEl.title = 'Cannot reach JBoss';
            dotEl.className = 'status-dot critical';
        });
}

document.addEventListener('DOMContentLoaded', () => {
    setActiveNav();
    _initDomainSelector();
    _initTheme();
    _updateGlobalStatus();
});
