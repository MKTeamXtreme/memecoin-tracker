/* ── app.js — Memecoin Tracker Frontend ──────────────────────────────────── */

const WS_URL   = `ws://${location.host}/ws`;
const API_BASE = `http://${location.host}/api`;

// ── State ─────────────────────────────────────────────────────────────────
let feedCoins = [];      // coins shown in sidebar
let allCoins  = [];      // full history
let stats     = {};
let winChart  = null;
let ws        = null;
const MAX_FEED = 60;     // max cards in live feed
let currentTab = 'history';
let sidebarSearchQuery = '';
let winnerCoins = [];

function switchTab(tab) {
  currentTab = tab;
  document.getElementById('tab-history').classList.toggle('active', tab === 'history');
  document.getElementById('tab-winners').classList.toggle('active', tab === 'winners');
  
  if (tab === 'winners') {
    fetch(`${API_BASE}/biggest_winners`)
      .then(r => r.json())
      .then(coins => {
        winnerCoins = coins;
        renderTable(winnerCoins);
      })
      .catch(() => renderTable(allCoins));
  } else {
    renderTable(allCoins);
  }
}

function handleFeedSearch() {
  sidebarSearchQuery = document.getElementById('feed-search').value.toLowerCase();
  renderFeed();
}

// ── Copy contract address to clipboard ───────────────────────────────────────
function copyAddr(addr, el) {
  const hint = el.querySelector ? el.querySelector('.addr-hint') : el;
  const doConfirm = () => {
    if (hint) { hint.textContent = '✅ copied!'; hint.style.color = 'var(--success)'; hint.style.opacity = '1'; }
    setTimeout(() => { if (hint) { hint.textContent = 'click to copy'; hint.style.color = ''; hint.style.opacity = ''; } }, 2000);
  };
  navigator.clipboard.writeText(addr).then(doConfirm).catch(() => {
    const t = document.createElement('textarea');
    t.value = addr; document.body.appendChild(t); t.select();
    document.execCommand('copy'); document.body.removeChild(t);
    doConfirm();
  });
}


// ── Desktop Notifications ──────────────────────────────────────────────────
let notificationsEnabled = false;

function toggleNotifications() {
  if (!notificationsEnabled) {
    Notification.requestPermission().then(perm => {
      if (perm === 'granted') {
        notificationsEnabled = true;
        document.getElementById('notif-btn').textContent = '🔔 Alerts On';
        document.getElementById('notif-btn').style.borderColor = 'var(--primary)';
        showToast('Notifications Enabled', 'You will receive desktop alerts for new coins.', 'success');
      } else {
        showToast('Permission Denied', 'Allow notifications in your browser settings.', 'error');
      }
    });
  } else {
    notificationsEnabled = false;
    document.getElementById('notif-btn').textContent = '🔕 Alerts Off';
    document.getElementById('notif-btn').style.borderColor = 'var(--border-color)';
  }
}

function notifyUser(coin) {
  if (!notificationsEnabled) return;
  new Notification(`New Coin: ${coin.name} ($${coin.symbol})`, {
    body: `Score: ${coin.score} | MC: ${fmtMC(coin.initial_mc)}`,
    icon: coin.image_uri || ''
  });
}

// ── Helpers ───────────────────────────────────────────────────────────────
function fmtMC(val) {
  if (val == null || val === 0) return '—';
  if (val >= 1_000_000) return `$${(val/1_000_000).toFixed(1)}M`;
  if (val >= 1_000)     return `$${(val/1_000).toFixed(1)}k`;
  return `$${Math.round(val)}`;
}

function fmtMult(initial, peak) {
  if (!initial || !peak || initial === 0) return null;
  return (peak / initial).toFixed(1);
}

function timeAgo(ts) {
  const d = Date.now()/1000 - ts;
  if (d <  60)  return `${Math.round(d)}s ago`;
  if (d < 3600) return `${Math.floor(d/60)}m ago`;
  return `${Math.floor(d/3600)}h ago`;
}

function scoreClass(score) {
  if (score >= 65) return 'high';
  if (score >= 45) return 'mid';
  return 'low';
}

function getStatusBadge(coin) {
  const mult = fmtMult(coin.initial_mc, coin.peak_mc);
  if (!mult) return `<span class="mult-badge pend">⏳ Pending</span>`;
  const m = parseFloat(mult);
  if (m >= 3)  return `<span class="mult-badge win3">🚀 ${mult}x</span>`;
  if (m >= 2)  return `<span class="mult-badge win2">✅ ${mult}x</span>`;
  if (m >= 1.3) return `<span class="mult-badge flat">➡ ${mult}x</span>`;
  return `<span class="mult-badge loss">💀 ${mult}x</span>`;
}

// ── Toast ─────────────────────────────────────────────────────────────────
function showToast(title, sub, type = '') {
  const container = document.getElementById('toast-container');
  const el = document.createElement('div');
  el.className = `toast ${type}`;
  el.innerHTML = `<div class="toast-title">${title}</div><div class="toast-sub">${sub}</div>`;
  container.appendChild(el);
  setTimeout(() => {
    el.style.animation = 'toast-out .3s ease forwards';
    setTimeout(() => el.remove(), 300);
  }, 4000);
}

// ── Coin Card Builder ─────────────────────────────────────────────────────
function buildCoinCard(coin) {
  const sc   = scoreClass(coin.score);
  const img  = coin.image_uri || '';
  const sym  = (coin.symbol || '??').substring(0, 3).toUpperCase();
  const mc   = fmtMC(coin.initial_mc);
  const time = timeAgo(coin.detected_at);
  const ax   = `https://axiom.trade/meme/${coin.mint}?chain=sol`;
  const dex  = `https://dexscreener.com/solana/${coin.mint}`;
  const gmgn = `https://gmgn.ai/sol/token/${coin.mint}`;
  const pump = `https://pump.fun/${coin.mint}`;
  const shortAddr = coin.mint ? coin.mint.substring(0, 6) + '...' + coin.mint.substring(coin.mint.length - 4) : '';
  const bd   = coin.score_breakdown || {};
  const flags = (bd.flags || []).slice(0, 2);

  const socials = [];
  if (coin.has_twitter)  socials.push(`<span class="meta-pill green">𝕏 Twitter</span>`);
  if (coin.has_telegram) socials.push(`<span class="meta-pill green">✈ Telegram</span>`);
  if (coin.reply_count > 0) socials.push(`<span class="meta-pill">💬 ${coin.reply_count}</span>`);
  socials.push(`<span class="meta-pill">${mc}</span>`);
  socials.push(`<span class="meta-pill">${time}</span>`);

  const flagHtml = flags.map(f => `<span class="flag-pill" title="${f}">⚠</span>`).join('');

  return `
    <div class="coin-card score-${sc}" data-mint="${coin.mint}">
      <div class="card-top">
        ${img
          ? `<img class="token-img" src="${img}" alt="${sym}" onerror="this.style.display='none';this.nextElementSibling.style.display='flex'">`
          : ''
        }
        <div class="token-img-fallback" style="${img ? 'display:none' : ''}">${sym[0]}</div>
        <div class="token-info">
          <div class="token-name">${coin.name || 'Unknown'} ${flagHtml}</div>
          <div class="token-symbol">$${coin.symbol || '???'}</div>
        </div>
        <div class="score-badge ${sc}">${coin.score}</div>
      </div>
      <div class="card-meta">${socials.join('')}</div>
      <div class="card-addr-bar" onclick="copyAddr('${coin.mint}', this)" title="Click to copy contract address">
        <span class="addr-icon">📋</span>
        <span class="addr-text">${coin.mint ? coin.mint.substring(0,8) + '...' + coin.mint.substring(coin.mint.length-6) : 'No address'}</span>
        <span class="addr-hint">click to copy</span>
      </div>
      <div class="card-actions">
        <a class="btn btn-primary" href="${pump}"  target="_blank" rel="noopener">Pump.fun ↗</a>
        <a class="btn btn-ghost"   href="https://jup.ag/swap/SOL-${coin.mint}" target="_blank" rel="noopener">Jupiter ↗</a>
        <a class="btn btn-ghost"   href="${ax}"    target="_blank" rel="noopener">Axiom ↗</a>
        <a class="btn btn-ghost"   href="${dex}"   target="_blank" rel="noopener">DEX ↗</a>
      </div>
    </div>
  `;
}

// ── Feed Update ───────────────────────────────────────────────────────────
function addToFeed(coin) {
  feedCoins.unshift(coin);
  if (feedCoins.length > MAX_FEED) feedCoins.pop();

  if (!sidebarSearchQuery) {
    renderFeed();
  }
}

function renderFeed() {
  const feed = document.getElementById('feed');
  feed.innerHTML = '';

  let displayCards = [];
  if (sidebarSearchQuery) {
    displayCards = allCoins.filter(c => 
      (c.name || '').toLowerCase().includes(sidebarSearchQuery) ||
      (c.symbol || '').toLowerCase().includes(sidebarSearchQuery) ||
      (c.mint || '').toLowerCase().includes(sidebarSearchQuery)
    ).slice(0, 60);
  } else {
    displayCards = feedCoins;
  }

  if (displayCards.length === 0) {
    feed.innerHTML = `<div class="empty-state" id="feed-empty"><div class="icon">🔍</div><p>${sidebarSearchQuery ? 'No matching coins found' : 'Scanning Pump.fun for new tokens...<br/>Coins scoring 35+ will appear here.'}</p></div>`;
  } else {
    displayCards.forEach(c => {
      const el = document.createElement('div');
      el.innerHTML = buildCoinCard(c);
      feed.appendChild(el.firstElementChild);
    });
  }
  document.getElementById('feed-count').textContent = `${displayCards.length} coins`;
}

// ── History Table ─────────────────────────────────────────────────────────
function renderTable(coins) {
  let displayCoins = [...(coins || [])];

  if (currentTab === 'winners') {
    displayCoins = displayCoins.filter(c => c.peak_mc && c.initial_mc && c.initial_mc > 0);
    displayCoins.sort((a, b) => (b.peak_mc / b.initial_mc) - (a.peak_mc / a.initial_mc));
    displayCoins = displayCoins.slice(0, 50);
  } else {
    displayCoins = displayCoins.slice(0, 100);
  }

  const tbody = document.getElementById('history-body');
  if (displayCoins.length === 0) {
    tbody.innerHTML = `<tr><td colspan="11" style="text-align:center;padding:24px;color:var(--text-muted)">Waiting for first coin detections…</td></tr>`;
    return;
  }

  tbody.innerHTML = displayCoins.map(c => {
    const sc = scoreClass(c.score);
    const mult = fmtMult(c.initial_mc, c.peak_mc);
    const time = new Date(c.detected_at * 1000).toLocaleTimeString([], {hour:'2-digit', minute:'2-digit'});
    return `
      <tr>
        <td>${time}</td>
        <td class="name-cell">
          <span class="status-dot" style="background:${sc==='high'?'var(--success)':sc==='mid'?'var(--warn)':'var(--danger)'}"></span>
          ${c.name || '?'} <span style="color:var(--text-muted)">$${c.symbol||'?'}</span>
        </td>
        <td><span class="score-badge ${sc}" style="font-size:10px;padding:2px 8px">${c.score}</span></td>
        <td>${fmtMC(c.initial_mc)}</td>
        <td>${fmtMC(c.mc_1min)}</td>
        <td>${fmtMC(c.mc_5min)}</td>
        <td>${fmtMC(c.mc_15min)}</td>
        <td>${fmtMC(c.mc_1hr)}</td>
        <td>${fmtMC(c.peak_mc)}</td>
        <td style="font-family:var(--mono);font-weight:600;color:${mult ? (parseFloat(mult)>=2?'var(--success)':'var(--text-muted)') : 'var(--warn)'}">
          ${mult ? mult+'x' : '⏳'}
        </td>
        <td>${getStatusBadge(c)}</td>
      </tr>
    `;
  }).join('');
}

// ── Stats & Chart ─────────────────────────────────────────────────────────
function renderStats(s) {
  if (!s) return;
  stats = s;

  document.getElementById('h-total').textContent  = s.total || 0;
  document.getElementById('h-today').textContent  = s.today || 0;
  document.getElementById('s-with-outcomes').textContent = s.brackets?.reduce((a,b) => a + (b.total||0), 0) || 0;

  const perf = s.brackets?.[0];
  const high = s.brackets?.[1];
  const mid  = s.brackets?.[2];
  const low  = s.brackets?.[3];

  if (perf) {
    const pct = perf.total > 0 ? Math.round((perf.wins_2x / perf.total) * 100) : 0;
    document.getElementById('s-high-win').textContent  = perf.total > 0 ? `${pct}%` : '—';
    document.getElementById('s-avg-mult').textContent  = perf.avg_mult ? `${perf.avg_mult}x` : '—';
    document.getElementById('h-winrate').textContent   = perf.total > 0 ? `${pct}%` : '—';
    document.getElementById('bar-perf').style.width    = `${pct}%`;
    document.getElementById('bs-perf').textContent     = perf.total > 0 ? `${pct}% win — ${perf.total} coins` : 'No data yet';
  }
  if (high) {
    const pct = high.total > 0 ? Math.round((high.wins_2x / high.total) * 100) : 0;
    document.getElementById('bar-high').style.width    = `${pct}%`;
    document.getElementById('bs-high').textContent     = high.total > 0 ? `${pct}% win — ${high.total} coins` : 'No data yet';
  }
  if (mid) {
    const pct = mid.total > 0 ? Math.round((mid.wins_2x / mid.total) * 100) : 0;
    document.getElementById('bar-mid').style.width  = `${pct}%`;
    document.getElementById('bs-mid').textContent   = mid.total > 0 ? `${pct}% win — ${mid.total} coins` : 'No data yet';
  }
  if (low) {
    const pct = low.total > 0 ? Math.round((low.wins_2x / low.total) * 100) : 0;
    document.getElementById('bar-low').style.width  = `${pct}%`;
    document.getElementById('bs-low').textContent   = low.total > 0 ? `${pct}% win — ${low.total} coins` : 'No data yet';
  }

  // Update chart
  updateChart(s.brackets || []);
}

function initChart() {
  const ctx = document.getElementById('win-chart').getContext('2d');
  winChart = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: ['100', '70–99', '50–69', '0–49'],
      datasets: [{
        label: '2x Win %',
        data: [0, 0, 0, 0],
        backgroundColor: [
          'rgba(168,85,247,.7)',
          'rgba(16,185,129,.7)',
          'rgba(245,158,11,.7)',
          'rgba(244,63,94,.7)',
        ],
        borderColor: ['#a855f7', '#10b981', '#f59e0b', '#f43f5e'],
        borderWidth: 1,
        borderRadius: 4,
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: ctx => `${ctx.raw}% chance of 2x`
          }
        }
      },
      scales: {
        x: { grid: { color: 'rgba(255,255,255,.04)' }, ticks: { color: '#64748b', font: { size: 10 } } },
        y: {
          grid: { color: 'rgba(255,255,255,.04)' },
          ticks: { color: '#64748b', font: { size: 10 }, callback: v => `${v}%` },
          min: 0, max: 100,
        }
      }
    }
  });
}

function updateChart(brackets) {
  if (!winChart) return;
  const data = brackets.map(b => b.total > 0 ? Math.round((b.wins_2x / b.total) * 100) : 0);
  winChart.data.datasets[0].data = data;
  winChart.update('active');
}

// ── WebSocket ─────────────────────────────────────────────────────────────
function connectWS() {
  ws = new WebSocket(WS_URL);

  ws.onopen = () => {
    setConn(true);
    console.log('[WS] Connected');
  };

  ws.onmessage = (evt) => {
    try {
      const msg = JSON.parse(evt.data);

      if (msg.type === 'initial') {
        allCoins = msg.coins || [];
        renderTable(allCoins);
        renderStats(msg.stats);
        // Show recent coins in feed on startup (newest first, up to 50)
        const toShow = [...allCoins].slice(0, 50);
        toShow.reverse().forEach(c => addToFeed(c));
      }

      else if (msg.type === 'new_coin') {
        const coin = msg.coin;
        allCoins.unshift(coin);
        addToFeed(coin);
        renderTable(allCoins);

        const sc = scoreClass(coin.score);
        const emoji = sc === 'high' ? '🔥' : sc === 'mid' ? '⚡' : '📌';
        showToast(
          `${emoji} ${coin.name} ($${coin.symbol}) — Score ${coin.score}`,
          `MC: ${fmtMC(coin.initial_mc)} · ${coin.has_twitter?'𝕏 ':'' }${coin.has_telegram?'✈ ':''}${coin.reply_count} replies`,
          sc === 'high' ? 'success' : sc === 'mid' ? 'warn' : ''
        );
        
        notifyUser(coin);
      }

      else if (msg.type === 'stats_update') {
        renderStats(msg.stats);
        // Refresh table with updated outcome data
        fetch(`${API_BASE}/coins`)
          .then(r => r.json())
          .then(coins => { allCoins = coins; if (currentTab === 'history') renderTable(allCoins); })
          .catch(() => {});
      }

    } catch (e) {
      console.error('[WS] Parse error:', e);
    }
  };

  ws.onerror = () => setConn(false);
  ws.onclose = () => {
    setConn(false);
    setTimeout(connectWS, 3000);
  };
}

function setConn(online) {
  const badge = document.getElementById('conn-badge');
  const dot   = document.getElementById('conn-dot');
  const text  = document.getElementById('conn-text');
  const ldot  = document.getElementById('live-dot');

  badge.className = `conn-badge ${online ? 'connected' : 'disconnected'}`;
  dot.className   = `live-dot ${online ? '' : 'offline'}`;
  text.textContent = online ? 'Live' : 'Reconnecting…';
  ldot.className  = `live-dot ${online ? '' : 'offline'}`;
}

// ── Periodic table refresh ────────────────────────────────────────────────
function startTableRefresh() {
  setInterval(() => {
    fetch(`${API_BASE}/coins`)
      .then(r => r.json())
      .then(coins => { allCoins = coins; if (currentTab === 'history') renderTable(allCoins); })
      .catch(() => {});
  }, 60 * 1000); // every 60s refresh table with latest outcome data
}

// ── Init ──────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  initChart();
  connectWS();
  startTableRefresh();
});
