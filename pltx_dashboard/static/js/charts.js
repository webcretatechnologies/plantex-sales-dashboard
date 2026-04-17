/* ══════════════════════════════════════════════════════════════
   Plantex Sales Dashboard — Frontend Rendering Layer
   All calculations and table rendering are done via SSR (Django).
   This file handles Chart.js rendering, modals, and lightweight UI.
   ══════════════════════════════════════════════════════════════ */

var charts = {};

function fmtNum(n) { return new Intl.NumberFormat('en-IN').format(n); }
function fmtShort(n) {
    if (n >= 10000000) return '₹' + (n / 10000000).toFixed(2) + ' Cr';
    if (n >= 100000)   return '₹' + (n / 100000).toFixed(1) + ' L';
    if (n >= 1000)     return '₹' + (n / 1000).toFixed(0) + ' K';
    return '₹' + n.toFixed(0);
}
function fmtDateLabel(d) {
    var p = d.split('-');
    if (p.length === 3) {
        var m = ['', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
        return parseInt(p[2]) + ' ' + m[parseInt(p[1])];
    }
    return d;
}

/* ── Chart Defaults ── */
Chart.defaults.font.family = "'Inter', -apple-system, sans-serif";
Chart.defaults.font.size = 11;
Chart.defaults.color = '#94a3b8';

function cOpts(extra) {
    var base = {
        responsive: true, maintainAspectRatio: false,
        plugins: {
            legend: { display: false },
            tooltip: {
                backgroundColor: '#0f172a',
                titleColor: '#f0f0ff',
                bodyColor: '#94a3b8',
                borderColor: '#1e293b',
                borderWidth: 1,
                cornerRadius: 8,
                padding: 10,
                titleFont: { weight: 600 },
                bodyFont: { size: 11 }
            }
        },
        scales: {
            x: {
                grid: { display: false },
                ticks: { font: { size: 10 }, maxRotation: 0 },
                border: { display: false }
            },
            y: {
                grid: { color: 'rgba(148,163,184,0.08)', drawBorder: false },
                ticks: {
                    font: { size: 10 },
                    callback: function (v) { return fmtShort(v); }
                },
                border: { display: false }
            }
        }
    };
    if (extra) {
        for (var k in extra) {
            if (k === 'plugins' || k === 'scales') {
                for (var sk in extra[k]) {
                    base[k][sk] = Object.assign(base[k][sk] || {}, extra[k][sk]);
                }
            } else {
                base[k] = extra[k];
            }
        }
    }
    return base;
}

function destroyChart(id) { if (charts[id]) { charts[id].destroy(); delete charts[id]; } }

/** Destroy every known chart instance to prevent canvas-reuse errors */
function destroyAllCharts() {
    var allIds = [
        'salesTrendChart', 'platformSplitChart', 'forecastChart',
        'bizProfitChart', 'bizHealthGauge'
    ];
    allIds.forEach(function(id) { destroyChart(id); });
    // Also destroy any leftover keys
    Object.keys(charts).forEach(function(id) { destroyChart(id); });
}

/* ── Modal Logic ── */
function openModal(id) {
    var el = document.getElementById(id);
    if (el) { el.classList.add('active'); document.body.style.overflow = 'hidden'; }
}
function closeModal(id) {
    var el = document.getElementById(id);
    if (el) { el.classList.remove('active'); document.body.style.overflow = ''; }
}
// Close on clicking overlay background
document.addEventListener('click', function (e) {
    if (e.target.classList.contains('tbl-overlay') && e.target.classList.contains('active')) {
        e.target.classList.remove('active');
        document.body.style.overflow = '';
    }
});
// Close on Escape key
document.addEventListener('keydown', function (e) {
    if (e.key === 'Escape') {
        document.querySelectorAll('.tbl-overlay.active').forEach(function (el) {
            el.classList.remove('active');
        });
        document.body.style.overflow = '';
    }
});

/* ══════════════════════════════════════════════════════════════
   INIT CHARTS
   ══════════════════════════════════════════════════════════════ */
function initCharts() {
    var data = window.DASHBOARD_PAYLOAD;
    if (!data) return;

    // Always destroy all existing charts first to prevent canvas-reuse errors
    destroyAllCharts();

    var viewType = window.DASHBOARD_VIEW_TYPE;

    /* ── Business Dashboard Charts ── */
    if (viewType === 'business') {
        initCEOCharts(data);

        // Profitability Overview Bar Chart
        if (data.waterfall && data.waterfall.length > 0 && document.getElementById('bizProfitChart')) {
            var wLabels = data.waterfall.map(function (w) { return w.label; });
            var wVals   = data.waterfall.map(function (w) { return w.value; });
            var wColors = data.waterfall.map(function (w) {
                if (w.label === 'Revenue')      return '#0891b2';
                if (w.label === 'Net Profit')   return '#8b5cf6';
                if (w.label === 'Gross Profit') return '#10b981';
                return w.value > 0 ? '#10b981' : '#ef4444';
            });
            charts.bizProfitChart = new Chart(document.getElementById('bizProfitChart'), {
                type: 'bar',
                data: {
                    labels: wLabels,
                    datasets: [{ data: wVals, backgroundColor: wColors, borderRadius: 6, maxBarThickness: 48 }]
                },
                options: cOpts({
                    plugins: {
                        legend: { display: false },
                        tooltip: { callbacks: { label: function (ctx) {
                            var v = ctx.raw; return (v < 0 ? '-' : '') + fmtShort(Math.abs(v));
                        }}}
                    },
                    scales: {
                        x: { grid: { display: false }, ticks: { font: { size: 10, weight: 500 }, maxRotation: 0 }, border: { display: false } },
                        y: { grid: { color: 'rgba(148,163,184,0.08)' }, ticks: { font: { size: 10 }, callback: function (v) { return fmtShort(Math.abs(v)); } }, border: { display: false } }
                    }
                })
            });
        }

        // Business Health Score Gauge (Doughnut)
        if (document.getElementById('bizHealthGauge')) {
            var score = data.business_health ? data.business_health.score : 0;
            var remaining = 100 - score;
            var gaugeColor = score >= 80 ? '#10b981' : score >= 60 ? '#f59e0b' : '#ef4444';
            charts.bizHealthGauge = new Chart(document.getElementById('bizHealthGauge'), {
                type: 'doughnut',
                data: { datasets: [{ data: [score, remaining], backgroundColor: [gaugeColor, 'rgba(148,163,184,0.1)'], borderWidth: 0, circumference: 270, rotation: 225 }] },
                options: { responsive: true, maintainAspectRatio: true, cutout: '78%', plugins: { legend: { display: false }, tooltip: { enabled: false } } }
            });
        }
    }

    /* ── CEO Dashboard Charts ── */
    if (viewType === 'ceo') {
        initCEOCharts(data);
    }

    /* ── Category Dashboard Charts ── */
    if (viewType === 'category') {
        initCEOCharts(data);
    }
}

/* ══════════════════════════════════════════════════════════════
   CEO / SHARED DASHBOARD CHARTS
   ══════════════════════════════════════════════════════════════ */
function initCEOCharts(data) {
    if (data.charts && data.charts.trend && document.getElementById('salesTrendChart')) {
        renderSalesTrend(data, 'daily');
    }
    if (data.platforms && document.getElementById('platformSplitChart')) {
        renderPlatformDonut(data);
    }
    if (data.forecast && document.getElementById('forecastChart')) {
        renderForecastChart(data);
    }
    initTrendTabs(data);
}

/* ── Aggregate helpers ── */
function aggregateData(labels, current, prev, chunkSize) {
    var newLabels = [], newCurrent = [], newPrev = [];
    for (var i = 0; i < labels.length; i += chunkSize) {
        var end = Math.min(i + chunkSize, labels.length);
        newLabels.push(labels[i] + ' - ' + labels[end - 1]);
        var sumC = 0, sumP = 0;
        for (var j = i; j < end; j++) { sumC += (current[j] || 0); sumP += (prev[j] || 0); }
        newCurrent.push(sumC); newPrev.push(sumP);
    }
    return { labels: newLabels, current: newCurrent, prev: newPrev };
}

function aggregateMultiData(labels, rawSets, chunkSize) {
    var newLabels = [];
    var newSets = {};
    var keys = Object.keys(rawSets);
    keys.forEach(function (k) { newSets[k] = []; });
    for (var i = 0; i < labels.length; i += chunkSize) {
        var end = Math.min(i + chunkSize, labels.length);
        newLabels.push(labels[i] + ' - ' + labels[end - 1]);
        keys.forEach(function (k) {
            var sum = 0;
            for (var j = i; j < end; j++) { sum += (rawSets[k][j] || 0); }
            newSets[k].push(sum);
        });
    }
    return Object.assign({ labels: newLabels }, newSets);
}

/* ── Sales Trend Chart ── */
function renderSalesTrend(data, period) {
    destroyChart('salesTrendChart');
    var trend = data.charts.trend;
    var labels = trend.labels.map(fmtDateLabel);

    var platformEl = document.getElementById('platformSelect');
    var platform   = platformEl ? platformEl.value : '';  // '' = All
    var dateEl     = document.getElementById('dateRangeSelect');
    var isFiltered = !!(dateEl && dateEl.value);
    var prevLabel  = isFiltered ? 'Selected Date Range' : 'Last Period';

    // Check if per-platform data is meaningful (all-zeros means only one platform active)
    var hasAmazon   = trend.amazon_revenue   && trend.amazon_revenue.some(function(v)   { return v > 0; });
    var hasFlipkart = trend.flipkart_revenue && trend.flipkart_revenue.some(function(v) { return v > 0; });
    var showDualLines = (!platform || platform === '') && hasAmazon && hasFlipkart;

    var rawSets = { 'current': trend.revenue };
    if (showDualLines) {
        rawSets['amazon']   = trend.amazon_revenue;
        rawSets['flipkart'] = trend.flipkart_revenue;
    }
    if (trend.prev_revenue && trend.prev_revenue.length > 0) {
        rawSets['prev'] = trend.prev_revenue;
    }

    var processed = Object.assign({ labels: labels }, rawSets);
    if (period === 'weekly')       processed = aggregateMultiData(labels, rawSets, 7);
    else if (period === 'monthly') processed = aggregateMultiData(labels, rawSets, 30);

    var datasets = [];

    if (platform === 'Amazon') {
        datasets.push({ label: 'Amazon Revenue', data: processed.current, borderColor: '#3b82f6', backgroundColor: 'rgba(59,130,246,0.08)', fill: true, tension: 0.4, borderWidth: 2.5, pointRadius: 0 });
    } else if (platform === 'Flipkart') {
        datasets.push({ label: 'Flipkart Revenue', data: processed.current, borderColor: '#f59e0b', backgroundColor: 'rgba(245,158,11,0.08)', fill: true, tension: 0.4, borderWidth: 2.5, pointRadius: 0 });
    } else if (showDualLines) {
        // "All" with both platforms — show two separate lines
        datasets.push({ label: 'Amazon',   data: processed.amazon,   borderColor: '#3b82f6', backgroundColor: 'rgba(59,130,246,0.05)', fill: true, tension: 0.4, borderWidth: 2, pointRadius: 0 });
        datasets.push({ label: 'Flipkart', data: processed.flipkart, borderColor: '#f59e0b', backgroundColor: 'rgba(245,158,11,0.05)', fill: true, tension: 0.4, borderWidth: 2, pointRadius: 0 });
    } else {
        // "All" but only one platform has data — single combined line
        var cBorder = '#0891b2';
        var cBg     = 'rgba(8,145,178,0.08)';
        var cLabel  = 'Total Revenue';

        if (hasFlipkart && !hasAmazon) {
            cBorder = '#f59e0b'; // Flipkart Orange
            cBg     = 'rgba(245,158,11,0.08)';
            cLabel  = 'Flipkart Revenue';
        } else if (hasAmazon && !hasFlipkart) {
            cBorder = '#3b82f6'; // Amazon Blue
            cBg     = 'rgba(59,130,246,0.08)';
            cLabel  = 'Amazon Revenue';
        }

        datasets.push({ label: cLabel, data: processed.current, borderColor: cBorder, backgroundColor: cBg, fill: true, tension: 0.4, borderWidth: 2.5, pointRadius: 0 });
    }

    if (processed.prev && processed.prev.length > 0) {
        datasets.push({ label: prevLabel, data: processed.prev, borderColor: '#94a3b8', borderDash: [5, 5], fill: false, tension: 0.4, borderWidth: 1.5, pointRadius: 0 });
    }

    // Show legend when multiple datasets are displayed
    var showLegend = datasets.length > 1;

    charts.salesTrendChart = new Chart(document.getElementById('salesTrendChart'), {
        type: 'line',
        data: { labels: processed.labels, datasets: datasets },
        options: cOpts({
            plugins: {
                legend: { display: showLegend, position: 'top', labels: { boxWidth: 12, font: { size: 11 } } },
                tooltip: { mode: 'index', intersect: false }
            },
            interaction: { mode: 'index', intersect: false }
        })
    });
}

function initTrendTabs(data) {
    var tabs = document.querySelectorAll('.ceo-trend-tab');
    tabs.forEach(function (tab) {
        tab.addEventListener('click', function () {
            tabs.forEach(function (t) { t.classList.remove('active'); });
            tab.classList.add('active');
            renderSalesTrend(data, tab.getAttribute('data-period'));
        });
    });
}

/* ── Platform Split Donut ── */
function renderPlatformDonut(data) {
    var el = document.getElementById('platformSplitChart');
    if (!el) return;
    destroyChart('platformSplitChart');
    var pLabels = Object.keys(data.platforms);
    if (!pLabels.length) return;
    var pVals   = pLabels.map(function (k) { return data.platforms[k].revenue; });
    var colors  = pLabels.map(function (name) {
        return name === 'Amazon' ? '#3b82f6' : (name === 'Flipkart' ? '#f59e0b' : '#10b981');
    });
    charts.platformSplitChart = new Chart(el, {
        type: 'doughnut',
        data: { labels: pLabels, datasets: [{ data: pVals, backgroundColor: colors, borderWidth: 0 }] },
        options: {
            responsive: true, maintainAspectRatio: false, cutout: '72%',
            plugins: {
                legend: { display: false },
                tooltip: { callbacks: { label: function (ctx) { return ctx.label + ': ' + fmtShort(ctx.raw) + ' (' + data.platforms[ctx.label].pct + '%)'; } } }
            }
        }
    });
}

/* ── Revenue Forecast Chart ── */
function renderForecastChart(data) {
    var el = document.getElementById('forecastChart');
    if (!el) return;
    destroyChart('forecastChart');
    var fc = data.forecast;

    // Validate required array fields exist
    if (!fc || !fc.labels || !fc.labels.length) return;

    charts.forecastChart = new Chart(el, {
        type: 'line',
        data: {
            labels: fc.labels,
            datasets: [
                {
                    label: 'Actual',
                    data: fc.actual,
                    borderColor: '#0891b2',
                    backgroundColor: 'rgba(8,145,178,0.08)',
                    fill: true,
                    tension: 0.3,
                    borderWidth: 2.5,
                    pointRadius: 0,
                    spanGaps: false
                },
                {
                    label: 'Forecast',
                    data: fc.forecast,
                    borderColor: '#10b981',
                    backgroundColor: 'rgba(16,185,129,0.06)',
                    borderDash: [5, 5],
                    fill: true,
                    tension: 0.3,
                    borderWidth: 2,
                    pointRadius: 0,
                    spanGaps: false
                },
                {
                    label: 'Target',
                    data: fc.target_line,
                    borderColor: '#94a3b8',
                    borderDash: [3, 3],
                    fill: false,
                    tension: 0,
                    borderWidth: 1.5,
                    pointRadius: 0,
                    spanGaps: true
                }
            ]
        },
        options: cOpts({
            plugins: {
                legend: {
                    display: true, position: 'top',
                    labels: { boxWidth: 12, font: { size: 10 } }
                }
            }
        })
    });
}

/* ══════════════════════════════════════════════════════════════
   INIT — on page load AND after every HTMX navigation
   ══════════════════════════════════════════════════════════════ */
window.addEventListener('DOMContentLoaded', function () {
    initCharts();
    syncFilterVisibility();
});

// Re-initialize charts after HTMX content swap (base_htmx.html dispatches this event)
document.addEventListener('dashboardContentLoaded', function () {
    setTimeout(function () {
        initCharts();
    }, 50);
});
