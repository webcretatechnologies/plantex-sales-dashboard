/* ══════════════════════════════════════════════════════════════
   Plantex Sales Dashboard — Frontend Rendering Layer
   All calculations and table rendering are done via SSR (Django).
   This file handles Chart.js rendering, modals, and lightweight UI.
   ══════════════════════════════════════════════════════════════ */

var charts = {};

function fmtNum(n) { return new Intl.NumberFormat('en-IN').format(n); }
function fmtShort(n) {
    if (n >= 10000000) return '₹' + (n / 10000000).toFixed(2) + ' Cr';
    if (n >= 100000) return '₹' + (n / 100000).toFixed(1) + ' L';
    if (n >= 1000) return '₹' + (n / 1000).toFixed(0) + ' K';
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
                    callback: function(v) { return fmtShort(v); }
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

/* ── Modal Logic ── */
function openModal(id) {
    var el = document.getElementById(id);
    if (el) {
        el.classList.add('active');
        document.body.style.overflow = 'hidden';
    }
}
function closeModal(id) {
    var el = document.getElementById(id);
    if (el) {
        el.classList.remove('active');
        document.body.style.overflow = '';
    }
}
// Close on clicking overlay background
document.addEventListener('click', function(e) {
    if (e.target.classList.contains('tbl-overlay') && e.target.classList.contains('active')) {
        e.target.classList.remove('active');
        document.body.style.overflow = '';
    }
});
// Close on Escape key
document.addEventListener('keydown', function(e) {
    if (e.key === 'Escape') {
        document.querySelectorAll('.tbl-overlay.active').forEach(function(el) {
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

    var viewType = window.DASHBOARD_VIEW_TYPE;

    /* ── Business Dashboard Charts ── */
    if (viewType === 'business') {
        // Reuse CEO chart logic for Sales Trend + Platform Donut
        initCEOCharts(data);

        // Profitability Overview Bar Chart
        if (data.waterfall && document.getElementById('bizProfitChart')) {
            destroyChart('bizProfitChart');
            var wLabels = data.waterfall.map(function(w) { return w.label; });
            var wVals = data.waterfall.map(function(w) { return w.value; });
            var wColors = data.waterfall.map(function(w) {
                if (w.label === 'Revenue') return '#0891b2';
                if (w.label === 'Net Profit') return '#8b5cf6';
                if (w.label === 'Gross Profit') return '#10b981';
                return w.value > 0 ? '#10b981' : '#ef4444';
            });
            charts.bizProfitChart = new Chart(document.getElementById('bizProfitChart'), {
                type: 'bar',
                data: {
                    labels: wLabels,
                    datasets: [{
                        data: wVals,
                        backgroundColor: wColors,
                        borderRadius: 6,
                        maxBarThickness: 48
                    }]
                },
                options: cOpts({
                    plugins: {
                        legend: { display: false },
                        tooltip: {
                            callbacks: {
                                label: function(ctx) {
                                    var v = ctx.raw;
                                    var prefix = v < 0 ? '-' : '';
                                    return prefix + fmtShort(Math.abs(v));
                                }
                            }
                        }
                    },
                    scales: {
                        x: {
                            grid: { display: false },
                            ticks: { font: { size: 10, weight: 500 }, maxRotation: 0 },
                            border: { display: false }
                        },
                        y: {
                            grid: { color: 'rgba(148,163,184,0.08)' },
                            ticks: {
                                font: { size: 10 },
                                callback: function(v) { return fmtShort(Math.abs(v)); }
                            },
                            border: { display: false }
                        }
                    }
                })
            });
        }

        // Business Health Score Gauge (Doughnut)
        if (document.getElementById('bizHealthGauge')) {
            destroyChart('bizHealthGauge');
            var score = data.business_health ? data.business_health.score : 0;
            var remaining = 100 - score;
            var gaugeColor = score >= 80 ? '#10b981' : score >= 60 ? '#f59e0b' : '#ef4444';

            charts.bizHealthGauge = new Chart(document.getElementById('bizHealthGauge'), {
                type: 'doughnut',
                data: {
                    datasets: [{
                        data: [score, remaining],
                        backgroundColor: [gaugeColor, 'rgba(148,163,184,0.1)'],
                        borderWidth: 0,
                        circumference: 270,
                        rotation: 225
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: true,
                    cutout: '78%',
                    plugins: {
                        legend: { display: false },
                        tooltip: { enabled: false }
                    }
                }
            });
        }
    }

    /* ── CEO Dashboard Charts ── */
    if (viewType === 'ceo') {
        initCEOCharts(data);
    }

    /* ── Category Dashboard Charts ── */
    if (viewType === 'category') {
        // Reuse CEO chart logic for Category Trend + Platform Donut
        initCEOCharts(data);
    }
}

/* ══════════════════════════════════════════════════════════════
   CEO DASHBOARD CHARTS
   ══════════════════════════════════════════════════════════════ */
function initCEOCharts(data) {
    // 1. Sales Trend Chart (dual line: this period + last period)
    if (data.charts && data.charts.trend && document.getElementById('salesTrendChart')) {
        renderSalesTrend(data, 'daily');
    }

    // 2. Platform Split Donut
    if (data.platforms && document.getElementById('platformSplitChart')) {
        renderPlatformDonut(data);
    }

    // 3. Revenue Forecast Chart
    if (data.forecast && document.getElementById('forecastChart')) {
        renderForecastChart(data);
    }

    // 4. Sales Trend filter tabs
    initTrendTabs(data);
}

/* ── Sales Trend ── */
function renderSalesTrend(data, period) {
    destroyChart('salesTrendChart');
    var trend = data.charts.trend;
    var labels = trend.labels.map(fmtDateLabel);
    var currentData = trend.revenue;
    var prevData = trend.prev_revenue || [];

    // For weekly/monthly, aggregate the data
    if (period === 'weekly') {
        var result = aggregateData(labels, currentData, prevData, 7);
        labels = result.labels;
        currentData = result.current;
        prevData = result.prev;
    } else if (period === 'monthly') {
        var result = aggregateData(labels, currentData, prevData, 30);
        labels = result.labels;
        currentData = result.current;
        prevData = result.prev;
    }

    charts.salesTrendChart = new Chart(document.getElementById('salesTrendChart'), {
        type: 'line',
        data: {
            labels: labels,
            datasets: [
                {
                    label: 'This Period',
                    data: currentData,
                    borderColor: '#0891b2',
                    backgroundColor: 'rgba(8,145,178,0.08)',
                    fill: true,
                    tension: 0.4,
                    borderWidth: 2.5,
                    pointRadius: 0,
                    pointHoverRadius: 5,
                    pointHoverBackgroundColor: '#0891b2',
                    pointHoverBorderColor: '#fff',
                    pointHoverBorderWidth: 2
                },
                {
                    label: 'Last Period',
                    data: prevData,
                    borderColor: '#94a3b8',
                    borderDash: [5, 5],
                    backgroundColor: 'transparent',
                    fill: false,
                    tension: 0.4,
                    borderWidth: 1.5,
                    pointRadius: 0,
                    pointHoverRadius: 4,
                    pointHoverBackgroundColor: '#94a3b8',
                    pointHoverBorderColor: '#fff',
                    pointHoverBorderWidth: 2
                }
            ]
        },
        options: cOpts({
            plugins: {
                tooltip: {
                    mode: 'index',
                    intersect: false,
                    callbacks: {
                        label: function(ctx) {
                            return ctx.dataset.label + ': ' + fmtShort(ctx.raw);
                        }
                    }
                }
            },
            interaction: { mode: 'index', intersect: false },
            hover: { mode: 'index', intersect: false }
        })
    });
}

function aggregateData(labels, current, prev, chunkSize) {
    var newLabels = [], newCurrent = [], newPrev = [];
    for (var i = 0; i < labels.length; i += chunkSize) {
        var end = Math.min(i + chunkSize, labels.length);
        newLabels.push(labels[i] + ' - ' + labels[end - 1]);
        var sumC = 0, sumP = 0;
        for (var j = i; j < end; j++) {
            sumC += (current[j] || 0);
            sumP += (prev[j] || 0);
        }
        newCurrent.push(sumC);
        newPrev.push(sumP);
    }
    return { labels: newLabels, current: newCurrent, prev: newPrev };
}

function initTrendTabs(data) {
    var tabs = document.querySelectorAll('.ceo-trend-tab');
    tabs.forEach(function(tab) {
        tab.addEventListener('click', function() {
            tabs.forEach(function(t) { t.classList.remove('active'); });
            tab.classList.add('active');
            var period = tab.getAttribute('data-period');
            renderSalesTrend(data, period);
        });
    });
}

/* ── Platform Donut ── */
function renderPlatformDonut(data) {
    destroyChart('platformSplitChart');
    var pLabels = Object.keys(data.platforms);
    var pVals = pLabels.map(function(k) { return data.platforms[k].revenue; });
    var colors = ['#f59e0b', '#3b82f6', '#10b981'];

    charts.platformSplitChart = new Chart(document.getElementById('platformSplitChart'), {
        type: 'doughnut',
        data: {
            labels: pLabels,
            datasets: [{
                data: pVals,
                backgroundColor: colors,
                borderWidth: 0,
                hoverBorderWidth: 2,
                hoverBorderColor: '#fff'
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            cutout: '72%',
            plugins: {
                legend: { display: false },
                tooltip: {
                    backgroundColor: '#0f172a',
                    titleColor: '#f0f0ff',
                    bodyColor: '#94a3b8',
                    cornerRadius: 8,
                    padding: 10,
                    callbacks: {
                        label: function(ctx) {
                            return ctx.label + ': ' + fmtShort(ctx.raw) + ' (' + data.platforms[ctx.label].pct + '%)';
                        }
                    }
                }
            }
        }
    });
}

/* ── Revenue Forecast Chart ── */
function renderForecastChart(data) {
    destroyChart('forecastChart');
    var fc = data.forecast;
    
    charts.forecastChart = new Chart(document.getElementById('forecastChart'), {
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
                    pointHoverRadius: 4,
                    spanGaps: false
                },
                {
                    label: 'Forecast',
                    data: fc.forecast,
                    borderColor: '#10b981',
                    borderDash: [5, 5],
                    backgroundColor: 'rgba(16,185,129,0.05)',
                    fill: true,
                    tension: 0.3,
                    borderWidth: 2,
                    pointRadius: 0,
                    pointHoverRadius: 4,
                    spanGaps: false
                },
                {
                    label: 'Target',
                    data: fc.target_line,
                    borderColor: '#94a3b8',
                    borderDash: [3, 3],
                    backgroundColor: 'transparent',
                    fill: false,
                    tension: 0,
                    borderWidth: 1.5,
                    pointRadius: 0,
                    pointHoverRadius: 3
                }
            ]
        },
        options: cOpts({
            plugins: {
                tooltip: {
                    mode: 'index',
                    intersect: false,
                    filter: function(item) { return item.raw !== null; },
                    callbacks: {
                        label: function(ctx) {
                            if (ctx.raw === null) return null;
                            return ctx.dataset.label + ': ' + fmtShort(ctx.raw);
                        }
                    }
                }
            },
            scales: {
                x: {
                    ticks: {
                        maxTicksLimit: 8,
                        callback: function(val, idx) {
                            var label = this.getLabelForValue(val);
                            if (idx === 0 || idx === this.max || idx % 5 === 0) return label;
                            return '';
                        }
                    }
                }
            },
            interaction: { mode: 'index', intersect: false }
        })
    });
}

/* ══════════════════════════════════════════════════════════════
   INIT
   ══════════════════════════════════════════════════════════════ */
window.addEventListener('DOMContentLoaded', initCharts);
