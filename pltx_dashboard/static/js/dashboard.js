(function () {
    function releaseDashboardClickBlockers() {
        var loadingOverlay = document.getElementById('htmxLoadingOverlay');
        if (loadingOverlay) {
            loadingOverlay.style.pointerEvents = 'none';
            if (loadingOverlay.dataset.allowVisible !== '1') {
                loadingOverlay.style.display = 'none';
            }
        }

        var drawer = document.getElementById('filterDrawer');
        var overlay = document.getElementById('filterDrawerOverlay');
        if (drawer && overlay && !drawer.classList.contains('open')) {
            document.body.classList.remove('filter-drawer-open');
            overlay.classList.remove('open');
            overlay.style.pointerEvents = 'none';
            overlay.style.display = 'none';
            drawer.style.pointerEvents = 'none';
        }
    }

    window.releaseDashboardClickBlockers = releaseDashboardClickBlockers;

    window.toggleFilterDrawer = window.toggleFilterDrawer || function () {
        var drawer = document.getElementById('filterDrawer');
        var overlay = document.getElementById('filterDrawerOverlay');
        if (!drawer || !overlay) return;

        var isOpening = !drawer.classList.contains('open');
        var topbarContainer = document.getElementById('topbarDefaultFilters');
        var drawerContainer = document.getElementById('drawerDefaultFilters');
        var divider = document.getElementById('drawerDefaultFiltersDivider');
        var secBar = document.getElementById('secondaryFilters');

        if (isOpening) {
            if (secBar) {
                secBar.style.zIndex = '1001';
                secBar.dataset.originalBackdrop = secBar.style.backdropFilter || '';
                secBar.style.backdropFilter = 'none';
                secBar.style.webkitBackdropFilter = 'none';
            }
            document.body.style.overflow = 'hidden';
            document.body.classList.add('filter-drawer-open');
            if (topbarContainer && drawerContainer) {
                while (topbarContainer.firstChild) {
                    drawerContainer.appendChild(topbarContainer.firstChild);
                }
                if (divider) divider.style.display = 'block';
            }
            drawer.classList.add('open');
            overlay.classList.add('open');
            overlay.style.display = 'block';
            drawer.style.pointerEvents = 'auto';
            setTimeout(function () {
                if (typeof window.refreshDashboardFilterSelects === 'function') {
                    window.refreshDashboardFilterSelects();
                }
            }, 0);
        } else {
            drawer.classList.remove('open');
            overlay.classList.remove('open');
            document.body.classList.remove('filter-drawer-open');
            overlay.style.pointerEvents = 'none';
            overlay.style.display = 'none';
            drawer.style.pointerEvents = 'none';
            if (topbarContainer && drawerContainer) {
                while (drawerContainer.firstChild) {
                    topbarContainer.appendChild(drawerContainer.firstChild);
                }
                if (divider) divider.style.display = 'none';
            }
            setTimeout(function () {
                if (secBar) {
                    secBar.style.zIndex = '';
                    secBar.style.backdropFilter = secBar.dataset.originalBackdrop || '';
                    secBar.style.webkitBackdropFilter = secBar.dataset.originalBackdrop || '';
                }
                document.body.style.overflow = '';
                if (typeof window.refreshDashboardFilterSelects === 'function') {
                    window.refreshDashboardFilterSelects();
                }
            }, 300);
        }
    };

    window.forceDashboardRefresh = window.forceDashboardRefresh || async function (button) {
        if (button && button.disabled) return;
        releaseDashboardClickBlockers();

        if (button) {
            button.disabled = true;
            button.classList.add('is-loading');
            var labelNode = button.querySelector('.sidebar-refresh-btn-text');
            if (labelNode) labelNode.textContent = 'Refreshing...';
        }

        var timeout = null;
        try {
            var controller = window.AbortController ? new AbortController() : null;
            timeout = setTimeout(function () {
                if (controller) controller.abort();
            }, 4000);
            await fetch(window.DASHBOARD_REFRESH_NOW_URL || '/api/dashboard/refresh-now/', {
                credentials: 'same-origin',
                cache: 'no-store',
                signal: controller ? controller.signal : undefined,
                headers: { 'X-Requested-With': 'XMLHttpRequest' }
            });
        } catch (e) {
            // reload anyway
        } finally {
            if (timeout) clearTimeout(timeout);
        }

        var refreshedUrl = new URL(window.location.href);
        refreshedUrl.searchParams.set('_refresh', String(Date.now()));
        window.location.replace(refreshedUrl.toString());
    };

    if (!window.__dashboardClickDelegationBound) {
        window.__dashboardClickDelegationBound = true;
        document.addEventListener('click', function (event) {
            var refreshButton = event.target.closest && event.target.closest('#sidebarRefreshButton, [data-dashboard-action="refresh"]');
            if (refreshButton) {
                event.preventDefault();
                event.stopPropagation();
                window.forceDashboardRefresh(refreshButton);
                return;
            }

            var filterButton = event.target.closest && event.target.closest('.btn-open-drawer, [data-dashboard-action="open-filters"]');
            if (filterButton) {
                event.preventDefault();
                event.stopPropagation();
                window.releaseDashboardClickBlockers();
                window.toggleFilterDrawer();
                return;
            }

            var drawer = document.getElementById('filterDrawer');
            if (drawer && drawer.classList.contains('open')) {
                var clickedSelect2 = event.target.closest && event.target.closest('.select2-container, .select2-dropdown');
                if (clickedSelect2) return;
                var clickedInsideDrawer = event.target.closest && event.target.closest('#filterDrawer');
                if (!clickedInsideDrawer) {
                    window.toggleFilterDrawer();
                }
            }
        }, true);
    }
})();
