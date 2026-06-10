// Make sure inventory health select is initialized as a select2.
// Fires on both first full-page load and after HTMX SPA navigations.
function initInventoryHealthSelect() {
    if (typeof window.jQuery === 'undefined') return;
    var $ = window.jQuery;
    if (!$.fn || !$.fn.select2) return;
    var $el = $('#inventoryHealthSelect');
    if (!$el.length) return;
    if ($el.data('select2')) {
        try { $el.select2('destroy'); } catch (e) { }
    }
    $el.select2({
        width: '220px',
        placeholder: 'All Statuses',
        closeOnSelect: false,
        allowClear: false
    });
    $el.on('change', function () {
        if (typeof markFiltersDirty === 'function') markFiltersDirty();
    });
}

document.addEventListener('DOMContentLoaded', function () {
    setTimeout(initInventoryHealthSelect, 300);
});

document.addEventListener('dashboardContentLoaded', function () {
    initInventoryHealthSelect();
});
