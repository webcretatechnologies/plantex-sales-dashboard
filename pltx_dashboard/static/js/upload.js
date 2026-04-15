// Get CSRF token from cookie (Django sets this automatically)
function getCookie(name) {
    let v = null;
    document.cookie.split(';').forEach(c => {
        c = c.trim();
        if (c.startsWith(name + '=')) v = decodeURIComponent(c.substring(name.length + 1));
    });
    return v;
}

// ---------------------------------------------------------------------------
// Platform Switching
// ---------------------------------------------------------------------------
function getSelectedPlatform() {
    return document.querySelector('input[name="platform"]:checked')?.value || 'amazon';
}

document.querySelectorAll('input[name="platform"]').forEach(radio => {
    radio.addEventListener('change', () => {
        let platform = getSelectedPlatform();
        document.getElementById('amazonSection').classList.toggle('active', platform === 'amazon');
        document.getElementById('flipkartSection').classList.toggle('active', platform === 'flipkart');

        // Reset file inputs and lists when switching
        clearAllFiles();
        document.getElementById('loadBtn').disabled = true;
    });
});

// ---------------------------------------------------------------------------
// File input IDs and list IDs — grouped by platform
// ---------------------------------------------------------------------------
const AMAZON_IDS = ['csvInput', 'catFile', 'spendFile', 'priceFile'];
const AMAZON_LISTS = ['fileList', 'catFileList', 'spendFileList', 'priceFileList'];

const FK_IDS = [
    'fkSearchTrafficFile', 'fkCategoryFile', 'fkPriceFile',
    'fkPcaNewFile', 'fkPlaNewFile', 'fkSalesInvoiceFile', 'fkCouponFile'
];
const FK_LISTS = [
    'fkSearchTrafficFileList', 'fkCategoryFileList', 'fkPriceFileList',
    'fkPcaNewFileList', 'fkPlaNewFileList', 'fkSalesInvoiceFileList', 'fkCouponFileList'
];

function clearAllFiles() {
    // Amazon inputs
    AMAZON_IDS.forEach(id => { let el = document.getElementById(id); if (el) el.value = ''; });
    AMAZON_LISTS.forEach(id => { let el = document.getElementById(id); if (el) el.innerHTML = ''; });
    // Flipkart inputs
    FK_IDS.forEach(id => { let el = document.getElementById(id); if (el) el.value = ''; });
    FK_LISTS.forEach(id => { let el = document.getElementById(id); if (el) el.innerHTML = ''; });
}

// ---------------------------------------------------------------------------
// Enable/disable Process button based on whether any file is selected
// ---------------------------------------------------------------------------
function updateProcessButton() {
    let platform = getSelectedPlatform();
    let hasFiles = false;

    if (platform === 'amazon') {
        hasFiles = document.getElementById('csvInput').files.length > 0;
    } else {
        FK_IDS.forEach(id => {
            let el = document.getElementById(id);
            if (el && el.files.length > 0) hasFiles = true;
        });
    }

    document.getElementById('loadBtn').disabled = !hasFiles;
}

// ---------------------------------------------------------------------------
// File input → render file pills + enable button
// ---------------------------------------------------------------------------
[
    { id: 'csvInput', listId: 'fileList' },
    { id: 'catFile', listId: 'catFileList' },
    { id: 'spendFile', listId: 'spendFileList' },
    { id: 'priceFile', listId: 'priceFileList' },
    // Flipkart (7 file types)
    { id: 'fkSearchTrafficFile', listId: 'fkSearchTrafficFileList' },
    { id: 'fkCategoryFile', listId: 'fkCategoryFileList' },
    { id: 'fkPriceFile', listId: 'fkPriceFileList' },
    { id: 'fkPcaNewFile', listId: 'fkPcaNewFileList' },
    { id: 'fkPlaNewFile', listId: 'fkPlaNewFileList' },
    { id: 'fkSalesInvoiceFile', listId: 'fkSalesInvoiceFileList' },
    { id: 'fkCouponFile', listId: 'fkCouponFileList' },
].forEach(cfg => {
    let el = document.getElementById(cfg.id);
    if (!el) return;
    el.addEventListener('change', (e) => {
        let listEl = document.getElementById(cfg.listId);
        listEl.innerHTML = Array.from(e.target.files)
            .map(f => `<span class="file-pill">${f.name}</span>`)
            .join('');
        updateProcessButton();
    });
});

// ---------------------------------------------------------------------------
// Main upload handler
// ---------------------------------------------------------------------------
async function loadDashboard() {
    let btn = document.getElementById('loadBtn');
    let status = document.getElementById('statusMsg');
    let platform = getSelectedPlatform();

    btn.disabled = true;
    let wss = window.location.protocol === "https:" ? "wss" : "ws";
    let ws = new WebSocket(`${wss}://${window.location.host}/ws/upload-progress/`);

    ws.onmessage = function(event) {
        let data = JSON.parse(event.data);
        status.textContent = `⚙️ ${data.message}`;
        if (data.status === 'complete') {
            status.textContent = '✅ All files processed successfully!';
            if (platform === 'amazon') {
                setTimeout(() => { window.location.href = '/dashboard/business/'; }, 500);
            }
        } else if (data.status === 'error') {
            btn.disabled = false;
        }
    };

    try {
        status.textContent = '⏳ Queuing files...';

        // Collect all file-type pairs based on platform
        let fileQueue = [];

        if (platform === 'amazon') {
            let csvFiles = document.getElementById('csvInput').files;
            let catFiles = document.getElementById('catFile').files;
            let spendFiles = document.getElementById('spendFile').files;
            let priceFiles = document.getElementById('priceFile').files;

            if (csvFiles.length === 0) {
                alert("Please upload at least one Daily Sales CSV!");
                btn.disabled = false;
                return;
            }

            for (let i = 0; i < csvFiles.length; i++) fileQueue.push({ file: csvFiles[i], type: 'sales' });
            for (let i = 0; i < catFiles.length; i++) fileQueue.push({ file: catFiles[i], type: 'category' });
            for (let i = 0; i < spendFiles.length; i++) fileQueue.push({ file: spendFiles[i], type: 'spend' });
            for (let i = 0; i < priceFiles.length; i++) fileQueue.push({ file: priceFiles[i], type: 'price' });

        } else {
            // Flipkart — 7 file types mapped to slim pipeline
            const fkFileMap = [
                { inputId: 'fkSearchTrafficFile', type: 'fk_search_traffic' },
                { inputId: 'fkCategoryFile',      type: 'fk_category' },
                { inputId: 'fkPriceFile',         type: 'fk_price' },
                { inputId: 'fkPcaNewFile',        type: 'fk_pca' },
                { inputId: 'fkPlaNewFile',        type: 'fk_pla' },
                { inputId: 'fkSalesInvoiceFile',  type: 'fk_sales_invoice' },
                { inputId: 'fkCouponFile',        type: 'fk_coupon' },
            ];

            let totalFk = 0;
            fkFileMap.forEach(m => {
                let el = document.getElementById(m.inputId);
                if (el) {
                    for (let i = 0; i < el.files.length; i++) {
                        fileQueue.push({ file: el.files[i], type: m.type });
                        totalFk++;
                    }
                }
            });

            if (totalFk === 0) {
                alert("Please upload at least one Flipkart report!");
                btn.disabled = false;
                return;
            }
        }

        let totalFiles = fileQueue.length;

        for (let idx = 0; idx < fileQueue.length; idx++) {
            let item = fileQueue[idx];
            let isLast = (idx === totalFiles - 1);

            status.textContent = `⏳ Uploading ${item.file.name} (${idx + 1}/${totalFiles})...`;

            let form = new FormData();
            form.append("file", item.file);
            form.append("file_type", item.type);
            if (isLast) form.append("is_last", "true");

            // For Amazon sales, extract date from filename
            if (item.type === 'sales') {
                let filename = item.file.name;
                let dateStr = filename.replace(/\.csv$/i, '').substring(0, 10);
                form.append("date", dateStr);
            }

            await fetch("/api/upload/", {
                method: "POST",
                body: form,
                headers: { "X-CSRFToken": getCookie("csrftoken") },
                credentials: "same-origin"
            });
        }

    } catch (err) {
        status.textContent = "❌ " + err.message;
        btn.disabled = false;
    }
}
