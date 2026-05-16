// Get CSRF token from cookie (Django sets this automatically)
function getCookie(name) {
    let v = null;
    document.cookie.split(';').forEach(c => {
        c = c.trim();
        if (c.startsWith(name + '=')) v = decodeURIComponent(c.substring(name.length + 1));
    });
    return v;
}

let uploadFlowPlatform = null;
let uploadFlowStarted = false;
let uploadFlowCompleted = false;

function syncInlineUploadStatusFromState(state) {
    const statusEl = document.getElementById('statusMsg');
    if (!statusEl) return;
    const message = String((state && state.message) || "");
    if (!message) return;
    statusEl.textContent = `⚙️ ${message}`;
}

function handleGlobalUploadProgressEvent(event) {
    const data = (event && event.detail) || {};
    const statusEl = document.getElementById('statusMsg');
    if (!statusEl) return;

    const status = String(data.status || "").toLowerCase();
    const message = String(data.message || "");
    if (message) {
        statusEl.textContent = `⚙️ ${message}`;
    }

    if (status === "complete" || status === "success") {
        uploadFlowCompleted = true;
        statusEl.textContent = "✅ All files processed successfully!";
        if (uploadFlowStarted && uploadFlowPlatform) {
            if (uploadFlowPlatform === 'amazon') {
                uploadFlowCompleted = true;
                setTimeout(() => { window.location.href = '/dashboard/business/'; }, 500);
            } else if (uploadFlowPlatform === 'flipkart') {
                uploadFlowCompleted = true;
                setTimeout(() => { window.location.href = '/dashboard/business/?platform=Flipkart'; }, 500);
            }
        }
    } else if (status === "error") {
        uploadFlowCompleted = true;
        const btn = document.getElementById('loadBtn');
        if (btn) btn.disabled = false;
    }
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
const AMAZON_IDS = ['csvInput', 'catFile', 'spendFile', 'priceFile', 'fbaStockFile', 'flexStockFile'];
const AMAZON_LISTS = ['fileList', 'catFileList', 'spendFileList', 'priceFileList', 'fbaStockFileList', 'flexStockFileList'];

const FK_IDS = [
    'fkSearchTrafficFile', 'fkCategoryFile', 'fkPriceFile',
    'fkPlaNewFile', 'fkFbaStockFile', 'fkInventoryFile'
];
const FK_LISTS = [
    'fkSearchTrafficFileList', 'fkCategoryFileList', 'fkPriceFileList',
    'fkPlaNewFileList', 'fkFbaStockFileList', 'fkInventoryFileList'
];

const DEMO_TEMPLATE_MAP = {
    csvInput: 'upload_sales',
    catFile: 'upload_category',
    spendFile: 'upload_spend',
    priceFile: 'upload_price',
    fbaStockFile: 'upload_fba_stock',
    flexStockFile: 'upload_flex_stock',
    fkSearchTrafficFile: 'fk_search_traffic',
    fkCategoryFile: 'fk_category',
    fkPriceFile: 'fk_price',
    fkPlaNewFile: 'fk_pla',
    fkFbaStockFile: 'fk_fba_stock',
    fkInventoryFile: 'fk_inventory',
};

function initDemoTemplateButtons() {
    Object.entries(DEMO_TEMPLATE_MAP).forEach(([inputId, templateKey]) => {
        const input = document.getElementById(inputId);
        if (!input) return;
        const card = input.closest('.upload-card');
        if (!card || card.querySelector(`.demo-template-link[data-template="${templateKey}"]`)) return;

        const link = document.createElement('a');
        link.href = `/api/demo-template/?template=${encodeURIComponent(templateKey)}`;
        link.className = 'demo-template-link';
        link.setAttribute('data-template', templateKey);
        link.textContent = 'Download Demo File';
        link.style.cssText = 'display:inline-flex;align-items:center;gap:6px;margin:-8px 0 16px;padding:8px 12px;border-radius:8px;border:1px solid #dbeafe;background:#eff6ff;color:#1d4ed8;font-size:12px;font-weight:600;text-decoration:none;';

        const title = card.querySelector('.upload-card-title');
        if (title && title.nextSibling) {
            card.insertBefore(link, title.nextSibling);
        } else if (title) {
            title.insertAdjacentElement('afterend', link);
        } else {
            card.prepend(link);
        }
    });
}

if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initDemoTemplateButtons);
} else {
    initDemoTemplateButtons();
}

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
        AMAZON_IDS.forEach(id => {
            let el = document.getElementById(id);
            if (el && el.files.length > 0) hasFiles = true;
        });
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
    // Flipkart
    { id: 'fkSearchTrafficFile', listId: 'fkSearchTrafficFileList' },
    { id: 'fkCategoryFile', listId: 'fkCategoryFileList' },
    { id: 'fkPriceFile', listId: 'fkPriceFileList' },
    { id: 'fkPlaNewFile', listId: 'fkPlaNewFileList' },
    { id: 'fkFbaStockFile', listId: 'fkFbaStockFileList' },
    { id: 'fkInventoryFile', listId: 'fkInventoryFileList' },
    // Amazon stock files
    { id: 'fbaStockFile', listId: 'fbaStockFileList' },
    { id: 'flexStockFile', listId: 'flexStockFileList' },
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

function generateBatchId() {
    if (window.crypto && typeof window.crypto.randomUUID === 'function') {
        return window.crypto.randomUUID().replace(/-/g, '');
    }
    return `batch_${Date.now()}_${Math.random().toString(16).slice(2, 10)}`;
}

async function loadDashboard() {
    let btn = document.getElementById('loadBtn');
    let status = document.getElementById('statusMsg');
    let platform = getSelectedPlatform();
    uploadFlowPlatform = platform;
    uploadFlowStarted = true;
    uploadFlowCompleted = false;

    btn.disabled = true;

    try {
        status.textContent = '⏳ Queuing files...';
        if (typeof window.setGlobalUploadProgress === "function") {
            window.setGlobalUploadProgress({
                status: "processing",
                message: "Files are queued for upload.",
                active: true,
            });
        }

        // Collect all file-type pairs based on platform
        let fileQueue = [];

        if (platform === 'amazon') {
            let csvFiles = document.getElementById('csvInput').files;
            let catFiles = document.getElementById('catFile').files;
            let spendFiles = document.getElementById('spendFile').files;
            let priceFiles = document.getElementById('priceFile').files;

            for (let i = 0; i < csvFiles.length; i++) fileQueue.push({ file: csvFiles[i], type: 'sales' });
            for (let i = 0; i < catFiles.length; i++) fileQueue.push({ file: catFiles[i], type: 'category' });
            for (let i = 0; i < spendFiles.length; i++) fileQueue.push({ file: spendFiles[i], type: 'spend' });
            for (let i = 0; i < priceFiles.length; i++) fileQueue.push({ file: priceFiles[i], type: 'price' });

            let fbaStockFiles = document.getElementById('fbaStockFile').files;
            let flexStockFiles = document.getElementById('flexStockFile').files;
            for (let i = 0; i < fbaStockFiles.length; i++) fileQueue.push({ file: fbaStockFiles[i], type: 'fba_stock' });
            for (let i = 0; i < flexStockFiles.length; i++) fileQueue.push({ file: flexStockFiles[i], type: 'flex_stock' });

            if (fileQueue.length === 0) {
                uploadFlowStarted = false;
                alert("Please upload at least one Amazon file.");
                btn.disabled = false;
                return;
            }

        } else {
            // Flipkart file types mapped to slim pipeline
            const fkFileMap = [
                { inputId: 'fkSearchTrafficFile', type: 'fk_search_traffic' },
                { inputId: 'fkCategoryFile', type: 'fk_category' },
                { inputId: 'fkPriceFile', type: 'fk_price' },
                { inputId: 'fkPlaNewFile', type: 'fk_pla' },
                { inputId: 'fkFbaStockFile', type: 'fk_fba_stock' },
                { inputId: 'fkInventoryFile', type: 'fk_inventory' },
            ];

            const missingFlipkartInputs = fkFileMap.filter(m => {
                const el = document.getElementById(m.inputId);
                return !el || !el.files || el.files.length === 0;
            });
            if (missingFlipkartInputs.length > 0) {
                uploadFlowStarted = false;
                alert("Please upload all Flipkart required files: Search Traffic, Category, PLA, Price, FK FBA Stock, and FK Inventory.");
                btn.disabled = false;
                return;
            }

            fkFileMap.forEach(m => {
                let el = document.getElementById(m.inputId);
                if (el) {
                    for (let i = 0; i < el.files.length; i++) {
                        fileQueue.push({ file: el.files[i], type: m.type });
                    }
                }
            });
        }

        let totalFiles = fileQueue.length;
        let batchId = generateBatchId();

        for (let idx = 0; idx < fileQueue.length; idx++) {
            let item = fileQueue[idx];

            status.textContent = `⏳ Uploading ${item.file.name} (${idx + 1}/${totalFiles})...`;
            if (typeof window.setGlobalUploadProgress === "function") {
                window.setGlobalUploadProgress({
                    status: "processing",
                    message: `Uploading ${item.file.name} (${idx + 1}/${totalFiles})...`,
                    active: true,
                });
            }

            let form = new FormData();
            form.append("file", item.file);
            form.append("file_type", item.type);
            form.append("batch_id", batchId);
            form.append("batch_total", String(totalFiles));

            // For Amazon sales, extract date from filename
            if (item.type === 'sales') {
                let filename = item.file.name;
                let dateStr = filename.replace(/\.(csv|xlsx|xls|xlsm)$/i, '').substring(0, 10);
                form.append("date", dateStr);
            }

            let resp = await fetch("/api/upload/", {
                method: "POST",
                body: form,
                headers: { "X-CSRFToken": getCookie("csrftoken") },
                credentials: "same-origin"
            });

            let respData = await resp.json();

            if (!resp.ok) {
                status.textContent = `❌ Upload failed: ${respData.error || 'Unknown error'}`;
                if (typeof window.setGlobalUploadProgress === "function") {
                    window.setGlobalUploadProgress({
                        status: "error",
                        message: `Upload failed: ${respData.error || 'Unknown error'}`,
                        active: false,
                        visible_until: Date.now() + 12000,
                    });
                }
                btn.disabled = false;
                return;
            }
        }

        status.textContent = "✅ All files uploaded. Processing continues in background. You can navigate safely.";
        if (typeof window.setGlobalUploadProgress === "function") {
            window.setGlobalUploadProgress({
                status: "processing",
                message: "All files uploaded. Processing and dashboard update continue in background.",
                active: true,
            });
        }

    } catch (err) {
        uploadFlowCompleted = true;
        status.textContent = "❌ " + err.message;
        if (typeof window.setGlobalUploadProgress === "function") {
            window.setGlobalUploadProgress({
                status: "error",
                message: err.message || "Upload failed.",
                active: false,
                visible_until: Date.now() + 12000,
            });
        }
        btn.disabled = false;
    }
}

document.addEventListener('DOMContentLoaded', function () {
    if (typeof window.readUploadProgressState === "function") {
        syncInlineUploadStatusFromState(window.readUploadProgressState());
    }
    window.addEventListener('pltx-upload-progress', handleGlobalUploadProgressEvent);
});
