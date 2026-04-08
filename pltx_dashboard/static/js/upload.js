// Get CSRF token from cookie (Django sets this automatically)
function getCookie(name) {
    let v = null;
    document.cookie.split(';').forEach(c => {
        c = c.trim();
        if (c.startsWith(name + '=')) v = decodeURIComponent(c.substring(name.length + 1));
    });
    return v;
}

async function loadDashboard() {
    let btn = document.getElementById('loadBtn');
    let status = document.getElementById('statusMsg');
    
    let csvFilesArr = document.getElementById('csvInput').files;
    let catFileEl = document.getElementById('catFile').files[0];
    let spendFiles = document.getElementById('spendFile').files;
    let priceFileEl = document.getElementById('priceFile').files[0];
    
    if (csvFilesArr.length === 0) {
        alert("Please upload sales CSVs!");
        return;
    }
    
    btn.disabled = true;
    let wss = window.location.protocol === "https:" ? "wss" : "ws";
    let ws = new WebSocket(`${wss}://${window.location.host}/ws/upload-progress/`);
    
    ws.onmessage = function(event) {
        let data = JSON.parse(event.data);
        status.textContent = `⚙️ ${data.message}`;
        if (data.status === 'complete') {
            status.textContent = '✅ Dashboard data generated! Redirecting...';
            setTimeout(() => {
                window.location.href = '/dashboard/business/';
            }, 500);
        } else if (data.status === 'error') {
            btn.disabled = false;
        }
    };

    try {
        status.textContent = '⏳ Queuing files...';
        let totalFiles = csvFilesArr.length + (catFileEl ? 1 : 0) + (priceFileEl ? 1 : 0) + spendFiles.length;
        let filesUploaded = 0;

        const uploadFile = async (file, fileType) => {
            filesUploaded++;
            let form = new FormData();
            form.append("file", file);
            form.append("file_type", fileType);
            if (filesUploaded === totalFiles) {
                form.append("is_last", "true");
            }
            await fetch("/api/upload/", {
                method: "POST",
                body: form,
                headers: { "X-CSRFToken": getCookie("csrftoken") },
                credentials: "same-origin"
            });
        };

        for(let i=0; i<csvFilesArr.length; i++){
            await uploadFile(csvFilesArr[i], "sales");
        }
        if(catFileEl){
            await uploadFile(catFileEl, "category");
        }
        if(priceFileEl){
            await uploadFile(priceFileEl, "price");
        }
        if(spendFiles.length > 0){
            for(let i=0; i<spendFiles.length; i++){
                await uploadFile(spendFiles[i], "spend");
            }
        }
        
    } catch (err) {
        status.textContent="❌ "+err.message;
        btn.disabled=false;
    }
}

document.getElementById('csvInput').addEventListener('change', e => {
    document.getElementById('fileList').innerHTML = Array.from(e.target.files).map(f => `<span class="file-pill">${f.name}</span>`).join('');
    document.getElementById('loadBtn').disabled = e.target.files.length === 0;
});
['catFile','spendFile','priceFile'].forEach(id => {
    document.getElementById(id).addEventListener('change', () => {
        document.getElementById('loadBtn').disabled = document.getElementById('csvInput').files.length === 0;
    });
});
