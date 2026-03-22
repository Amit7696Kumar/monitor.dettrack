function qs(sel, root=document){ return root.querySelector(sel); }
function qsa(sel, root=document){ return Array.from(root.querySelectorAll(sel)); }

function safeJsonParse(str) {
  try { return JSON.parse(str); } catch { return null; }
}

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function getCsrfToken() {
  return window.__CSRF_TOKEN || document.querySelector('meta[name="csrf-token"]')?.content || "";
}

function ensureCsrfInput(form) {
  if (!form || String(form.method || "").toLowerCase() !== "post") return;
  const token = getCsrfToken();
  if (!token) return;
  let input = form.querySelector('input[name="csrf_token"]');
  if (!input) {
    input = document.createElement("input");
    input.type = "hidden";
    input.name = "csrf_token";
    form.appendChild(input);
  }
  input.value = token;
}

function buildUploadPreviewUrl(src) {
  const raw = String(src || "").trim();
  if (!raw) return "";
  return `/api/uploads/preview?src=${encodeURIComponent(raw)}`;
}

function renderUploadImageCard({ src, title, alt, eager = false }) {
  const fullSrc = String(src || "").trim();
  if (!fullSrc) return "";
  const previewSrc = buildUploadPreviewUrl(fullSrc) || fullSrc;
  const safeTitle = escapeHtml(title || "Uploaded Image");
  const safeAlt = escapeHtml(alt || title || "Uploaded Image");
  const loading = eager ? "eager" : "lazy";
  return `
    <div class="imgcard">
      <div class="small-muted" style="margin-bottom:6px;">${safeTitle}</div>
      <img
        src="${previewSrc}"
        data-fullsrc="${fullSrc}"
        loading="${loading}"
        decoding="async"
        alt="${safeAlt}"
        style="width:100%;border-radius:10px;border:1px solid #333;"
      />
    </div>
  `;
}

const warmedUploadPreviews = new Set();

function warmUploadPreview(src) {
  const previewSrc = buildUploadPreviewUrl(src);
  if (!previewSrc || warmedUploadPreviews.has(previewSrc)) return;
  warmedUploadPreviews.add(previewSrc);
  const img = new Image();
  img.decoding = "async";
  img.src = previewSrc;
}

function warmPreviewFromTrigger(trigger) {
  if (!trigger) return;
  const directSrc = String(trigger.dataset?.src || "").trim();
  const directSrc2 = String(trigger.dataset?.src2 || "").trim();
  if (directSrc) warmUploadPreview(directSrc);
  if (directSrc2) warmUploadPreview(directSrc2);

  const row = trigger.closest("tr, .admin-mobile-reading-card");
  const filename = String(row?.dataset?.filename || "").trim();
  const filename2 = String(row?.dataset?.filename2 || "").trim();
  if (filename) warmUploadPreview(`/uploads/${filename}`);
  if (filename2) warmUploadPreview(`/uploads/${filename2}`);
}

document.addEventListener("mouseover", (event) => {
  const trigger = event.target instanceof Element ? event.target.closest(".js-open-image, .js-open-reading") : null;
  if (trigger) warmPreviewFromTrigger(trigger);
});

document.addEventListener("focusin", (event) => {
  const trigger = event.target instanceof Element ? event.target.closest(".js-open-image, .js-open-reading") : null;
  if (trigger) warmPreviewFromTrigger(trigger);
});

document.addEventListener("touchstart", (event) => {
  const trigger = event.target instanceof Element ? event.target.closest(".js-open-image, .js-open-reading") : null;
  if (trigger) warmPreviewFromTrigger(trigger);
}, { passive: true });

function setupTabs() {
  const tabs = qsa(".tab");
  const contents = qsa(".tab-content");
  if (!tabs.length) return;
  const storageKey = `activeTab:${window.location.pathname}`;

  const activateTab = (name) => {
    tabs.forEach(x => x.classList.remove("active"));
    contents.forEach(c => c.classList.remove("active"));
    const tab = tabs.find(x => x.dataset.tab === name);
    const content = qs(`[data-tab-content="${name}"]`);
    if (!tab || !content) return;
    tab.classList.add("active");
    content.classList.add("active");
    try { localStorage.setItem(storageKey, name); } catch {}
  };

  tabs.forEach(t => {
    t.addEventListener("click", () => {
      activateTab(t.dataset.tab);
    });
  });

  try {
    const saved = localStorage.getItem(storageKey);
    if (saved) activateTab(saved);
  } catch {}
}

function enhanceFormSubmits() {
  qsa('form[method="post"]').forEach((form) => {
    if (form.dataset.submitEnhanced === "1") return;
    form.dataset.submitEnhanced = "1";
    ensureCsrfInput(form);
    form.addEventListener("submit", (e) => {
      if (form.dataset.allowMultiSubmit === "1") return;
      if (form.dataset.submitting === "1") {
        e.preventDefault();
        return;
      }
      ensureCsrfInput(form);
      const btn = form.querySelector('button[type="submit"], input[type="submit"]');
      if (!btn) return;
      form.dataset.submitting = "1";
      btn.classList.add("is-loading");
      btn.disabled = true;
      if (btn.tagName === "BUTTON") {
        btn.dataset.originalLabel = btn.textContent || "";
        btn.textContent = "Processing...";
      } else if (btn.tagName === "INPUT") {
        btn.dataset.originalLabel = btn.value || "";
        btn.value = "Processing...";
      }
    });
  });
}

function setupToastLifecycle() {
  qsa(".toast").forEach((toast) => {
    if (toast.dataset.toastWired === "1") return;
    toast.dataset.toastWired = "1";
    if (toast.dataset.autodismiss === "0") return;
    if (toast.closest("#alertsLive")) return;
    const timeoutMs = Number(toast.dataset.timeout || "5000");
    window.setTimeout(() => {
      toast.classList.add("toast-hide");
      window.setTimeout(() => toast.remove(), 240);
    }, Number.isFinite(timeoutMs) ? timeoutMs : 5000);
  });
}

function setupPWAInstall() {
  let deferredPrompt = null;
  const installButtons = qsa(".js-install-app");
  const installHints = qsa(".js-install-hint");
  if (!installButtons.length && !installHints.length) return;

  const showInstallButtons = () => {
    installButtons.forEach((btn) => {
      btn.hidden = false;
      btn.style.display = "";
    });
  };

  const hideInstallButtons = () => {
    installButtons.forEach((btn) => {
      btn.hidden = true;
      btn.style.display = "none";
    });
  };

  const showIOSHint = () => {
    installHints.forEach((hint) => {
      hint.hidden = false;
      hint.style.display = "";
    });
  };

  const isIos = /iphone|ipad|ipod/i.test(navigator.userAgent || "");
  const isStandalone = window.matchMedia("(display-mode: standalone)").matches || window.navigator.standalone === true;

  if (isStandalone) {
    hideInstallButtons();
    return;
  }

  window.addEventListener("beforeinstallprompt", (e) => {
    e.preventDefault();
    deferredPrompt = e;
    showInstallButtons();
  });

  installButtons.forEach((btn) => {
    btn.addEventListener("click", async () => {
      if (!deferredPrompt) return;
      deferredPrompt.prompt();
      try { await deferredPrompt.userChoice; } catch {}
      deferredPrompt = null;
      hideInstallButtons();
    });
  });

  if (isIos) showIOSHint();
}

function registerServiceWorker() {
  if (!("serviceWorker" in navigator)) return;
  // Keep SW scope conservative and only in secure contexts (or localhost).
  if (!(window.isSecureContext || location.hostname === "127.0.0.1" || location.hostname === "localhost")) return;
  window.addEventListener("load", () => {
    const v = encodeURIComponent(String(window.__ASSET_VERSION || "1"));
    navigator.serviceWorker.register(`/static/sw.js?v=${v}`, { updateViaCache: "none" }).catch(() => {});
  });
}

function setupFiltering(tableId, searchId, meterSelectId, statusSelectId = null) {
  const table = qs(`#${tableId}`);
  const search = qs(`#${searchId}`);
  const meterSel = qs(`#${meterSelectId}`);
  const statusSel = statusSelectId ? qs(`#${statusSelectId}`) : null;
  const mobileCards = qsa(".admin-mobile-reading-card");

  if ((!table && !mobileCards.length) || !search || !meterSel) return;

  const meterKeywords = {
    earthing: ["earthing"],
    temp: ["temperature", " temp ", "temp"],
    voltage: ["voltage"],
    odometer: ["odometer"],
    fire_point: ["fire point", "fire_point", "firepoint"],
  };

  const meterMatchesItem = (meter, item) => {
    if (meter === "all") return true;
    const rowMeter = (item.dataset.meter || "").toLowerCase();
    if (rowMeter === meter) return true;

    // Support task-title filtering on merged tables where rowMeter can be "task"
    // and also cases where labels/titles carry the semantic meter type.
    const label = (item.dataset.label || "").toLowerCase();
    const taskCell = (item.querySelector("td:nth-child(3)")?.innerText || "").toLowerCase();
    const rowText = (` ${item.innerText.toLowerCase()} `);
    const haystack = ` ${label} ${taskCell} ${rowText} `;
    const keys = meterKeywords[meter] || [];
    return keys.some((k) => haystack.includes(` ${k} `) || haystack.includes(k));
  };

  const normalizeStatusBucket = (rawStatus) => {
    const status = String(rawStatus || "").toLowerCase().trim();
    if (["submitted", "completed", "late"].includes(status)) return "submitted";
    if (status === "overdue") return "overdue";
    return "pending";
  };

  const filter = () => {
    const q = (search.value || "").toLowerCase().trim();
    const meter = meterSel.value;
    const status = (statusSel?.value || "all").toLowerCase();

    qsa("tbody tr", table || document).forEach(tr => {
      const meterOk = meterMatchesItem(meter, tr);
      const text = tr.innerText.toLowerCase();
      const qOk = !q || text.includes(q);
      const rowStatus = ((tr.dataset.status || tr.querySelector("td:nth-child(5)")?.innerText || "").toLowerCase()).trim();
      const statusBucket = normalizeStatusBucket(rowStatus);
      const statusOk = status === "all" || statusBucket === status || rowStatus === status;
      tr.style.display = (meterOk && qOk && statusOk) ? "" : "none";
    });

    mobileCards.forEach((card) => {
      const meterOk = meterMatchesItem(meter, card);
      const text = card.innerText.toLowerCase();
      const qOk = !q || text.includes(q);
      const cardStatus = (card.dataset.status || "").toLowerCase().trim();
      const statusBucket = normalizeStatusBucket(cardStatus);
      const statusOk = status === "all" || statusBucket === status || cardStatus === status;
      card.style.display = (meterOk && qOk && statusOk) ? "" : "none";
    });

    document.dispatchEvent(new CustomEvent("dashboard:filter-changed", {
      detail: { tableId, status, meter, query: q }
    }));
  };

  search.addEventListener("input", filter);
  meterSel.addEventListener("change", filter);
  statusSel?.addEventListener("change", filter);
  filter();
  return { filter, normalizeStatusBucket };
}

function openModal({title, sub, images, jsonText}) {
  const modal = qs("#modal");
  const modalTitle = qs("#modalTitle");
  const modalSub = qs("#modalSub");
  const modalImages = qs("#modalImages");
  const modalJson = qs("#modalJson");

  modalTitle.textContent = title || "Reading";
  modalSub.textContent = sub || "";
  modalImages.innerHTML = "";
  modalJson.textContent = jsonText || "";

  (images || []).forEach(url => {
    if (!url) return;
    const img = document.createElement("img");
    img.src = url;
    img.loading = "lazy";
    modalImages.appendChild(img);
  });

  modal.classList.add("show");
}

function closeModal() {
  const modal = qs("#modal");
  if (modal) modal.classList.remove("show");
}

function wireModal(opts = {}) {
  const openImageInModal = opts.openImageInModal === true;

  const modal = document.getElementById("modal");
  const modalClose = document.getElementById("modalClose");
  const modalTitle = document.getElementById("modalTitle");
  const modalSub = document.getElementById("modalSub");
  const modalImages = document.getElementById("modalImages");
  const modalJson = document.getElementById("modalJson");

  if (!modal) return;

  function openModal(row) {
    const created = row.dataset.created || "";
    const meter = row.dataset.meter || "";
    const value = row.dataset.value || "";
    const team = row.dataset.team || "";
    const user = row.dataset.user || "";
    const label = row.dataset.label || "";
    const filename = row.dataset.filename || "";

    modalTitle.textContent = `Team ${team} | ${user} | ${meter.toUpperCase()} = ${value || "-"}`;
    modalSub.textContent = `${label} • ${created}`;

    modalImages.innerHTML = "";

    const imgs = [];
      if (filename) imgs.push({ label: "Uploaded", url: buildUploadPreviewUrl(`/uploads/${filename}`) || `/uploads/${filename}` });
    if (row.dataset.debugYolo) imgs.push({ label: "YOLO", url: row.dataset.debugYolo });
    if (row.dataset.debugCrop) imgs.push({ label: "Crop", url: row.dataset.debugCrop });
    if (row.dataset.debugPrep) imgs.push({ label: "Preprocess", url: row.dataset.debugPrep });

    imgs.forEach(i => {
      const wrap = document.createElement("div");
      wrap.className = "imgcard";
      wrap.innerHTML = `
        <div class="small-muted" style="margin-bottom:6px;"><b>${i.label}</b></div>
        <img src="${i.url}" style="max-width:100%; border:2px solid #333; border-radius:10px;" />
      `;
      modalImages.appendChild(wrap);
    });

    let raw = row.dataset.ocrjson || "";
    try {
      const obj = JSON.parse(raw);
      modalJson.textContent = JSON.stringify(obj, null, 2);
    } catch (e) {
      modalJson.textContent = raw || "(empty)";
    }

    modal.classList.add("open");
  }

  function closeModal() {
    modal.classList.remove("open");
  }

  modalClose?.addEventListener("click", closeModal);
  modal.addEventListener("click", (e) => {
    if (e.target === modal) closeModal();
  });

  // Click value -> modal
  document.querySelectorAll(".js-open-reading").forEach(btn => {
    btn.addEventListener("click", () => {
      const row = btn.closest("tr");
      if (row) openModal(row);
    });
  });

  // Click Open -> modal (same page)
  document.querySelectorAll(".js-open-image").forEach(btn => {
    btn.addEventListener("click", () => {
      const row = btn.closest("tr");
      if (!row) return;

      if (openImageInModal) {
        openModal(row);
      } else {
        const filename = row.dataset.filename;
        if (filename) window.open(`/uploads/${filename}`, "_blank");
      }
    });
  });
}

function decodeHtml(s) {
  const txt = document.createElement("textarea");
  txt.innerHTML = s;
  return txt.value;
}

function prettyJson(raw) {
  const obj = safeJsonParse(raw);
  if (!obj) return raw || "";
  return JSON.stringify(obj, null, 2);
}

/** Build trend chart from table rows */
function buildTrendChartFromTable(tableId, canvasId) {
  const table = qs(`#${tableId}`);
  const canvas = qs(`#${canvasId}`);
  if (!table || !canvas || !window.Chart) return;

  // group per day per meter_type: avg
  const rows = qsa("tbody tr", table);
  const buckets = {}; // {day: {earthing: [v], temp:[v]}}

  rows.forEach(tr => {
    const meter = tr.dataset.meter;
    const v = parseFloat(tr.dataset.value);
    if (!meter || Number.isNaN(v)) return;

    // created_at is text; take first 10 chars as YYYY-MM-DD if possible
    const created = tr.dataset.created || "";
    const day = created.slice(0, 10) || "unknown";

    if (!buckets[day]) buckets[day] = {earthing: [], temp: []};
    if (meter === "earthing") buckets[day].earthing.push(v);
    if (meter === "temp") buckets[day].temp.push(v);
  });

  const days = Object.keys(buckets).sort();
  const avg = (arr) => arr.length ? (arr.reduce((a,b)=>a+b,0)/arr.length) : null;

  const earthingSeries = days.map(d => avg(buckets[d].earthing));
  const tempSeries = days.map(d => avg(buckets[d].temp));

  new Chart(canvas, {
    type: "line",
    data: {
      labels: days,
      datasets: [
        { label: "Earthing (avg)", data: earthingSeries },
        { label: "Temp (avg)", data: tempSeries }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      plugins: { legend: { display: true } },
      scales: { y: { beginAtZero: true } }
    }
  });
}

/** Auto refresh alerts for admin/coadmin without reload */
async function pollAlerts() {
  const badge = qs("#unreadBadge");
  const list = qs("#alertsLive");
  const refreshState = qs("#alertsRefreshState");
  const role = (list && list.dataset.role) || null;
  const team = (list && list.dataset.team) || null;

  if (!role) return;

  try {
    if (list) list.classList.add("is-loading");
    if (refreshState) refreshState.textContent = "Updating alerts...";
    const url = role === "admin"
      ? `/api/alerts/admin`
      : `/api/alerts/coadmin?team=${encodeURIComponent(team || "")}`;

    const res = await fetch(url, { cache: "no-store" });
    if (!res.ok) return;
    const data = await res.json();

    if (badge) badge.textContent = data.unread_count ?? 0;

    if (list) {
      list.innerHTML = "";
      list.classList.add("alerts-list");
      (data.alerts || []).forEach(a => {
        const div = document.createElement("div");
        const level = (a.severity === "high" || a.severity === "low") ? a.severity : "ok";
        const statusText = a.is_read ? "Read" : "Unread";
        const unreadDot = a.is_read ? "" : `<span class="unread-dot"></span>`;
        const topTitle = role === "admin" ? "Team Alert" : "Team Notification";
        div.className = `alert-card ${level}`;
        div.innerHTML = `
          <div class="alert-top">
            <div>
              <div>
                <span class="pill ${level}">${String(a.severity || "ok").toUpperCase()}</span>
                <span style="margin-left:8px; font-weight:800;">${topTitle}</span>
              </div>
              <div style="margin-top:8px; font-weight:700;">${a.message}</div>
              <div class="alert-meta">
                <span>Created: ${a.created_at}</span>
                <span>${unreadDot}${statusText}</span>
              </div>
            </div>
            ${a.is_read ? "" : `
              <form method="post" action="/alerts/${a.id}/read">
                <button class="btn small" type="submit">Mark as Read</button>
              </form>
            `}
          </div>
        `;
        list.appendChild(div);
      });
      if (!(data.alerts || []).length) {
        list.innerHTML = `<div class="empty-state"><h3 class="empty-title">No active alerts</h3><p class="empty-copy">Everything looks stable right now. New alerts will appear here automatically.</p></div>`;
      }
    }
    if (refreshState) {
      const now = new Date();
      refreshState.textContent = `Last updated at ${now.toLocaleTimeString()}`;
    }
  } catch {
    if (refreshState) refreshState.textContent = "Unable to refresh alerts right now.";
  } finally {
    if (list) list.classList.remove("is-loading");
  }
}

async function pollLatestReadings() {
  const adminRef = qs("#adminAutoRefresh");
  const coadminRef = qs("#coadminAutoRefresh");
  let url = null;
  let ref = null;

  if (adminRef) {
    ref = adminRef;
    url = "/api/readings/admin/latest";
  } else if (coadminRef) {
    ref = coadminRef;
    const team = coadminRef.dataset.team || "";
    url = `/api/readings/coadmin/latest?team=${encodeURIComponent(team)}`;
  }
  if (!url || !ref) return;

  try {
    const res = await fetch(url, { cache: "no-store" });
    if (!res.ok) return;
    const data = await res.json();
    const current = Number(ref.dataset.latestId || "0");
    const latest = Number(data.latest_id || 0);
    if (latest > current) {
      window.location.reload();
      return;
    }
  } catch {}
}

function setupAlertsBellModal() {
  const bellButtons = qsa(".js-alert-bell");
  const modal = qs("#appAlertsModal");
  const closeBtn = qs("#appAlertsClose");
  const refreshBtn = qs("#appAlertsRefresh");
  const list = qs("#appAlertsList");
  const summary = qs("#appAlertsSummary");
  const meta = qs("#appAlertsMeta");
  const chatMeta = qs("#chatMeta");
  const unreadBadge = qs("#unreadBadge");
  if (!modal || !list || !chatMeta) return;

  const role = (chatMeta.dataset.role || "").toLowerCase();
  const team = chatMeta.dataset.team || "";
  if (!["admin", "coadmin"].includes(role)) return;

  const endpoint = role === "admin"
    ? "/api/alerts/admin"
    : `/api/alerts/coadmin?team=${encodeURIComponent(team)}`;

  const esc = (v) => String(v ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");

  const severityLabel = (sev) => {
    const s = String(sev || "ok").toLowerCase();
    if (s === "high") return "High";
    if (s === "low") return "Warning";
    return "Info";
  };

  const severityIcon = (sev) => {
    const s = String(sev || "ok").toLowerCase();
    if (s === "high") return "⛔";
    if (s === "low") return "⚠";
    return "ℹ";
  };

  const renderSummary = (items) => {
    const all = Array.isArray(items) ? items : [];
    const unread = all.filter((x) => !x.is_read).length;
    const high = all.filter((x) => String(x.severity || "").toLowerCase() === "high").length;
    const warning = all.filter((x) => String(x.severity || "").toLowerCase() === "low").length;
    if (!summary) return;
    summary.innerHTML = `
      <div class="alerts-chip"><span class="alerts-chip-label">Total</span><span class="alerts-chip-value">${all.length}</span></div>
      <div class="alerts-chip"><span class="alerts-chip-label">Unread</span><span class="alerts-chip-value">${unread}</span></div>
      <div class="alerts-chip high"><span class="alerts-chip-label">High</span><span class="alerts-chip-value">${high}</span></div>
      <div class="alerts-chip low"><span class="alerts-chip-label">Warning</span><span class="alerts-chip-value">${warning}</span></div>
    `;
  };

  const renderAlert = (a) => {
    const sev = (a.severity === "high" || a.severity === "low") ? a.severity : "ok";
    const statusText = a.is_read ? "Read" : "Unread";
    const statClass = a.is_read ? "read" : "unread";
    return `
      <article class="alert-card ${sev}">
        <div class="alert-row-head">
          <div class="alert-left-meta">
            <span class="alert-sev-icon ${sev}" aria-hidden="true">${severityIcon(sev)}</span>
            <span class="pill ${sev}">${severityLabel(sev)}</span>
            <span class="alert-status ${statClass}">${statusText}</span>
          </div>
          <div class="alert-time">${esc(a.created_at || "-")}</div>
        </div>
        <div class="alert-top">
          <div>
            <div class="alert-message">${esc(a.message || "")}</div>
            <div class="alert-meta">
              <span>Alert ID: #${Number(a.id || 0) || "-"}</span>
              <span>Target: ${role === "admin" ? "Admin" : `Team ${esc(team || "-")}`}</span>
            </div>
          </div>
          ${a.is_read ? "" : `<form method="post" action="/alerts/${a.id}/read"><button class="btn small" type="submit">Mark as Read</button></form>`}
        </div>
      </article>
    `;
  };

  async function loadAlerts() {
    list.innerHTML = `<div class="small-muted">Loading alerts...</div>`;
    try {
      const res = await fetch(endpoint, { cache: "no-store" });
      if (!res.ok) {
        list.innerHTML = `<div class="toast error">Unable to load alerts.</div>`;
        if (summary) summary.innerHTML = "";
        return;
      }
      const data = await res.json();
      if (unreadBadge && data && data.unread_count !== undefined) {
        unreadBadge.textContent = String(data.unread_count);
      }
      const items = data.alerts || [];
      renderSummary(items);
      if (!items.length) {
        list.innerHTML = `<div class="empty-state"><h3 class="empty-title">No active alerts</h3><p class="empty-copy">Everything looks stable right now.</p></div>`;
      } else {
        list.innerHTML = items.map(renderAlert).join("");
      }
      if (meta) {
        const now = new Date();
        meta.textContent = `Last updated at ${now.toLocaleTimeString()}`;
      }
    } catch {
      list.innerHTML = `<div class="toast error">Unable to load alerts right now.</div>`;
      if (summary) summary.innerHTML = "";
    }
  }

  function openModal() {
    modal.classList.add("open");
    modal.setAttribute("aria-hidden", "false");
    loadAlerts();
  }

  function closeModal() {
    modal.classList.remove("open");
    modal.setAttribute("aria-hidden", "true");
  }

  bellButtons.forEach((btn) => btn.addEventListener("click", openModal));
  document.addEventListener("click", (e) => {
    const trigger = e.target.closest(".js-alert-bell");
    if (!trigger) return;
    e.preventDefault();
    openModal();
  });
  closeBtn?.addEventListener("click", closeModal);
  refreshBtn?.addEventListener("click", () => { loadAlerts().catch(() => {}); });
  modal.addEventListener("click", (e) => {
    if (e.target === modal) closeModal();
  });
}

function wireModal() {
  const modal = document.getElementById("modal");
  const closeBtn = document.getElementById("modalClose");
  const jsonSection = document.getElementById("modalJsonSection");
  const modalJsonEl = document.getElementById("modalJson");

  function openModal(title, sub, imagesHtml, jsonText) {
    document.getElementById("modalTitle").textContent = title || "Reading";
    document.getElementById("modalSub").textContent = sub || "";
    document.getElementById("modalImages").innerHTML = imagesHtml || "";
    if (modalJsonEl) modalJsonEl.textContent = jsonText || "";
    if (jsonSection) jsonSection.style.display = (jsonText && String(jsonText).trim()) ? "" : "none";
    modal.classList.add("open");
  }

  function closeModal() {
    modal.classList.remove("open");
  }

  closeBtn?.addEventListener("click", closeModal);
  modal?.addEventListener("click", (e) => {
    if (e.target === modal) closeModal();
  });

  //  Click VALUE -> opens debug + JSON
  document.querySelectorAll(".js-open-reading").forEach(btn => {
    btn.addEventListener("click", () => {
      const tr = btn.closest("tr");
      if (!tr) return;

      const filename = tr.dataset.filename || "";
      const ocrjson = tr.dataset.ocrjson || "";
      const yolo = tr.dataset.debugYolo || "";
      const crop = tr.dataset.debugCrop || "";
      const prep = tr.dataset.debugPrep || "";

      const imgs = [];
      if (filename) imgs.push({ label: "Uploaded", url: buildUploadPreviewUrl("/uploads/" + filename) || "/uploads/" + filename });
      if (yolo) imgs.push({ label: "YOLO", url: yolo });
      if (crop) imgs.push({ label: "Crop", url: crop });
      if (prep) imgs.push({ label: "Preprocess", url: prep });

      const imagesHtml = imgs.map(x => `
        <div class="imgcard">
          <div class="small-muted" style="margin-bottom:6px;">${x.label}</div>
          <img src="${x.url}" style="width:100%;border-radius:10px;border:1px solid #333;" />
        </div>
      `).join("");

      let pretty = ocrjson;
      try { pretty = JSON.stringify(JSON.parse(ocrjson), null, 2); } catch(e) {}

      openModal("Reading Details", "", imagesHtml, pretty);
    });
  });

  //  Click IMAGE -> opens uploaded image in SAME modal (not new tab)
  document.querySelectorAll(".js-open-image").forEach(btn => {
    btn.addEventListener("click", () => {
      const src = btn.getAttribute("data-src");
      if (!src) return;

      const imagesHtml = renderUploadImageCard({
        src,
        title: "Uploaded Image",
        alt: "Uploaded Image",
        eager: true,
      });
      openModal("Uploaded Image", "", imagesHtml, "");
    });
  });
}

function setupChatPopup() {
  const openBtn = document.getElementById("openChatPopupBtn");
  const modal = document.getElementById("chatPopup");
  const closeBtn = document.getElementById("chatCloseBtn");
  const meta = document.getElementById("chatMeta");
  if (!openBtn || !modal || !meta) return;

  const state = {
    me: {
      id: Number(meta.dataset.userId || "0"),
      role: meta.dataset.role || "",
      team: meta.dataset.team || "",
    },
    users: [],
    conversations: [],
    userRows: [],
    activeConversationId: null,
    activePeerUserId: null,
    showArchived: false,
    isOpen: false,
    pageScrollY: 0,
    pollTimer: null,
    typingTimer: null,
    typingStopTimer: null,
  };

  const q = (id) => document.getElementById(id);
  const listEl = q("chatConversations");
  const msgEl = q("chatMessages");
  const threadTitleEl = q("chatThreadTitle");
  const threadMetaEl = q("chatThreadMeta");
  const threadAvatarEl = q("chatThreadAvatar");
  const backBtn = q("chatBackBtn");
  const archivedLabelEl = q("chatArchivedLabel");
  const toggleArchivedBtn = q("chatToggleArchivedBtn");
  const presenceEl = q("chatPresenceLine");
  const searchEl = q("chatSearchInput");
  const composerEl = q("chatComposerInput");
  const sendBtn = q("chatSendBtn");
  const newDirectBtn = q("chatNewDirectBtn");
  const newGroupBtn = q("chatNewGroupBtn");
  const isMobileChatViewport = () => window.matchMedia("(max-width: 900px)").matches;
  let lastChatVh = 0;

  function lockBodyScroll() {
    document.body.classList.add("chat-open-lock");
    if (!isMobileChatViewport()) return;
    state.pageScrollY = window.scrollY || window.pageYOffset || 0;
    document.body.style.position = "fixed";
    document.body.style.top = `-${state.pageScrollY}px`;
    document.body.style.left = "0";
    document.body.style.right = "0";
    document.body.style.width = "100%";
  }

  function unlockBodyScroll() {
    if (isMobileChatViewport()) {
      const y = Number(state.pageScrollY || 0);
      document.body.style.position = "";
      document.body.style.top = "";
      document.body.style.left = "";
      document.body.style.right = "";
      document.body.style.width = "";
      window.scrollTo(0, y);
    }
    document.body.classList.remove("chat-open-lock");
  }

  function syncChatViewportHeight() {
    const vv = window.visualViewport;
    const h = vv && vv.height ? vv.height : window.innerHeight;
    const safe = Math.max(320, Math.round(h));
    if (Math.abs(safe - lastChatVh) < 2) return;
    lastChatVh = safe;
    document.documentElement.style.setProperty("--chat-vh", `${safe}px`);
  }

  const api = async (url, opts = {}) => {
    const headers = {"Content-Type": "application/json"};
    const csrf = getCsrfToken();
    if (csrf) headers["X-CSRF-Token"] = csrf;
    const res = await fetch(url, {
      method: opts.method || "GET",
      headers,
      body: opts.body ? JSON.stringify(opts.body) : undefined,
      cache: "no-store",
    });
    let data = {};
    try { data = await res.json(); } catch {}
    if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
    return data;
  };

  function openPopup() {
    if (state.isOpen) return;
    state.isOpen = true;
    modal.classList.add("open");
    modal.setAttribute("aria-hidden", "false");
    modal.classList.remove("mobile-thread-active");
    lockBodyScroll();
    syncChatViewportHeight();
    refreshAll();
    if (state.pollTimer) clearInterval(state.pollTimer);
    state.pollTimer = setInterval(refreshAll, 4000);
  }

  function closePopup() {
    if (!state.isOpen) return;
    state.isOpen = false;
    modal.classList.remove("open");
    modal.setAttribute("aria-hidden", "true");
    modal.classList.remove("mobile-thread-active");
    unlockBodyScroll();
    if (state.pollTimer) clearInterval(state.pollTimer);
    state.pollTimer = null;
  }

  // Expose a safe global fallback so button always works even if event wiring is interrupted.
  window.__openChatPopup = openPopup;

  function relativeTime(ts) {
    if (!ts) return "";
    const d = new Date(ts.replace(" ", "T"));
    if (Number.isNaN(d.getTime())) return ts;
    const diff = Math.floor((Date.now() - d.getTime()) / 1000);
    if (diff < 60) return "now";
    if (diff < 3600) return `${Math.floor(diff / 60)}m`;
    if (diff < 86400) return `${Math.floor(diff / 3600)}h`;
    return `${Math.floor(diff / 86400)}d`;
  }

  function renderConversations() {
    if (!listEl) return;
    const term = (searchEl?.value || "").trim().toLowerCase();
    const allRows = state.userRows || [];
    const visible = allRows.filter((r) => {
      if (!term) return true;
      const name = String(r.display_name || "").toLowerCase();
      const preview = String(r.last_message_body || "").toLowerCase();
      return name.includes(term) || preview.includes(term);
    });
    const archivedCount = allRows.filter((r) => Number(r.is_archived || 0) === 1).length;
    if (archivedLabelEl) archivedLabelEl.textContent = `Archived messages (${archivedCount})`;
    if (toggleArchivedBtn) toggleArchivedBtn.textContent = state.showArchived ? "Show inbox" : "View all";

    if (!visible.length) {
      listEl.innerHTML = `<div class="small-muted">No chats yet.</div>`;
      return;
    }
    listEl.innerHTML = visible.map(c => {
      const hasActiveConversation = Number(state.activeConversationId || 0) > 0;
      const active = hasActiveConversation
        ? Number(c.id || 0) === Number(state.activeConversationId || 0)
        : Number(c.peer_user_id || 0) === Number(state.activePeerUserId || 0);
      const typing = (c.typing_user_ids || []).length ? "typing..." : "";
      const preview = typing || c.last_message_body || "No chat yet";
      const unread = Number(c.unread_count || 0);
      const unreadBadge = unread > 0 ? `<span class="chat-unread-badge">${unread}</span>` : "";
      const name = c.display_name || "Chat";
      const initial = (name || "C").trim().charAt(0).toUpperCase();
      const statusText = Number(c.id || 0) > 0 ? "Existing Chat" : "No Chat Yet";
      const previewClass = typing ? "chat-conv-preview is-typing" : "chat-conv-preview";
      return `
        <div class="chat-conv-item ${active ? "active" : ""}" data-conv-id="${c.id || 0}" data-peer-user-id="${c.peer_user_id || 0}">
          <div class="chat-conv-avatar">${initial}</div>
          <div class="chat-conv-main">
            <div class="chat-conv-top">
              <div class="chat-conv-name">${escapeHtml(name)}</div>
              <div class="chat-conv-time">${escapeHtml(relativeTime(c.last_message_at || c.updated_at))}</div>
            </div>
            <div class="chat-conv-bottom">
              <div class="${previewClass}">${escapeHtml(preview)}</div>
              ${unreadBadge}
            </div>
            <div class="chat-conv-state">${escapeHtml(statusText)}</div>
          </div>
        </div>
      `;
    }).join("");
  }

  function renderMessages(messages, members, typingUserIds) {
    const memberMap = {};
    (members || []).forEach(m => { memberMap[m.user_id] = m.display_name || m.username; });
    if (threadMetaEl) {
      const typingNames = (typingUserIds || []).map(uid => memberMap[uid]).filter(Boolean);
      threadMetaEl.textContent = typingNames.length ? `${typingNames.join(", ")} typing...` : "";
    }
    if (!msgEl) return;
    if (!messages || !messages.length) {
      msgEl.innerHTML = `
        <div class="chat-empty-state">
          <div class="chat-empty-glyph">...</div>
          <div class="chat-empty-title">No messages yet</div>
          <div class="chat-empty-copy">Start the conversation from the message box below.</div>
        </div>
      `;
      return;
    }
    msgEl.innerHTML = messages.map(m => {
      const mine = Number(m.sender_user_id) === Number(state.me.id);
      const body = m.is_deleted ? "This message was deleted" : (m.body || "");
      const edited = Number(m.is_edited || 0) ? " • edited" : "";
      const sender = mine ? "You" : (m.sender_username || "User");
      return `
        <div class="chat-msg ${mine ? "mine" : ""}" data-mid="${m.id}">
          <div class="chat-msg-meta">${escapeHtml(sender)} • ${escapeHtml(m.created_at)}${escapeHtml(edited)}</div>
          <div class="chat-msg-body">${escapeHtml(body)}</div>
        </div>
      `;
    }).join("");
    msgEl.scrollTop = msgEl.scrollHeight;
  }

  function buildUserRows() {
    const convs = state.conversations || [];
    const byPeer = new Map();
    convs.forEach((c) => {
      const peerId = Number(c.direct_peer_user_id || 0);
      if (!peerId) return;
      byPeer.set(peerId, c);
    });
    const rows = (state.users || []).filter((u) => Number(u.id || 0) !== Number(state.me.id || 0)).map((u) => {
      const peerId = Number(u.id || 0);
      const c = byPeer.get(peerId);
      if (!c) {
        return {
          id: 0,
          peer_user_id: peerId,
          display_name: u.display_name || u.username,
          last_message_body: "",
          last_message_at: "",
          unread_count: 0,
          is_archived: 0,
          typing_user_ids: [],
        };
      }
      return {
        ...c,
        peer_user_id: peerId,
        display_name: c.display_name || u.display_name || u.username,
      };
    });
    rows.sort((a, b) => {
      const aHas = Number(a.id || 0) > 0 ? 1 : 0;
      const bHas = Number(b.id || 0) > 0 ? 1 : 0;
      if (aHas !== bHas) return bHas - aHas;
      const at = Date.parse(String(a.last_message_at || a.updated_at || "").replace(" ", "T")) || 0;
      const bt = Date.parse(String(b.last_message_at || b.updated_at || "").replace(" ", "T")) || 0;
      if (at !== bt) return bt - at;
      return String(a.display_name || "").localeCompare(String(b.display_name || ""));
    });
    state.userRows = rows;
  }

  async function fetchBootstrap() {
    try {
      const data = await api("/api/chat/bootstrap");
      state.users = data.users || [];
      if (presenceEl) {
        const meName = data.me.display_name || data.me.username;
        presenceEl.textContent = `Signed in as ${meName} (${String(data.me.role || "").toUpperCase()})`;
      }
    } catch (e) {
      if (presenceEl) presenceEl.textContent = String(e.message || "Failed to load chat users");
    }
  }

  async function fetchConversations() {
    const data = await api(`/api/chat/conversations?search=${encodeURIComponent("")}`);
    state.conversations = data.conversations || [];
    buildUserRows();
    if (!state.activeConversationId && !state.activePeerUserId && state.userRows.length) {
      const first = state.userRows[0];
      state.activeConversationId = Number(first.id || 0) || null;
      state.activePeerUserId = Number(first.peer_user_id || 0) || null;
    }
    if (state.activeConversationId && !state.conversations.find(c => Number(c.id) === Number(state.activeConversationId))) {
      state.activeConversationId = null;
    }
    renderConversations();
  }

  async function fetchMessages() {
    const convId = Number(state.activeConversationId || 0);
    if (!convId && state.activePeerUserId) {
      const u = (state.users || []).find((x) => Number(x.id) === Number(state.activePeerUserId));
      const nm = (u && (u.display_name || u.username)) || "Conversation";
      if (threadTitleEl) threadTitleEl.textContent = nm;
      if (threadAvatarEl) threadAvatarEl.textContent = nm.trim().charAt(0).toUpperCase();
      if (threadMetaEl) threadMetaEl.textContent = "No chat yet";
      if (msgEl) {
        msgEl.innerHTML = `
          <div class="chat-empty-state">
            <div class="chat-empty-glyph">+</div>
            <div class="chat-empty-title">No messages yet</div>
            <div class="chat-empty-copy">Send the first message to start this conversation.</div>
          </div>
        `;
      }
      return;
    }
    if (!convId) {
      if (threadTitleEl) threadTitleEl.textContent = "Select a conversation";
      if (msgEl) {
        msgEl.innerHTML = `
          <div class="chat-empty-state">
            <div class="chat-empty-glyph">></div>
            <div class="chat-empty-title">Pick a conversation</div>
            <div class="chat-empty-copy">Select any member from the left panel to open the thread.</div>
          </div>
        `;
      }
      return;
    }
    const conv = state.conversations.find(c => Number(c.id) === convId);
    const threadName = (conv && conv.display_name) || "Conversation";
    if (threadTitleEl) threadTitleEl.textContent = threadName;
    if (threadAvatarEl) threadAvatarEl.textContent = (threadName || "C").trim().charAt(0).toUpperCase();
    const data = await api(`/api/chat/conversations/${convId}/messages?limit=60`);
    renderMessages(data.messages || [], data.members || [], data.typing_user_ids || []);
    for (const m of (data.messages || [])) {
      if (Number(m.sender_user_id) !== Number(state.me.id)) {
        api(`/api/chat/messages/${m.id}/read`, {method: "POST"}).catch(() => {});
      }
    }
  }

  async function refreshAll() {
    try {
      await fetchBootstrap();
      await fetchConversations();
      await fetchMessages();
    } catch (e) {
      if (msgEl) msgEl.innerHTML = `<div class="toast error">${String(e.message || e)}</div>`;
    }
  }

  async function sendMessage() {
    const body = (composerEl?.value || "").trim();
    if (!body) return;
    if (!state.activeConversationId && state.activePeerUserId) {
      const created = await api("/api/chat/conversations", {
        method: "POST",
        body: { type: "direct", dm_user_id: Number(state.activePeerUserId) },
      });
      state.activeConversationId = Number(created.conversation_id || 0) || null;
    }
    if (!state.activeConversationId) return;
    const clientMsgId = `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
    await api("/api/chat/messages", {
      method: "POST",
      body: {conversation_id: Number(state.activeConversationId), body, client_msg_id: clientMsgId, message_type: "text"},
    });
    composerEl.value = "";
    syncComposerHeight();
    await fetchConversations();
    await fetchMessages();
  }

  function syncComposerHeight() {
    if (!composerEl) return;
    const minPx = 54;
    const maxPx = 140;
    composerEl.style.height = "auto";
    const next = Math.max(minPx, Math.min(maxPx, composerEl.scrollHeight));
    composerEl.style.height = `${next}px`;
  }

  function keepChatBottomVisible() {
    if (!isMobileChatViewport()) return;
    if (!modal.classList.contains("open")) return;
    requestAnimationFrame(() => {
      if (msgEl) msgEl.scrollTop = msgEl.scrollHeight;
    });
  }

  async function createDirect() {
    if (!state.users.length) await fetchBootstrap();
    const options = state.users.map(u => `${u.display_name || u.username} (${u.role}${u.team ? ` T${u.team}` : ""})`).join("\n");
    const chosen = window.prompt(`Start direct chat with name:\n${options}`);
    if (!chosen) return;
    const target = state.users.find(u => String(u.display_name || u.username).toLowerCase() === chosen.trim().toLowerCase());
    if (!target) {
      window.alert("User not found in allowed chat list.");
      return;
    }
    const data = await api("/api/chat/conversations", {
      method: "POST",
      body: {type: "direct", dm_user_id: Number(target.id)},
    });
    state.activeConversationId = Number(data.conversation_id);
    await refreshAll();
  }

  async function createGroup() {
    if (!state.users.length) await fetchBootstrap();
    const title = (window.prompt("Enter group name:") || "").trim();
    if (!title) return;
    const pick = window.prompt("Enter comma-separated names to add:");
    if (!pick) return;
    const names = pick.split(",").map(x => x.trim().toLowerCase()).filter(Boolean);
    const ids = state.users
      .filter(u => names.includes(String(u.display_name || u.username).toLowerCase()))
      .map(u => Number(u.id));
    if (!ids.length) {
      window.alert("No valid users selected.");
      return;
    }
    const data = await api("/api/chat/conversations", {
      method: "POST",
      body: {type: "group", title, member_ids: ids},
    });
    state.activeConversationId = Number(data.conversation_id);
    await refreshAll();
  }

  listEl?.addEventListener("click", async (e) => {
    const item = e.target.closest(".chat-conv-item");
    if (!item) return;
    const convId = Number(item.dataset.convId || "0");
    const peerUserId = Number(item.dataset.peerUserId || "0");
    const selected = (state.userRows || []).find((r) => (
      (convId > 0 && Number(r.id || 0) === convId) ||
      (peerUserId > 0 && Number(r.peer_user_id || 0) === peerUserId)
    ));
    if (selected) {
      const nm = selected.display_name || "Conversation";
      if (threadTitleEl) threadTitleEl.textContent = nm;
      if (threadAvatarEl) threadAvatarEl.textContent = nm.trim().charAt(0).toUpperCase();
      if (threadMetaEl) threadMetaEl.textContent = Number(selected.id || 0) > 0 ? "Existing Chat" : "No chat yet";
    }
    state.activeConversationId = convId > 0 ? convId : null;
    state.activePeerUserId = peerUserId > 0 ? peerUserId : null;
    renderConversations();
    modal.classList.add("mobile-thread-active");
    await fetchMessages();
    if (isMobileChatViewport()) {
      setTimeout(() => composerEl?.focus(), 120);
    }
  });

  toggleArchivedBtn?.addEventListener("click", () => {
    state.showArchived = !state.showArchived;
    renderConversations();
  });

  backBtn?.addEventListener("click", () => {
    modal.classList.remove("mobile-thread-active");
  });

  sendBtn?.addEventListener("click", () => { sendMessage().catch(() => {}); });
  composerEl?.addEventListener("keydown", (e) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      sendMessage().catch(() => {});
      return;
    }
    if (state.typingTimer) clearTimeout(state.typingTimer);
    if (state.typingStopTimer) clearTimeout(state.typingStopTimer);
    if (state.activeConversationId) {
      api(`/api/chat/conversations/${state.activeConversationId}/typing`, {
        method: "POST",
        body: {is_typing: true},
      }).catch(() => {});
      state.typingStopTimer = setTimeout(() => {
        api(`/api/chat/conversations/${state.activeConversationId}/typing`, {
          method: "POST",
          body: {is_typing: false},
        }).catch(() => {});
      }, 1200);
    }
  });
  composerEl?.addEventListener("input", syncComposerHeight);
  composerEl?.addEventListener("focus", () => {
    syncChatViewportHeight();
    keepChatBottomVisible();
    modal.classList.add("mobile-thread-active");
  });
  composerEl?.addEventListener("input", keepChatBottomVisible);

  searchEl?.addEventListener("input", () => {
    renderConversations();
  });

  newDirectBtn?.addEventListener("click", () => { createDirect().catch(err => alert(err.message || "Failed")); });
  newGroupBtn?.addEventListener("click", () => { createGroup().catch(err => alert(err.message || "Failed")); });

  // Per-message moderation controls intentionally removed from normal chat UI.

  closeBtn?.addEventListener("click", closePopup);
  modal.addEventListener("click", (e) => {
    if (e.target === modal) closePopup();
  });

  // open is handled by inline onclick -> window.__openChatPopup fallback

  window.addEventListener("resize", syncChatViewportHeight);
  window.addEventListener("orientationchange", syncChatViewportHeight);
  if (window.visualViewport) {
    window.visualViewport.addEventListener("resize", syncChatViewportHeight);
  }

  syncChatViewportHeight();
  syncComposerHeight();
}
document.addEventListener("DOMContentLoaded", () => {
  wireModal();
  setupChatPopup();
  setupAlertsBellModal();
  setupTabs();
  enhanceFormSubmits();
  setupToastLifecycle();
  setupPWAInstall();
  registerServiceWorker();
  // only if live alerts container exists
  const list = qs("#alertsLive");
  if (list) {
    pollAlerts();
    setInterval(pollAlerts, 10000);
  }
  pollLatestReadings();
  setInterval(pollLatestReadings, 5000);
});

document.addEventListener("submit", (e) => {
  const form = e.target;
  if (!(form instanceof HTMLFormElement)) return;
  ensureCsrfInput(form);
});
