const API_BASE = import.meta.env.VITE_API_URL || "";

// Build hierarchy query string fragment from {division, customer, platform}
function _hq(h = {}) {
  let q = "";
  if (h.division) q += `&division=${encodeURIComponent(h.division)}`;
  if (h.customer) q += `&customer=${encodeURIComponent(h.customer)}`;
  if (h.platform) q += `&platform=${encodeURIComponent(h.platform)}`;
  return q;
}

async function fetchJSON(path) {
  const res = await fetch(`${API_BASE}${path}`, { credentials: "include" });
  if (!res.ok) {
    if (res.status === 403) {
      try {
        const data = await res.json();
        if (data.mfa_required) {
          window.dispatchEvent(new CustomEvent("mfa-required"));
          throw new Error("MFA verification required");
        }
      } catch (e) {
        if (e.message === "MFA verification required") throw e;
      }
    }
    throw new Error(`API error: ${res.status}`);
  }
  return res.json();
}

export const api = {
  // Auth
  login: (email, password) =>
    fetch(`${API_BASE}/api/auth/login`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ email, password }),
      credentials: "include",
    }).then(r => { if (!r.ok) throw new Error("Login failed"); return r.json(); }),
  authCheck: () => fetchJSON("/api/auth/check"),
  logout: () =>
    fetch(`${API_BASE}/api/auth/logout`, { method: "POST", credentials: "include" }).then(r => r.json()),
  me: () => fetchJSON("/api/me"),
  myPermissions: () => fetchJSON("/api/permissions/me"),
  allPermissions: () => fetchJSON("/api/permissions"),
  updatePermission: (user, tab, enabled) =>
    fetch(`${API_BASE}/api/permissions`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ user, tab, enabled }),
      credentials: "include",
    }).then(r => { if (!r.ok) throw new Error("Permission update failed"); return r.json(); }),

  // Existing endpoints
  summary: (days = 365, h = {}) => fetchJSON(`/api/summary?days=${days}${_hq(h)}`),
  daily: (days = 365, granularity = "daily", h = {}) =>
    fetchJSON(`/api/daily?days=${days}&granularity=${granularity}${_hq(h)}`),
  products: (days = 365, h = {}) => fetchJSON(`/api/products?days=${days}${_hq(h)}`),
  inventory: (h = {}) => fetchJSON(`/api/inventory${_hq(h) ? '?' + _hq(h).slice(1) : ''}`),
  productDetail: (asin, days = 365) =>
    fetchJSON(`/api/product/${asin}?days=${days}`),
  pnl: (days = 365, h = {}) => fetchJSON(`/api/pnl?days=${days}${_hq(h)}`),

  // Dashboard analytics
  comparison: (view = "realtime", h = {}) => fetchJSON(`/api/comparison?view=${view}${_hq(h)}`),
  monthlyYoY: (h = {}) => fetchJSON(`/api/monthly-yoy${_hq(h) ? '?' + _hq(h).slice(1) : ''}`),
  productMix: (days = 365, h = {}) => fetchJSON(`/api/product-mix?days=${days}${_hq(h)}`),
  colorMix: (days = 365, h = {}) => fetchJSON(`/api/color-mix?days=${days}${_hq(h)}`),

  // Profitability (Sellerboard-style)
  profitability: (view = "realtime", h = {}) => fetchJSON(`/api/profitability?view=${view}${_hq(h)}`),
  profitabilityItems: (days = 365, h = {}) => fetchJSON(`/api/profitability/items?days=${days}${_hq(h)}`),

  // Advertising endpoints
  adsSummary: (days = 30, h = {}) => fetchJSON(`/api/ads/summary?days=${days}${_hq(h)}`),
  adsDaily: (days = 30, h = {}) => fetchJSON(`/api/ads/daily?days=${days}${_hq(h)}`),
  adsCampaigns: (days = 30) => fetchJSON(`/api/ads/campaigns?days=${days}`),
  adsKeywords: (days = 30, limit = 50) =>
    fetchJSON(`/api/ads/keywords?days=${days}&limit=${limit}`),
  adsSearchTerms: (days = 30, limit = 50) =>
    fetchJSON(`/api/ads/search-terms?days=${days}&limit=${limit}`),
  adsNegativeKeywords: () => fetchJSON(`/api/ads/negative-keywords`),

  // Original Warehouse (Moose 3PL grouped)
  warehouse: () => fetchJSON(`/api/warehouse`),

  // Golf/Housewares Warehouse
  warehouseGolf: (channel) => fetchJSON(`/api/warehouse/golf${channel ? `?channel=${encodeURIComponent(channel)}` : ""}`),
  warehouseHousewares: () => fetchJSON(`/api/warehouse/housewares`),
  warehouseSummary: () => fetchJSON(`/api/warehouse/summary`),
  warehouseUnified: (division = "golf", channel = null) =>
    fetchJSON(`/api/warehouse/unified?division=${division}${channel && channel !== "All" ? `&channel=${encodeURIComponent(channel)}` : ""}`),

  // Upload metadata
  uploadMeta: () => fetchJSON(`/api/upload/metadata`),

  // Item Master
  itemMaster: () => fetchJSON(`/api/item-master`),
  updateItem: (asin, data) =>
    fetch(`${API_BASE}/api/item-master/${asin}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(data),
      credentials: "include",
    }).then(r => r.json()),
  itemMasterWalmart: () => fetchJSON(`/api/item-master/walmart`),
  updateWalmartItem: (golfgenItem, data) =>
    fetch(`${API_BASE}/api/item-master/walmart/${encodeURIComponent(golfgenItem)}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(data),
      credentials: "include",
    }).then(r => r.json()),
  pricingStatus: () => fetchJSON(`/api/pricing/status`),
  triggerPricingSync: () =>
    fetch(`${API_BASE}/api/pricing/sync`, { method: "POST", credentials: "include" }).then(r => r.json()),
  itemMasterAmazon: () => fetchJSON(`/api/item-master/amazon`),
  itemMasterOther: () => fetchJSON(`/api/item-master/other`),
  itemMasterHousewares: () => fetchJSON(`/api/item-master/housewares`),

  // Factory PO Summary
  factoryPO: () => fetchJSON(`/api/factory-po`),
  uploadFactoryPO: (file) => {
    const fd = new FormData();
    fd.append("file", file);
    return fetch(`${API_BASE}/api/factory-po/upload`, { method: "POST", body: fd, credentials: "include" }).then(r => r.json());
  },

  // Logistics Tracking
  logistics: () => fetchJSON(`/api/logistics`),
  uploadLogistics: (file) => {
    const fd = new FormData();
    fd.append("file", file);
    return fetch(`${API_BASE}/api/logistics/upload`, { method: "POST", body: fd, credentials: "include" }).then(r => r.json());
  },

  // Combined Supply Chain Upload (Factory PO + Logistics in one file)
  uploadSupplyChain: (file) => {
    const fd = new FormData();
    fd.append("file", file);
    return fetch(`${API_BASE}/api/supply-chain/upload`, { method: "POST", body: fd, credentials: "include" }).then(r => r.json());
  },

  // Supply Chain (unified PO / OTW / Invoice)
  supplyChainOTW: () => fetchJSON(`/api/supply-chain/otw`),
  supplyChainPO: () => fetchJSON(`/api/supply-chain/po`),
  supplyChainInvoices: () => fetchJSON(`/api/supply-chain/invoices`),
  uploadSupplyChainV2: (file) => {
    const fd = new FormData();
    fd.append("file", file);
    return fetch(`${API_BASE}/api/supply-chain/upload`, { method: "POST", body: fd, credentials: "include" }).then(r => r.json());
  },

  // FBA Shipments (SP-API)
  fbaShipments: (refresh = false) => fetchJSON(`/api/fba-shipments${refresh ? "?refresh=true" : ""}`),
  fbaShipmentsSync: () =>
    fetch(`${API_BASE}/api/fba-shipments/sync`, { method: "POST", credentials: "include" }).then(r => r.json()),
  fbaShipmentItems: (shipmentId) => fetchJSON(`/api/fba-shipments/${encodeURIComponent(shipmentId)}/items`),

  // Item Planning (legacy)
  itemPlanning: () => fetchJSON(`/api/item-planning`),
  itemPlanningRawSales: () => fetchJSON(`/api/item-planning/raw-product-sales`),
  itemPlanningRawDaily: () => fetchJSON(`/api/item-planning/raw-daily-data`),
  itemPlanningOverride: (sku, field, values) =>
    fetch(`${API_BASE}/api/item-planning/override`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ sku, field, values }),
      credentials: "include",
    }).then(r => r.json()),
  uploadItemPlanning: (file) => {
    const fd = new FormData();
    fd.append("file", file);
    return fetch(`${API_BASE}/api/item-planning/upload`, { method: "POST", body: fd, credentials: "include" }).then(r => r.json());
  },

  // New Item Plan (FY2026 planning module)
  itemPlan: () => fetchJSON(`/api/item-plan`),
  itemPlanOverride: (sku, field, month, value) =>
    fetch(`${API_BASE}/api/item-plan/override`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ sku, field, month, value }),
      credentials: "include",
    }).then(r => { if (!r.ok) throw new Error("Override failed"); return r.json(); }),
  itemPlanCurve: (sku, curve_type) =>
    fetch(`${API_BASE}/api/item-plan/curve`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ sku, curve_type }),
      credentials: "include",
    }).then(r => { if (!r.ok) throw new Error("Curve update failed"); return r.json(); }),
  itemPlanSalesCurves: () => fetchJSON(`/api/item-plan/sales-curves`),
  factoryOnOrder: () => fetchJSON(`/api/factory-on-order`),
  updateFactoryOnOrder: (data) =>
    fetch(`${API_BASE}/api/factory-on-order`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(data),
      credentials: "include",
    }).then(r => r.json()),
  // Inventory Excel upload (GolfGenInventory page)
  uploadInventoryExcel: (file, division) => {
    const fd = new FormData();
    fd.append("file", file);
    return fetch(`${API_BASE}/api/upload/inventory-excel?division=${division}`, {
      method: "POST", body: fd, credentials: "include",
    }).then(r => r.json().then(data => ({ ok: r.ok, data })));
  },

  // Warehouse Excel upload
  uploadWarehouseExcel: (file) => {
    const fd = new FormData();
    fd.append("file", file);
    return fetch(`${API_BASE}/api/upload/warehouse-excel`, {
      method: "POST", body: fd, credentials: "include",
    }).then(r => r.json().then(data => ({ ok: r.ok, data })));
  },

  dashboardSettings: () => fetchJSON(`/api/dashboard-settings`),
  updateDashboardSetting: (key, value) =>
    fetch(`${API_BASE}/api/dashboard-settings`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ key, value: String(value) }),
      credentials: "include",
    }).then(r => r.json()),

  // System
  health: () => fetchJSON(`/api/health`),
  syncLog: (limit = 50) => fetchJSON(`/api/system/sync-log?limit=${limit}`),
  systemStatus: () => fetchJSON(`/api/system/status`),

  // Backup
  backupStatus: () => fetchJSON(`/api/backup/status`),
  triggerBackup: () =>
    fetch(`${API_BASE}/api/backup/trigger`, { method: "POST", credentials: "include" }).then(r => {
      if (!r.ok) throw new Error(`Backup failed: ${r.status}`);
      return r.json();
    }),

  // GitHub Backup
  githubBackupStatus: () => fetchJSON(`/api/backup/github-status`),
  triggerGithubBackup: () =>
    fetch(`${API_BASE}/api/backup/github-trigger`, { method: "POST", credentials: "include" }).then(r => {
      if (!r.ok) throw new Error(`GitHub backup failed: ${r.status}`);
      return r.json();
    }),
};

export function fmt$(n) {
  if (n == null) return "$0";
  return "$" + Math.round(n).toLocaleString();
}

export function fmtPct(n) {
  if (n == null) return "0%";
  return n.toFixed(1) + "%";
}
