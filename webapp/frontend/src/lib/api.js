const API_BASE = import.meta.env.VITE_API_URL || "";

async function fetchJSON(path) {
  const res = await fetch(`${API_BASE}${path}`, { credentials: "include" });
  if (!res.ok) throw new Error(`API error: ${res.status}`);
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
  summary: (days = 365) => fetchJSON(`/api/summary?days=${days}`),
  daily: (days = 365, granularity = "daily") =>
    fetchJSON(`/api/daily?days=${days}&granularity=${granularity}`),
  products: (days = 365) => fetchJSON(`/api/products?days=${days}`),
  inventory: () => fetchJSON(`/api/inventory`),
  productDetail: (asin, days = 365) =>
    fetchJSON(`/api/product/${asin}?days=${days}`),
  pnl: (days = 365) => fetchJSON(`/api/pnl?days=${days}`),

  // Dashboard analytics
  comparison: (view = "realtime") => fetchJSON(`/api/comparison?view=${view}`),
  monthlyYoY: () => fetchJSON(`/api/monthly-yoy`),
  productMix: (days = 365) => fetchJSON(`/api/product-mix?days=${days}`),
  colorMix: (days = 365) => fetchJSON(`/api/color-mix?days=${days}`),

  // Profitability (Sellerboard-style)
  profitability: (view = "realtime") => fetchJSON(`/api/profitability?view=${view}`),
  profitabilityItems: (days = 365) => fetchJSON(`/api/profitability/items?days=${days}`),

  // Advertising endpoints
  adsSummary: (days = 30) => fetchJSON(`/api/ads/summary?days=${days}`),
  adsDaily: (days = 30) => fetchJSON(`/api/ads/daily?days=${days}`),
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
    return fetch(`${API_BASE}/api/logistics/upload`, { method: "POST", body: fd, credentials: "include" })
      .then(async r => {
        const text = await r.text();
        try { var data = JSON.parse(text); } catch { throw new Error(text || `Server error ${r.status}`); }
        if (!r.ok) throw new Error(data.detail || data.error || `Upload failed (${r.status})`);
        return data;
      });
  },

  // Combined Supply Chain Upload (Factory PO + Logistics in one file)
  uploadSupplyChain: (file) => {
    const fd = new FormData();
    fd.append("file", file);
    return fetch(`${API_BASE}/api/supply-chain/upload`, { method: "POST", body: fd, credentials: "include" })
      .then(async r => {
        const text = await r.text();
        try { var data = JSON.parse(text); } catch { throw new Error(text || `Server error ${r.status}`); }
        if (!r.ok) throw new Error(data.detail || data.error || `Upload failed (${r.status})`);
        return data;
      });
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
