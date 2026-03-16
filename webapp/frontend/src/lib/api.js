const API_BASE = import.meta.env.VITE_API_URL || "";

// Build hierarchy query string fragment from {division, customer, platform, marketplace}
function _hq(h = {}) {
  let q = "";
  if (h.division) q += `&division=${encodeURIComponent(h.division)}`;
  if (h.customer) q += `&customer=${encodeURIComponent(h.customer)}`;
  if (h.platform) q += `&platform=${encodeURIComponent(h.platform)}`;
  if (h.marketplace) q += `&marketplace=${encodeURIComponent(h.marketplace)}`;
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
  inventoryCommandCenter: (h = {}) => fetchJSON(`/api/inventory/command-center${_hq(h) ? '?' + _hq(h).slice(1) : ''}`),
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
  profitabilityOverview: (days = 30, h = {}) => fetchJSON(`/api/profitability/overview?days=${days}${_hq(h)}`),
  profitabilityFeeDetail: (days = 30, h = {}) => fetchJSON(`/api/profitability/fee-detail?days=${days}${_hq(h)}`),
  profitabilityAur: (days = 56, h = {}) => fetchJSON(`/api/profitability/aur?days=${days}${_hq(h)}`),

  // Amazon Live Pricing + Coupons (read-only from SP-API cache)
  amazonPricing: () => fetchJSON(`/api/profitability/amazon-pricing`),

  // Sale Prices CRUD
  salePrices: (h = {}) => fetchJSON(`/api/profitability/sale-prices${_hq(h) ? '?' + _hq(h).slice(1) : ''}`),
  createSalePrice: (data) =>
    fetch(`${API_BASE}/api/profitability/sale-prices`, {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify(data), credentials: "include",
    }).then(r => r.json()),
  updateSalePrice: (id, data) =>
    fetch(`${API_BASE}/api/profitability/sale-prices/${id}`, {
      method: "PUT", headers: { "Content-Type": "application/json" },
      body: JSON.stringify(data), credentials: "include",
    }).then(r => r.json()),
  deleteSalePrice: (id) =>
    fetch(`${API_BASE}/api/profitability/sale-prices/${id}`, {
      method: "DELETE", credentials: "include",
    }).then(r => r.json()),
  pushPrice: (id) =>
    fetch(`${API_BASE}/api/profitability/push-price/${id}`, {
      method: "POST", credentials: "include",
    }).then(r => r.json()),

  // Coupons CRUD
  coupons: (h = {}) => fetchJSON(`/api/profitability/coupons${_hq(h) ? '?' + _hq(h).slice(1) : ''}`),
  createCoupon: (data) =>
    fetch(`${API_BASE}/api/profitability/coupons`, {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify(data), credentials: "include",
    }).then(r => r.json()),
  updateCoupon: (id, data) =>
    fetch(`${API_BASE}/api/profitability/coupons/${id}`, {
      method: "PUT", headers: { "Content-Type": "application/json" },
      body: JSON.stringify(data), credentials: "include",
    }).then(r => r.json()),
  deleteCoupon: (id) =>
    fetch(`${API_BASE}/api/profitability/coupons/${id}`, {
      method: "DELETE", credentials: "include",
    }).then(r => r.json()),

  // Advertising endpoints
  adsSummary: (days = 30, h = {}) => fetchJSON(`/api/ads/summary?days=${days}${_hq(h)}`),
  adsDaily: (days = 30, h = {}) => fetchJSON(`/api/ads/daily?days=${days}${_hq(h)}`),
  adsCampaigns: (days = 30, h = {}) => fetchJSON(`/api/ads/campaigns?days=${days}${_hq(h)}`),
  adsKeywords: (days = 30, limit = 50, h = {}) =>
    fetchJSON(`/api/ads/keywords?days=${days}&limit=${limit}${_hq(h)}`),
  adsSearchTerms: (days = 30, limit = 50, h = {}) =>
    fetchJSON(`/api/ads/search-terms?days=${days}&limit=${limit}${_hq(h)}`),
  adsNegativeKeywords: (h = {}) => fetchJSON(`/api/ads/negative-keywords${_hq(h) ? '?' + _hq(h).slice(1) : ''}`),

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

  // FBA Shipments (SP-API) — marketplace: "US" | "CA"
  fbaShipments: (refresh = false, marketplace = "US") => {
    const params = [];
    if (refresh) params.push("refresh=true");
    if (marketplace && marketplace !== "US") params.push(`marketplace=${marketplace}`);
    const qs = params.length ? `?${params.join("&")}` : "";
    return fetchJSON(`/api/fba-shipments${qs}`);
  },
  fbaShipmentsSync: (marketplace = "US") =>
    fetch(`${API_BASE}/api/fba-shipments/sync${marketplace && marketplace !== "US" ? `?marketplace=${marketplace}` : ""}`, { method: "POST", credentials: "include" }).then(r => r.json()),
  fbaShipmentItems: (shipmentId, marketplace = "US") =>
    fetchJSON(`/api/fba-shipments/${encodeURIComponent(shipmentId)}/items${marketplace && marketplace !== "US" ? `?marketplace=${marketplace}` : ""}`),
  fbaShipmentProducts: () => fetchJSON(`/api/fba-shipments/products`),
  fbaCreatePlan: (data) =>
    fetch(`${API_BASE}/api/fba-shipments/create-plan`, { method: "POST", credentials: "include", headers: { "Content-Type": "application/json" }, body: JSON.stringify(data) }).then(r => r.json()),
  fbaConfirmPlan: (data) =>
    fetch(`${API_BASE}/api/fba-shipments/confirm-plan`, { method: "POST", credentials: "include", headers: { "Content-Type": "application/json" }, body: JSON.stringify(data) }).then(r => r.json()),

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
  syncLog: (limit = 100) => fetchJSON(`/api/system/sync-log?limit=${limit}`),
  dataCoverage: () => fetchJSON(`/api/system/data-coverage`),
  systemStatus: () => fetchJSON(`/api/system/status`),
  triggerGapFill: () =>
    fetch(`${API_BASE}/api/sync/gap-fill`, { method: "POST", credentials: "include" }).then(r => r.json()),

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

  // Backup Verification & DR
  backupVerificationStatus: () => fetchJSON(`/api/backup/verification-status`),
  triggerBackupVerification: () =>
    fetch(`${API_BASE}/api/backup/verify`, { method: "POST", credentials: "include" }).then(r => {
      if (!r.ok) throw new Error(`Verification failed: ${r.status}`);
      return r.json();
    }),
  drStatus: () => fetchJSON(`/api/backup/dr-status`),

  // Retail Reporting
  retailSummary: (h = {}) => fetchJSON(`/api/retail/summary${_hq(h) ? '?' + _hq(h).slice(1) : ''}`),
  retailScorecard: (h = {}) => fetchJSON(`/api/retail/scorecard${_hq(h) ? '?' + _hq(h).slice(1) : ''}`),
  retailStorePerformance: (h = {}, week = "", limit = 100, offset = 0, sortBy = "pos_sales_ty", sortDir = "desc") =>
    fetchJSON(`/api/retail/store-performance?limit=${limit}&offset=${offset}&sort_by=${sortBy}&sort_dir=${sortDir}${week ? '&week=' + week : ''}${_hq(h)}`),
  retailItemPerformance: (h = {}, periodType = "weekly", week = "", limit = 100, offset = 0) =>
    fetchJSON(`/api/retail/item-performance?period_type=${periodType}&limit=${limit}&offset=${offset}${week ? '&week=' + week : ''}${_hq(h)}`),
  retailEcomm: (h = {}, week = "", limit = 100, offset = 0) =>
    fetchJSON(`/api/retail/ecomm?limit=${limit}&offset=${offset}${week ? '&week=' + week : ''}${_hq(h)}`),
  retailOrderForecast: (h = {}) => fetchJSON(`/api/retail/order-forecast${_hq(h) ? '?' + _hq(h).slice(1) : ''}`),
  walmartAnalytics: (h = {}) => fetchJSON('/api/retail/walmart-analytics' + (_hq(h) ? '?' + _hq(h).slice(1) : '')),

  // Walmart modular endpoints (new architecture)
  walmartAvailability: (h = {}) => fetchJSON('/api/walmart/availability' + (_hq(h) ? '?' + _hq(h).slice(1) : '')),
  walmartSales: (h = {}) => fetchJSON('/api/walmart/sales' + (_hq(h) ? '?' + _hq(h).slice(1) : '')),
  walmartInventory: (h = {}) => fetchJSON('/api/walmart/inventory' + (_hq(h) ? '?' + _hq(h).slice(1) : '')),
  walmartScorecard: (h = {}) => fetchJSON('/api/walmart/scorecard' + (_hq(h) ? '?' + _hq(h).slice(1) : '')),
  walmartEcommerce: (h = {}) => fetchJSON('/api/walmart/ecommerce' + (_hq(h) ? '?' + _hq(h).slice(1) : '')),
  walmartForecast: (h = {}) => fetchJSON('/api/walmart/forecast' + (_hq(h) ? '?' + _hq(h).slice(1) : '')),
  walmartWeeklyTrend: (h = {}) => fetchJSON('/api/walmart/weekly-trend' + (_hq(h) ? '?' + _hq(h).slice(1) : '')),
  walmartStoreAnalytics: (h = {}, opts = {}) => {
    const { week, sortBy, sortDir, limit, offset } = opts;
    let q = _hq(h) ? '?' + _hq(h).slice(1) : '?';
    if (week) q += `&week=${week}`;
    if (sortBy) q += `&sort_by=${sortBy}`;
    if (sortDir) q += `&sort_dir=${sortDir}`;
    if (limit) q += `&limit=${limit}`;
    if (offset) q += `&offset=${offset}`;
    return fetchJSON('/api/walmart/store-analytics' + q);
  },

  retailUpload: (file) => {
    const fd = new FormData();
    fd.append("file", file);
    return fetch(`${API_BASE}/api/retail/upload`, {
      method: "POST", credentials: "include", body: fd,
    }).then(r => { if (!r.ok) throw new Error(`Upload failed: ${r.status}`); return r.json(); });
  },
};

export function fmt$(n) {
  if (n == null) return "$0";
  return "$" + Math.round(n).toLocaleString();
}

export function fmtPct(n) {
  if (n == null) return "0%";
  return n.toFixed(1) + "%";
}
