const API_BASE = import.meta.env.VITE_API_URL || "";

async function fetchJSON(path) {
  const res = await fetch(`${API_BASE}${path}`, { credentials: "include" });
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

export const api = {
  // Auth
  login: (password) =>
    fetch(`${API_BASE}/api/auth/login`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ password }),
      credentials: "include",
    }).then(r => { if (!r.ok) throw new Error("Login failed"); return r.json(); }),
  authCheck: () => fetchJSON("/api/auth/check"),
  logout: () =>
    fetch(`${API_BASE}/api/auth/logout`, { method: "POST", credentials: "include" }).then(r => r.json()),

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
};

export function fmt$(n) {
  if (n == null) return "$0";
  return "$" + Math.round(n).toLocaleString();
}

export function fmtPct(n) {
  if (n == null) return "0%";
  return n.toFixed(1) + "%";
}
