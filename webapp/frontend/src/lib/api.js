const API_BASE = import.meta.env.VITE_API_URL || "";

async function fetchJSON(path) {
  const res = await fetch(`${API_BASE}${path}`);
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

export const api = {
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
};

export function fmt$(n) {
  if (n == null) return "$0";
  return "$" + Math.round(n).toLocaleString();
}

export function fmtPct(n) {
  if (n == null) return "0%";
  return n.toFixed(1) + "%";
}
