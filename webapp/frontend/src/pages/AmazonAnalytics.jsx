import Sales from "./Sales";
import Products from "./Products";
import Profitability from "./Profitability";
import Inventory from "./Inventory";
import FBAShipments from "./FBAShipments";
import Advertising from "./Advertising";
import ItemPlanning from "./ItemPlanning";
import ItemMaster from "./ItemMaster";

const TABS = [
  { key: "exec-summary", label: "SALES SUMMARY" },
  { key: "item-performance", label: "ITEM PERFORMANCE" },
  { key: "profitability", label: "PROFITABILITY" },
  { key: "fba-inventory", label: "FBA INVENTORY" },
  { key: "fba-shipments", label: "FBA SHIPMENTS" },
  { key: "advertising", label: "ADVERTISING" },
  { key: "forecasting", label: "FORECASTING" },
  { key: "item-master", label: "ITEM MASTER" },
];

export default function AmazonAnalytics({ filters = {}, onMarketplaceChange, page, setPage }) {
  const mp = filters.marketplace || "US";

  return (
    <div style={{ padding: "0 24px 40px" }}>
      {/* ── Tab content ── */}
      {page === "exec-summary" && <Sales filters={filters} />}
      {page === "item-performance" && <Products filters={filters} />}
      {page === "profitability" && <Profitability filters={filters} />}
      {page === "fba-inventory" && <Inventory filters={filters} />}
      {page === "fba-shipments" && <FBAShipments filters={filters} />}
      {page === "advertising" && <Advertising filters={filters} />}
      {page === "forecasting" && <ItemPlanning />}
      {page === "item-master" && <ItemMaster />}
    </div>
  );
}
