import Sales from "./Sales";
import Products from "./Products";
import Profitability from "./Profitability";
import Advertising from "./Advertising";
import AmazonInsights from "./AmazonInsights";
import FBACombined from "./FBACombined";

export default function AmazonAnalytics({ filters = {}, onMarketplaceChange, page, setPage }) {
  return (
    <div style={{ padding: "0 24px 40px" }}>
      {page === "insights"         && <AmazonInsights filters={filters} />}
      {page === "exec-summary"     && <Sales filters={filters} />}
      {page === "item-performance" && <Products filters={filters} />}
      {page === "profitability"    && <Profitability filters={filters} />}
      {page === "fba-combined"     && <FBACombined filters={filters} />}
      {page === "advertising"      && <Advertising filters={filters} />}
    </div>
  );
}
