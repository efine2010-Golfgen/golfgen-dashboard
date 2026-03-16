import { useState, useEffect } from "react";
import { api } from "../../lib/api";
import {
  SG,
  Card,
  CardHdr,
  fN,
  f$,
  fPct,
  COLORS,
  KPICard,
} from "./WalmartHelpers";

export function WalmartStoreAnalytics({ filters }) {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [data, setData] = useState(null);
  const [week, setWeek] = useState(null);
  const [sortBy, setSortBy] = useState("storeName");
  const [sortDir, setSortDir] = useState("asc");
  const [page, setPage] = useState(0);
  const [limit] = useState(50);

  const weeks = data?.weeks || [];

  useEffect(() => {
    if (weeks.length > 0 && !week) {
      setWeek(weeks[0]);
    }
  }, [weeks, week]);

  useEffect(() => {
    (async () => {
      try {
        setLoading(true);
        setError(null);
        const result = await api.walmartStoreAnalytics(filters, {
          week: week || weeks?.[0],
          sortBy,
          sortDir,
          limit,
          offset: page * limit,
        });
        setData(result);
      } catch (err) {
        setError(err.message);
      } finally {
        setLoading(false);
      }
    })();
  }, [filters.division, filters.customer, week, sortBy, sortDir, page]);

  if (loading) {
    return (
      <Card>
        <p style={{ ...SG(12), color: "var(--txt3)" }}>Loading...</p>
      </Card>
    );
  }
  if (error) {
    return (
      <Card>
        <p style={{ ...SG(12), color: "#f87171" }}>Error: {error}</p>
      </Card>
    );
  }
  if (!data) {
    return (
      <Card>
        <p style={{ ...SG(12), color: "var(--txt3)" }}>No data</p>
      </Card>
    );
  }

  const kpis = data.kpis || {};
  const stores = data.stores || [];
  const total = data.total || 0;
  const offset = page * limit;
  const totalPages = Math.ceil(total / limit);

  const handleSort = (col) => {
    if (sortBy === col) {
      setSortDir(sortDir === "asc" ? "desc" : "asc");
    } else {
      setSortBy(col);
      setSortDir("asc");
    }
    setPage(0);
  };

  const renderSortIcon = (col) => {
    if (sortBy !== col) return "";
    return sortDir === "asc" ? " ▲" : " ▼";
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "24px" }}>
      {/* KPI Cards */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(4, 1fr)",
          gap: "12px",
        }}
      >
        <KPICard
          label="Total Stores"
          value={fN(kpis.totalStores)}
        />
        <KPICard
          label="Total POS Sales"
          value={f$(kpis.posSalesTy)}
        />
        <KPICard
          label="Avg POS/Store"
          value={f$(kpis.avgPosSalesPerStore)}
        />
        <KPICard
          label="Returns Qty"
          value={fN(kpis.returnsQtyTy)}
        />
      </div>

      {/* Week Filter */}
      <Card>
        <div style={{ marginBottom: "12px" }}>
          <label style={{ ...SG(11, 500), marginRight: "12px" }}>
            Week:
          </label>
          <select
            value={week || ""}
            onChange={(e) => {
              setWeek(e.target.value);
              setPage(0);
            }}
            style={{
              padding: "6px 8px",
              ...SG(11),
              borderRadius: "4px",
              border: "1px solid var(--border)",
            }}
          >
            {weeks.map((w) => (
              <option key={w} value={w}>
                {w}
              </option>
            ))}
          </select>
        </div>
      </Card>

      {/* Store Table */}
      <Card>
        <CardHdr title="Store Analysis" />
        <div style={{ overflowX: "auto", fontSize: "11px", lineHeight: "1.6" }}>
          <table
            style={{
              width: "100%",
              borderCollapse: "collapse",
            }}
          >
            <thead>
              <tr style={{ borderBottom: "1px solid var(--border)" }}>
                <th
                  style={{
                    textAlign: "left",
                    padding: "8px",
                    ...SG(10, 600),
                    cursor: "pointer",
                    userSelect: "none",
                  }}
                  onClick={() => handleSort("store_name")}
                >
                  Store Name{renderSortIcon("store_name")}
                </th>
                <th
                  style={{
                    textAlign: "right",
                    padding: "8px",
                    ...SG(10, 600),
                    cursor: "pointer",
                    userSelect: "none",
                  }}
                  onClick={() => handleSort("store_number")}
                >
                  Store #{renderSortIcon("store_number")}
                </th>
                <th
                  style={{
                    textAlign: "right",
                    padding: "8px",
                    ...SG(10, 600),
                    cursor: "pointer",
                    userSelect: "none",
                  }}
                  onClick={() => handleSort("pos_sales_ty")}
                >
                  POS Sales TY{renderSortIcon("pos_sales_ty")}
                </th>
                <th
                  style={{
                    textAlign: "right",
                    padding: "8px",
                    ...SG(10, 600),
                    cursor: "pointer",
                    userSelect: "none",
                  }}
                  onClick={() => handleSort("pos_sales_ly")}
                >
                  POS Sales LY{renderSortIcon("pos_sales_ly")}
                </th>
                <th
                  style={{
                    textAlign: "right",
                    padding: "8px",
                    ...SG(10, 600),
                    cursor: "pointer",
                    userSelect: "none",
                  }}
                  onClick={() => handleSort("pos_qty_ty")}
                >
                  POS Qty TY{renderSortIcon("pos_qty_ty")}
                </th>
                <th
                  style={{
                    textAlign: "right",
                    padding: "8px",
                    ...SG(10, 600),
                    cursor: "pointer",
                    userSelect: "none",
                  }}
                  onClick={() => handleSort("pos_qty_ly")}
                >
                  POS Qty LY{renderSortIcon("pos_qty_ly")}
                </th>
                <th
                  style={{
                    textAlign: "right",
                    padding: "8px",
                    ...SG(10, 600),
                    cursor: "pointer",
                    userSelect: "none",
                  }}
                  onClick={() => handleSort("returns_qty_ty")}
                >
                  Returns{renderSortIcon("returns_qty_ty")}
                </th>
                <th
                  style={{
                    textAlign: "right",
                    padding: "8px",
                    ...SG(10, 600),
                    cursor: "pointer",
                    userSelect: "none",
                  }}
                  onClick={() => handleSort("on_hand_qty_ty")}
                >
                  OH{renderSortIcon("on_hand_qty_ty")}
                </th>
                <th
                  style={{
                    textAlign: "right",
                    padding: "8px",
                    ...SG(10, 600),
                    cursor: "pointer",
                    userSelect: "none",
                  }}
                  onClick={() => handleSort("instock_pct_ty")}
                >
                  Instock %{renderSortIcon("instock_pct_ty")}
                </th>
              </tr>
            </thead>
            <tbody>
              {stores.map((store, idx) => (
                <tr
                  key={idx}
                  style={{
                    borderBottom: "1px solid var(--border)",
                    backgroundColor: idx % 2 === 0 ? "var(--bg2)" : "transparent",
                  }}
                >
                  <td style={{ padding: "8px", ...SG(11) }}>{store.storeName}</td>
                  <td style={{ padding: "8px", textAlign: "right", ...SG(11) }}>
                    {fN(store.storeNumber)}
                  </td>
                  <td style={{ padding: "8px", textAlign: "right", ...SG(11) }}>
                    {f$(store.posSalesTy)}
                  </td>
                  <td style={{ padding: "8px", textAlign: "right", ...SG(11) }}>
                    {f$(store.posSalesLy)}
                  </td>
                  <td style={{ padding: "8px", textAlign: "right", ...SG(11) }}>
                    {fN(store.posQtyTy)}
                  </td>
                  <td style={{ padding: "8px", textAlign: "right", ...SG(11) }}>
                    {fN(store.posQtyLy)}
                  </td>
                  <td style={{ padding: "8px", textAlign: "right", ...SG(11) }}>
                    {fN(store.returnsQtyTy)}
                  </td>
                  <td style={{ padding: "8px", textAlign: "right", ...SG(11) }}>
                    {fN(store.onHandQtyTy)}
                  </td>
                  <td style={{ padding: "8px", textAlign: "right", ...SG(11) }}>
                    {store.instockPctTy != null ? Number(store.instockPctTy).toFixed(1) + "%" : "—"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {/* Pagination */}
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            marginTop: "12px",
            ...SG(10),
            color: "var(--txt3)",
          }}
        >
          <div>
            Showing {offset + 1}–{Math.min(offset + limit, total)} of {total}
          </div>
          <div style={{ display: "flex", gap: "6px" }}>
            <button
              onClick={() => setPage(Math.max(0, page - 1))}
              disabled={page === 0}
              style={{
                padding: "4px 12px",
                ...SG(10),
                borderRadius: "4px",
                border: "1px solid var(--border)",
                backgroundColor: page === 0 ? "var(--bg2)" : "var(--card)",
                cursor: page === 0 ? "default" : "pointer",
                color: "var(--txt)",
              }}
            >
              Prev
            </button>
            <button
              onClick={() => setPage(Math.min(totalPages - 1, page + 1))}
              disabled={page >= totalPages - 1}
              style={{
                padding: "4px 12px",
                ...SG(10),
                borderRadius: "4px",
                border: "1px solid var(--border)",
                backgroundColor: page >= totalPages - 1 ? "var(--bg2)" : "var(--card)",
                cursor: page >= totalPages - 1 ? "default" : "pointer",
                color: "var(--txt)",
              }}
            >
              Next
            </button>
          </div>
        </div>
      </Card>
    </div>
  );
}
