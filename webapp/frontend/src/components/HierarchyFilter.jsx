import { useState, useEffect } from "react";

/**
 * Division → Customer mapping.
 * Golf: amazon, walmart_marketplace, walmart_stores, shopify, first_tee
 * Housewares: amazon, walmart_marketplace, walmart_stores, belk, hobby_lobby, albertsons, family_dollar
 */
const DIVISIONS = [
  { value: "", label: "All Divisions" },
  { value: "golf", label: "Golf (PGAT)" },
  { value: "housewares", label: "Housewares" },
];

const CUSTOMERS_BY_DIVISION = {
  "": [
    { value: "", label: "All Customers" },
    { value: "amazon", label: "Amazon" },
    { value: "walmart_marketplace", label: "Walmart Marketplace" },
    { value: "walmart_stores", label: "Walmart Stores" },
    { value: "shopify", label: "Shopify" },
    { value: "first_tee", label: "First Tee" },
    { value: "belk", label: "Belk" },
    { value: "hobby_lobby", label: "Hobby Lobby" },
    { value: "albertsons", label: "Albertsons" },
    { value: "family_dollar", label: "Family Dollar" },
  ],
  golf: [
    { value: "", label: "All Customers" },
    { value: "amazon", label: "Amazon" },
    { value: "walmart_marketplace", label: "Walmart Marketplace" },
    { value: "walmart_stores", label: "Walmart Stores" },
    { value: "shopify", label: "Shopify" },
    { value: "first_tee", label: "First Tee" },
  ],
  housewares: [
    { value: "", label: "All Customers" },
    { value: "amazon", label: "Amazon" },
    { value: "walmart_marketplace", label: "Walmart Marketplace" },
    { value: "walmart_stores", label: "Walmart Stores" },
    { value: "belk", label: "Belk" },
    { value: "hobby_lobby", label: "Hobby Lobby" },
    { value: "albertsons", label: "Albertsons" },
    { value: "family_dollar", label: "Family Dollar" },
  ],
};

export default function HierarchyFilter({ division, customer, onChange, compact = false }) {
  const customers = CUSTOMERS_BY_DIVISION[division || ""] || CUSTOMERS_BY_DIVISION[""];

  // Reset customer when division changes and current customer isn't valid for new division
  useEffect(() => {
    if (customer && !customers.find(c => c.value === customer)) {
      onChange?.({ division, customer: "" });
    }
  }, [division]);

  const handleDivisionChange = (value) => {
    // Reset customer when division changes
    onChange?.({ division: value, customer: "" });
  };

  const handleCustomerChange = (value) => {
    onChange?.({ division, customer: value });
  };

  const selectStyle = {
    padding: compact ? "4px 8px" : "6px 12px",
    borderRadius: 6,
    border: "1px solid var(--brd)",
    background: "var(--ibg)",
    color: "var(--txt2)",
    fontSize: compact ? 12 : 13,
    cursor: "pointer",
    outline: "none",
  };

  return (
    <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
      <select
        value={division || ""}
        onChange={e => handleDivisionChange(e.target.value)}
        style={selectStyle}
      >
        {DIVISIONS.map(d => (
          <option key={d.value} value={d.value}>{d.label}</option>
        ))}
      </select>
      <select
        value={customer || ""}
        onChange={e => handleCustomerChange(e.target.value)}
        style={selectStyle}
      >
        {customers.map(c => (
          <option key={c.value} value={c.value}>{c.label}</option>
        ))}
      </select>
    </div>
  );
}
