import { useState } from "react";

const DIVISIONS = [
  { value: "", label: "All Divisions" },
  { value: "golf", label: "Golf (PGAT)" },
  { value: "housewares", label: "Housewares" },
];

const CUSTOMERS = [
  { value: "", label: "All Customers" },
  { value: "amazon", label: "Amazon" },
  { value: "walmart_stores", label: "Walmart Stores" },
  { value: "first_tee", label: "First Tee" },
];

const PLATFORMS = [
  { value: "", label: "All Platforms" },
  { value: "sp_api", label: "SP-API (Amazon)" },
  { value: "walmart_api", label: "Walmart API" },
  { value: "manual_entry", label: "Manual Entry" },
];

export default function HierarchyFilter({ onChange, compact = false }) {
  const [division, setDivision] = useState("");
  const [customer, setCustomer] = useState("");
  const [platform, setPlatform] = useState("");

  const handleChange = (field, value) => {
    const next = { division, customer, platform, [field]: value };
    if (field === "division") next[field] = value;
    if (field === "customer") next[field] = value;
    if (field === "platform") next[field] = value;

    setDivision(next.division);
    setCustomer(next.customer);
    setPlatform(next.platform);
    onChange?.(next);
  };

  const selectStyle = {
    padding: compact ? "4px 8px" : "6px 12px",
    borderRadius: 6,
    border: "1px solid #334155",
    background: "#1e293b",
    color: "#e2e8f0",
    fontSize: compact ? 12 : 13,
    cursor: "pointer",
    outline: "none",
  };

  return (
    <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
      <select
        value={division}
        onChange={e => handleChange("division", e.target.value)}
        style={selectStyle}
      >
        {DIVISIONS.map(d => (
          <option key={d.value} value={d.value}>{d.label}</option>
        ))}
      </select>
      <select
        value={customer}
        onChange={e => handleChange("customer", e.target.value)}
        style={selectStyle}
      >
        {CUSTOMERS.map(c => (
          <option key={c.value} value={c.value}>{c.label}</option>
        ))}
      </select>
      <select
        value={platform}
        onChange={e => handleChange("platform", e.target.value)}
        style={selectStyle}
      >
        {PLATFORMS.map(p => (
          <option key={p.value} value={p.value}>{p.label}</option>
        ))}
      </select>
    </div>
  );
}
