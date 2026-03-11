import { useState, useEffect } from "react";
import { BrowserRouter, Routes, Route, NavLink, useLocation, Navigate } from "react-router-dom";
import { api } from "./lib/api";
import Login from "./pages/Login";
import Dashboard from "./pages/Dashboard";
import Profitability from "./pages/Profitability";
import Products from "./pages/Products";
import Inventory from "./pages/Inventory";
import GolfGenInventory from "./pages/GolfGenInventory";
import Advertising from "./pages/Advertising";
import ItemMaster from "./pages/ItemMaster";
import FactoryPO from "./pages/FactoryPO";
import LogisticsTracking from "./pages/LogisticsTracking";
import ItemPlanning from "./pages/ItemPlanning";
import FBAShipments from "./pages/FBAShipments";
import Permissions from "./pages/Permissions";
import "./App.css";

/* ── Tab definitions keyed by tab_key matching backend ALL_TABS ── */
const ANALYTICS_TABS = [
  { key: "dashboard", path: "/", label: "Dashboard", icon: "📊", end: true },
  { key: "products", path: "/products", label: "Products", icon: "📦" },
  { key: "profitability", path: "/profitability", label: "Profitability", icon: "💰" },
  { key: "advertising", path: "/advertising", label: "Advertising", icon: "📣" },
];

const LOGISTICS_TABS = [
  { key: "inventory", path: "/inventory", label: "Amazon FBA", icon: "🏭" },
  { key: "golfgen-inventory", path: "/golfgen-inventory", label: "GolfGen Inventory", icon: "📦" },
  { key: "item-master", path: "/item-master", label: "Item Master", icon: "📋" },
  { key: "factory-po", path: "/factory-po", label: "Factory PO", icon: "🏭" },
  { key: "logistics", path: "/logistics", label: "OTW", icon: "🚢" },
  { key: "fba-shipments", path: "/fba-shipments", label: "Shipments to FBA", icon: "📦" },
  { key: "item-planning", path: "/item-planning", label: "Item Planning", icon: "📋" },
];

const LOGISTICS_PATHS = LOGISTICS_TABS.map(t => t.path);

function NavBars({ permissions }) {
  const location = useLocation();
  const path = location.pathname;
  const isLogistics = LOGISTICS_PATHS.some(p => path === p || path.startsWith(p + "/"));

  // Filter tabs based on permissions (allowed object: { tab_key: true/false })
  const allowed = permissions || {};
  const visibleAnalytics = ANALYTICS_TABS.filter(t => allowed[t.key] !== false);
  const visibleLogistics = LOGISTICS_TABS.filter(t => allowed[t.key] !== false);

  return (
    <>
      {visibleAnalytics.length > 0 && (
        <nav className={`nav-bar ${!isLogistics ? "" : "nav-bar-inactive"}`}>
          <div className="nav-inner">
            <div className="nav-section-label">Analytics</div>
            {visibleAnalytics.map(t => (
              <NavLink key={t.key} to={t.path} end={t.end} className={({ isActive }) => isActive ? "nav-tab active" : "nav-tab"}>
                <span>{t.icon}</span> {t.label}
              </NavLink>
            ))}
          </div>
        </nav>
      )}

      {visibleLogistics.length > 0 && (
        <nav className={`nav-bar nav-bar-logistics ${isLogistics ? "" : "nav-bar-inactive"}`}>
          <div className="nav-inner">
            <div className="nav-section-label">Logistics &amp; Inventory</div>
            {visibleLogistics.map(t => (
              <NavLink key={t.key} to={t.path} className={({ isActive }) => isActive ? "nav-tab active" : "nav-tab"}>
                <span>{t.icon}</span> {t.label}
              </NavLink>
            ))}
          </div>
        </nav>
      )}
    </>
  );
}

export default function App() {
  const [authed, setAuthed] = useState(null); // null = checking, true/false
  const [user, setUser] = useState(null);     // { name, email, role }
  const [permissions, setPermissions] = useState(null); // { tab_key: true/false }

  const loadUserData = async () => {
    try {
      const [meData, permData] = await Promise.all([api.me(), api.myPermissions()]);
      setUser(meData);
      // permData.tabs is an array of allowed tab keys; convert to { key: true } map
      // For admins, all tabs are allowed
      const tabList = permData.tabs || [];
      const allTabs = permData.allTabs || {};
      const permMap = {};
      for (const k of Object.keys(allTabs)) {
        permMap[k] = tabList.includes(k);
      }
      setPermissions(permMap);
      setAuthed(true);
    } catch {
      setAuthed(false);
      setUser(null);
      setPermissions(null);
    }
  };

  useEffect(() => {
    api.authCheck()
      .then(() => loadUserData())
      .catch(() => setAuthed(false));
  }, []);

  if (authed === null) {
    return <div className="loading"><div className="spinner" /> Checking session...</div>;
  }

  if (!authed) {
    return <Login onLogin={() => loadUserData()} />;
  }

  const handleLogout = async () => {
    await api.logout();
    setAuthed(false);
    setUser(null);
    setPermissions(null);
  };

  const isAdmin = user?.role === "admin";
  const allowed = permissions || {};

  return (
    <BrowserRouter>
      <div className="app">
        {/* ── Branded Header ── */}
        <header className="header">
          <div className="header-inner">
            <div className="header-brand">
              <div className="brand-logo">
                <span className="golf">Golf</span>
                <span className="gen">Gen</span>
              </div>
              <div className="brand-tagline">GOLF FOR EVERYONE. SERIOUSLY EVERYONE.</div>
            </div>
            <div className="header-meta">
              <div className="brand-title">
                <h1>Amazon FBA Dashboard</h1>
                <p>PGA TOUR Licensed &bull; SP-API Analytics</p>
              </div>
              <span className="live-badge">LIVE DATA</span>
              <div className="user-info">
                {user && <span className="user-name">{user.name}{isAdmin && <span className="admin-badge">Admin</span>}</span>}
                {isAdmin && (
                  <NavLink to="/permissions" className="permissions-link">Permissions</NavLink>
                )}
                <button className="logout-btn" onClick={handleLogout}>Sign Out</button>
              </div>
            </div>
          </div>
          <div className="gradient-bar" />
        </header>

        {/* ── Navigation Tabs (Two Rows) ── */}
        <NavBars permissions={allowed} />

        {/* ── Main Content ── */}
        <main className="main-content">
          <Routes>
            {allowed["dashboard"] !== false && <Route path="/" element={<Dashboard />} />}
            {allowed["products"] !== false && <Route path="/products" element={<Products />} />}
            {allowed["profitability"] !== false && <Route path="/profitability" element={<Profitability />} />}
            {allowed["inventory"] !== false && <Route path="/inventory" element={<Inventory />} />}
            {allowed["golfgen-inventory"] !== false && <Route path="/golfgen-inventory" element={<GolfGenInventory />} />}
            {allowed["advertising"] !== false && <Route path="/advertising" element={<Advertising />} />}
            {allowed["item-master"] !== false && <Route path="/item-master" element={<ItemMaster />} />}
            {allowed["factory-po"] !== false && <Route path="/factory-po" element={<FactoryPO />} />}
            {allowed["logistics"] !== false && <Route path="/logistics" element={<LogisticsTracking />} />}
            {allowed["fba-shipments"] !== false && <Route path="/fba-shipments" element={<FBAShipments />} />}
            {allowed["item-planning"] !== false && <Route path="/item-planning" element={<ItemPlanning />} />}
            {isAdmin && <Route path="/permissions" element={<Permissions />} />}
            <Route path="*" element={<Navigate to="/" replace />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  );
}
