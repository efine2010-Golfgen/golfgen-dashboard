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
import SupplyChain from "./pages/SupplyChain";
import ItemPlanning from "./pages/ItemPlanning";
import FBAShipments from "./pages/FBAShipments";
import Permissions from "./pages/Permissions";
import System from "./pages/System";
import MfaSetup from "./pages/MfaSetup";
import MfaVerify from "./pages/MfaVerify";
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
  { key: "supply-chain", path: "/supply-chain", label: "Supply Chain", icon: "🚢" },
  { key: "fba-shipments", path: "/fba-shipments", label: "Shipments to FBA", icon: "📦" },
  { key: "item-planning", path: "/item-planning", label: "Item Planning", icon: "📋" },
];

const LOGISTICS_PATHS = LOGISTICS_TABS.map(t => t.path);

function NavBars({ permissions, mfaProtected, userMfaEnabled }) {
  const location = useLocation();
  const path = location.pathname;
  const isLogistics = LOGISTICS_PATHS.some(p => path === p || path.startsWith(p + "/"));

  // Filter tabs based on permissions (allowed object: { tab_key: true/false })
  // Also hide MFA-protected tabs when user hasn't enrolled in MFA (hidden page behavior)
  const allowed = permissions || {};
  const isTabVisible = (t) => {
    if (allowed[t.key] === false) return false;
    // If this tab requires MFA and user hasn't enrolled, hide it entirely
    if (mfaProtected[t.key] && !userMfaEnabled) return false;
    return true;
  };
  const visibleAnalytics = ANALYTICS_TABS.filter(isTabVisible);
  const visibleLogistics = LOGISTICS_TABS.filter(isTabVisible);

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
  const [mfaNeeded, setMfaNeeded] = useState(false); // MFA verification pending
  const [mfaProtected, setMfaProtected] = useState({}); // { tab_key: true/false }
  const [userMfaEnabled, setUserMfaEnabled] = useState(false); // current user enrolled in MFA

  const loadMfaState = async () => {
    try {
      const [protectedRes, statusRes] = await Promise.all([
        fetch(`${import.meta.env.VITE_API_URL || ""}/api/mfa/protected-routes`, { credentials: "include" }).then(r => r.ok ? r.json() : { routes: {} }),
        fetch(`${import.meta.env.VITE_API_URL || ""}/api/mfa/verify/status`, { credentials: "include" }).then(r => r.ok ? r.json() : { mfa_enabled: false, session_verified: true }),
      ]);
      setMfaProtected(protectedRes.routes || {});
      setUserMfaEnabled(statusRes.mfa_enabled || false);
      // If user has MFA enabled but session not verified, show MFA verify screen
      if (statusRes.mfa_enabled && !statusRes.session_verified) {
        setMfaNeeded(true);
      }
    } catch {
      // MFA not available — continue normally
    }
  };

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
      // Load MFA state after auth succeeds
      await loadMfaState();
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

    // Listen for MFA-required events from api.js fetchJSON
    const handleMfaRequired = () => setMfaNeeded(true);
    window.addEventListener("mfa-required", handleMfaRequired);
    return () => window.removeEventListener("mfa-required", handleMfaRequired);
  }, []);

  if (authed === null) {
    return <div className="loading"><div className="spinner" /> Checking session...</div>;
  }

  if (!authed) {
    return <Login onLogin={() => loadUserData()} />;
  }

  if (mfaNeeded) {
    return <MfaVerify onVerified={() => { setMfaNeeded(false); loadMfaState(); }} />;
  }

  const handleLogout = async () => {
    await api.logout();
    setAuthed(false);
    setUser(null);
    setPermissions(null);
    setMfaNeeded(false);
    setMfaProtected({});
    setUserMfaEnabled(false);
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
                <NavLink to="/account/security/mfa-setup" className="permissions-link">MFA Setup</NavLink>
                {isAdmin && (
                  <>
                    <NavLink to="/permissions" className="permissions-link">Permissions</NavLink>
                    <NavLink to="/system" className="permissions-link">System</NavLink>
                  </>
                )}
                <button className="logout-btn" onClick={handleLogout}>Sign Out</button>
              </div>
            </div>
          </div>
          <div className="gradient-bar" />
        </header>

        {/* ── Navigation Tabs (Two Rows) ── */}
        <NavBars permissions={allowed} mfaProtected={mfaProtected} userMfaEnabled={userMfaEnabled} />

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
            {allowed["supply-chain"] !== false && <Route path="/supply-chain" element={<SupplyChain />} />}
            {allowed["fba-shipments"] !== false && <Route path="/fba-shipments" element={<FBAShipments />} />}
            {allowed["item-planning"] !== false && <Route path="/item-planning" element={<ItemPlanning />} />}
            <Route path="/account/security/mfa-setup" element={<MfaSetup />} />
            {isAdmin && <Route path="/permissions" element={<Permissions />} />}
            {isAdmin && <Route path="/system" element={<System />} />}
            <Route path="*" element={<Navigate to="/" replace />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  );
}
