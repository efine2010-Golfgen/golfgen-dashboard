import { useState, useEffect, useRef } from "react";
import { BrowserRouter, Routes, Route, useLocation, useNavigate, Navigate } from "react-router-dom";
import { api } from "./lib/api";
import { ThemeProvider, useTheme } from "./context/ThemeContext";
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
import AuditLog from "./pages/AuditLog";
import Sales from "./pages/Sales";
import HierarchyFilter from "./components/HierarchyFilter";
import AskClaude from "./components/AskClaude";
import "./App.css";

/* ── Category definitions with view mappings ── */
const CATEGORIES = [
  {
    key: "exec-summary", label: "Exec Summary",
    views: [
      { key: "sales", path: "/sales", label: "Exec Summary" },
    ],
  },
  {
    key: "sales", label: "Item Sales",
    views: [
      { key: "products", path: "/products", label: "Item Sales" },
      { key: "item-master", path: "/item-master", label: "Item Master" },
    ],
  },
  {
    key: "profitability", label: "Profitability",
    views: [
      { key: "profitability", path: "/profitability", label: "Profitability" },
    ],
  },
  {
    key: "inventory", label: "Inventory",
    views: [
      { key: "golfgen-inventory", path: "/golfgen-inventory", label: "GolfGen Inventory" },
      { key: "inventory", path: "/inventory", label: "Amazon Inventory" },
      { key: "fba-shipments", path: "/fba-shipments", label: "Shipments to FBA" },
    ],
  },
  {
    key: "supply-chain", label: "Supply Chain",
    views: [
      { key: "supply-chain", path: "/supply-chain", label: "OTW" },
      { key: "supply-chain-po", path: "/supply-chain", label: "PO Summary", hash: "po" },
      { key: "supply-chain-inv", path: "/supply-chain", label: "Invoice Summary", hash: "invoices" },
    ],
  },
  {
    key: "forecasting", label: "Forecasting",
    views: [
      { key: "item-planning", path: "/item-planning", label: "Item Planning" },
    ],
  },
  {
    key: "advertising", label: "Advertising",
    views: [
      { key: "advertising", path: "/advertising", label: "Advertising" },
    ],
  },
  {
    key: "financial", label: "Financial",
    views: [
      { key: "dashboard", path: "/", label: "Financial", hash: "financial" },
    ],
  },
];

function detectCategory(pathname) {
  for (const cat of CATEGORIES) {
    for (const v of cat.views) {
      if (v.path === pathname || pathname.startsWith(v.path + "/")) {
        return cat.key;
      }
    }
  }
  return "exec-summary";
}

/* ── Theme Selector (double-stacked: Dark row on top, Light row below, selected theme pill to the right) ── */
function ThemeSelector() {
  const { theme, setTheme, themes } = useTheme();
  const lblStyle = { fontFamily:"'Space Grotesk',monospace", fontSize:7, fontWeight:700, textTransform:'uppercase', letterSpacing:'.1em', color:'var(--txt3)', whiteSpace:'nowrap', minWidth:28 };
  return (
    <div style={{ display:'flex', alignItems:'center', gap:8, marginLeft:'auto', flexShrink:0 }}>
      {/* Two rows of theme buttons */}
      <div style={{ display:'flex', flexDirection:'column', gap:3 }}>
        {/* Dark row */}
        <div style={{ display:'flex', alignItems:'center', gap:4 }}>
          <span style={lblStyle}>Dark</span>
          {['midnight', 'night', 'fairway'].map(t => (
            <button key={t} className={`tbtn${theme === t ? ' active' : ''}`} onClick={() => setTheme(t)}>
              {themes[t].name}
            </button>
          ))}
        </div>
        {/* Light row */}
        <div style={{ display:'flex', alignItems:'center', gap:4 }}>
          <span style={lblStyle}>Light</span>
          {['slate', 'warm', 'fresh'].map(t => (
            <button key={t} className={`tbtn${theme === t ? ' active' : ''}`} onClick={() => setTheme(t)}>
              {themes[t].name}
            </button>
          ))}
        </div>
      </div>
      {/* Current theme indicator pill */}
      <div style={{ display:'flex', alignItems:'center', gap:5, padding:'3px 8px', borderRadius:6, border:'1px solid var(--brd2)', background:'var(--ibg)' }}>
        <div style={{ width:10, height:10, borderRadius:3, background:themes[theme].sw, flexShrink:0 }} />
        <span style={{ fontFamily:"'Space Grotesk',monospace", fontSize:10, fontWeight:700, color:'var(--txt2)', whiteSpace:'nowrap' }}>{themes[theme].name}</span>
      </div>
    </div>
  );
}

/* ── Navigation (subnav bar + sub-views row) ── */
function NavSystem({ permissions, mfaProtected, userMfaEnabled }) {
  const location = useLocation();
  const navigate = useNavigate();
  const detected = detectCategory(location.pathname);
  const [activeCategory, setActiveCategory] = useState(detected);

  useEffect(() => {
    setActiveCategory(detectCategory(location.pathname));
  }, [location.pathname]);

  const allowed = permissions || {};
  const isViewVisible = (v) => {
    if (allowed[v.key] === false) return false;
    if (mfaProtected[v.key] && !userMfaEnabled) return false;
    return true;
  };

  const activeCat = CATEGORIES.find(c => c.key === activeCategory) || CATEGORIES[0];
  const visibleViews = activeCat.views.filter(isViewVisible);

  const handleCategorySelect = (cat) => {
    setActiveCategory(cat.key);
    const firstVisible = cat.views.find(v => isViewVisible(v));
    if (firstVisible) navigate(firstVisible.path);
  };

  const handleViewClick = (v) => {
    navigate(v.path);
  };

  return (
    <>
      {/* Primary: category tabs */}
      <nav className="subnav">
        {CATEGORIES.map((cat, i) => {
          const hasVisible = cat.views.some(v => isViewVisible(v));
          if (!hasVisible) return null;
          return (
            <span key={cat.key} style={{ display: 'contents' }}>
              {i > 0 && <div className="sn-div" />}
              <button
                className={`snbtn${activeCategory === cat.key ? ' active' : ''}`}
                onClick={() => handleCategorySelect(cat)}
              >
                {cat.label}
              </button>
            </span>
          );
        })}
      </nav>

      {/* Secondary: sub-views for current category (only when > 1 view) */}
      {visibleViews.length > 1 && activeCat.key !== 'inventory' && (
        <nav className="subnav subnav-sub" style={{ background: 'var(--card)', borderBottom: '1px solid var(--brd2)' }}>
          {visibleViews.map((v, i) => {
            const isActive = location.pathname === v.path;
            return (
              <span key={v.key} style={{ display: 'contents' }}>
                {i > 0 && <div className="sn-div" />}
                <button
                  className={`snbtn${isActive ? ' active' : ''}`}
                  style={{ fontSize: 11 }}
                  onClick={() => handleViewClick(v)}
                >
                  {v.label}
                </button>
              </span>
            );
          })}
        </nav>
      )}
    </>
  );
}

/* ── App Shell ── */
function AppShell({ user, isAdmin, allowed, mfaProtected, userMfaEnabled, filters, division, customer, marketplace, handleFilterChange, handleMarketplaceChange, handleLogout }) {
  const location = useLocation();
  const activeTab = detectCategory(location.pathname);
  return (
    <div className="app">
      {/* ── Sticky header wrapper ── */}
      <div className="sticky-header-wrapper">

        {/* ── Header ── */}
        <div className="hdr">
          <div className="hdr-inner">

            {/* LEFT: existing logo — DO NOT CHANGE */}
            <div className="header-brand">
              <div className="brand-logo">
                <span className="golf">Golf</span>
                <span className="gen">Gen</span>
              </div>
              <div className="brand-tagline">GOLF FOR EVERYONE. SERIOUSLY EVERYONE.</div>
            </div>

            {/* CENTER: title + subtitle + user badge */}
            <div className="hdr-mid">
              <div className="hdr-title">GolfGen / EGB Analytics</div>
              <div className="hdr-sub">Performance Dashboard</div>
              {user && (
                <div style={{ display:'flex', alignItems:'center', justifyContent:'center', gap:4, marginTop:1 }}>
                  <span style={{ fontFamily:"'Sora',sans-serif", fontSize:8.5, fontWeight:600, color:'rgba(255,255,255,.5)' }}>{user.name}</span>
                  {isAdmin && <span className="admin-pill">ADMIN</span>}
                </div>
              )}
            </div>

            {/* RIGHT: 2×3 button grid only */}
            <div className="hdr-right">
              <div className="hdr-btn-grid">
                <a href="/account/security/mfa-setup" className="hnav">MFA Setup</a>
                {isAdmin
                  ? <a href="/permissions" className="hnav">Permissions</a>
                  : <span className="hnav hnav-hidden" />
                }
                {isAdmin
                  ? <a href="/system" className="hnav">System</a>
                  : <span className="hnav hnav-hidden" />
                }
                {isAdmin
                  ? <a href="/audit-log" className="hnav">Audit Log</a>
                  : <span className="hnav hnav-hidden" />
                }
                <button className="hnav" onClick={handleLogout}>Sign Out</button>
                <span className="hnav live">LIVE DATA</span>
              </div>
            </div>

          </div>
        </div>

        {/* ── Filter Bar (View filter + Ask Claude + theme selector) — Tier 2 ── */}
        <div className="filter-bar">
          <span className="filter-lbl">View:</span>
          <HierarchyFilter division={division} customer={customer} onChange={handleFilterChange} compact />
          {/* Marketplace toggle — show on relevant Amazon pages */}
          {["exec-summary", "sales", "profitability", "inventory", "advertising"].includes(activeTab) && (
            <div style={{ display: 'flex', alignItems: 'center', gap: 0, marginLeft: 4 }}>
              <button
                onClick={() => handleMarketplaceChange("US")}
                className={`mp-toggle${marketplace === "US" ? " mp-active" : ""}`}
                style={{
                  height: 28, padding: '0 10px', borderRadius: '6px 0 0 6px',
                  border: '1px solid var(--brd)', borderRight: 'none',
                  background: marketplace === "US" ? 'var(--acc1)' : 'var(--ibg)',
                  color: marketplace === "US" ? '#fff' : 'var(--txt3)',
                  fontSize: 10, fontWeight: 700, fontFamily: "'Space Grotesk',monospace",
                  cursor: 'pointer', transition: 'all .2s', letterSpacing: '.03em',
                  display: 'inline-flex', alignItems: 'center', gap: 4,
                }}
              >
                <span style={{ fontSize: 11 }}>🇺🇸</span> US
              </button>
              <button
                onClick={() => handleMarketplaceChange("CA")}
                className={`mp-toggle${marketplace === "CA" ? " mp-active" : ""}`}
                style={{
                  height: 28, padding: '0 10px', borderRadius: '0 6px 6px 0',
                  border: '1px solid var(--brd)',
                  background: marketplace === "CA" ? 'var(--acc1)' : 'var(--ibg)',
                  color: marketplace === "CA" ? '#fff' : 'var(--txt3)',
                  fontSize: 10, fontWeight: 700, fontFamily: "'Space Grotesk',monospace",
                  cursor: 'pointer', transition: 'all .2s', letterSpacing: '.03em',
                  display: 'inline-flex', alignItems: 'center', gap: 4,
                }}
              >
                <span style={{ fontSize: 11 }}>🇨🇦</span> CA
              </button>
            </div>
          )}
          <AskClaude activeTab={activeTab} division={division} customer={customer} />
          <ThemeSelector />
        </div>

        {/* ── Sub-nav — Tier 3 ── */}
        <NavSystem permissions={allowed} mfaProtected={mfaProtected} userMfaEnabled={userMfaEnabled} />

        {/* ── Accent stripe — below all header tiers ── */}
        <div className="stripe" />

      </div>

      {/* ── Main Content ── */}
      <main className="page">
        <Routes>
          {allowed["dashboard"] !== false && <Route path="/" element={<Dashboard filters={filters} />} />}
          <Route path="/sales" element={<Sales filters={filters} />} />
          {allowed["products"] !== false && <Route path="/products" element={<Products filters={filters} />} />}
          {allowed["profitability"] !== false && <Route path="/profitability" element={<Profitability filters={filters} />} />}
          {allowed["inventory"] !== false && <Route path="/inventory" element={<Inventory filters={filters} />} />}
          {allowed["golfgen-inventory"] !== false && <Route path="/golfgen-inventory" element={<GolfGenInventory filters={filters} />} />}
          {allowed["advertising"] !== false && <Route path="/advertising" element={<Advertising filters={filters} />} />}
          {allowed["item-master"] !== false && <Route path="/item-master" element={<ItemMaster filters={filters} />} />}
          {allowed["factory-po"] !== false && <Route path="/factory-po" element={<FactoryPO />} />}
          {allowed["logistics"] !== false && <Route path="/logistics" element={<LogisticsTracking />} />}
          {allowed["supply-chain"] !== false && <Route path="/supply-chain" element={<SupplyChain />} />}
          {allowed["fba-shipments"] !== false && <Route path="/fba-shipments" element={<FBAShipments filters={filters} />} />}
          {allowed["item-planning"] !== false && <Route path="/item-planning" element={<ItemPlanning />} />}
          <Route path="/account/security/mfa-setup" element={<MfaSetup />} />
          {isAdmin && <Route path="/permissions" element={<Permissions />} />}
          {isAdmin && <Route path="/system" element={<System />} />}
          {isAdmin && <Route path="/audit-log" element={<AuditLog />} />}
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </main>
    </div>
  );
}

/* ── Root App ── */
export default function App() {
  const [authed, setAuthed] = useState(null);
  const [user, setUser] = useState(null);
  const [permissions, setPermissions] = useState(null);
  const [mfaNeeded, setMfaNeeded] = useState(false);
  const [mfaProtected, setMfaProtected] = useState({});
  const [userMfaEnabled, setUserMfaEnabled] = useState(false);

  const [division, setDivision] = useState("");
  const [customer, setCustomer] = useState("");
  const [marketplace, setMarketplace] = useState("US");
  const handleFilterChange = ({ division: d, customer: c }) => {
    setDivision(d);
    setCustomer(c);
  };
  const handleMarketplaceChange = (mp) => setMarketplace(mp);
  const filters = { division, customer, marketplace };

  const loadMfaState = async () => {
    try {
      const [protectedRes, statusRes] = await Promise.all([
        fetch(`${import.meta.env.VITE_API_URL || ""}/api/mfa/protected-routes`, { credentials: "include" }).then(r => r.ok ? r.json() : { routes: {} }),
        fetch(`${import.meta.env.VITE_API_URL || ""}/api/mfa/verify/status`, { credentials: "include" }).then(r => r.ok ? r.json() : { mfa_enabled: false, session_verified: true }),
      ]);
      setMfaProtected(protectedRes.routes || {});
      setUserMfaEnabled(statusRes.mfa_enabled || false);
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
      const tabList = permData.tabs || [];
      const allTabs = permData.allTabs || {};
      const permMap = {};
      for (const k of Object.keys(allTabs)) {
        permMap[k] = tabList.includes(k);
      }
      setPermissions(permMap);
      setAuthed(true);
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

    const handleMfaRequired = () => setMfaNeeded(true);
    window.addEventListener("mfa-required", handleMfaRequired);
    return () => window.removeEventListener("mfa-required", handleMfaRequired);
  }, []);

  if (authed === null) {
    return <div className="loading"><div className="spinner" /> Checking session...</div>;
  }

  if (!authed) {
    return (
      <ThemeProvider>
        <Login onLogin={() => loadUserData()} />
      </ThemeProvider>
    );
  }

  if (mfaNeeded) {
    return (
      <ThemeProvider>
        <MfaVerify onVerified={() => { setMfaNeeded(false); loadMfaState(); }} />
      </ThemeProvider>
    );
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
    <ThemeProvider>
      <BrowserRouter>
        <AppShell
          user={user}
          isAdmin={isAdmin}
          allowed={allowed}
          mfaProtected={mfaProtected}
          userMfaEnabled={userMfaEnabled}
          filters={filters}
          division={division}
          customer={customer}
          marketplace={marketplace}
          handleFilterChange={handleFilterChange}
          handleMarketplaceChange={handleMarketplaceChange}
          handleLogout={handleLogout}
        />
      </BrowserRouter>
    </ThemeProvider>
  );
}
