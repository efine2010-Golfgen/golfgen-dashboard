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
import ExecSummary from "./pages/ExecSummary";
import RetailReporting from "./pages/RetailReporting";
import WalmartAnalytics from "./pages/WalmartAnalytics";
import AmazonAnalytics from "./pages/AmazonAnalytics";
import GolfgenCommandCenter from "./pages/GolfgenCommandCenter";
import HierarchyFilter from "./components/HierarchyFilter";
import AskClaude from "./components/AskClaude";
import "./App.css";

/* ── Category definitions with view mappings ── */
/* Golf Division Navigation:
   Exec Summary | Amazon Analytics | Walmart Analytics | Shopify Analytics | GolfGen Command Center
*/
const CATEGORIES = [
  {
    key: "exec-summary", label: "Exec Summary",
    views: [
      { key: "exec-summary", path: "/exec-summary", label: "Exec Summary" },
    ],
  },
  {
    key: "amazon-analytics", label: "Amazon",
    views: [
      { key: "amazon-analytics", path: "/amazon-analytics", label: "Amazon Analytics" },
    ],
  },
  {
    key: "retail-reporting", label: "Walmart",
    views: [
      { key: "walmart-analytics", path: "/walmart-analytics", label: "Walmart Analytics" },
    ],
  },
  {
    key: "shopify-analytics", label: "Shopify",
    views: [
      { key: "shopify-analytics", path: "/shopify-analytics", label: "Shopify Analytics" },
    ],
  },
  {
    key: "golfgen-command", label: "GG-EGB",
    views: [
      { key: "golfgen-command", path: "/golfgen-command-center", label: "GolfGen Command Center" },
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
  return "amazon-analytics";
}

/* ── Theme Selector (4 themes: Midnight, Fairway, Warm, Fresh) ── */
function ThemeSelector() {
  const { theme, setTheme, themes } = useTheme();
  return (
    <div style={{ display:'flex', alignItems:'center', gap:8 }}>
      <span style={{ fontFamily:"'Space Grotesk',monospace", fontSize:8, fontWeight:700, textTransform:'uppercase', letterSpacing:'.1em', color:'rgba(255,255,255,.35)', whiteSpace:'nowrap' }}>Theme</span>
      {['midnight','fairway','warm','fresh'].map(t => (
        <button key={t} className={`tbtn${theme === t ? ' active' : ''}`} onClick={() => setTheme(t)}>
          {themes[t].name}
        </button>
      ))}
    </div>
  );
}

/* ── Amazon Analytics sub-tabs (lifted to sticky header) ── */
const AMAZON_TABS = [
  { key: "insights",        label: "INSIGHTS" },
  { key: "exec-summary",    label: "SALES SUMMARY" },
  { key: "item-performance",label: "ITEM PERFORMANCE" },
  { key: "profitability",   label: "PRICING & PROFIT" },
  { key: "fba-combined",    label: "FBA INV. & SHIPMENTS" },
  { key: "advertising",     label: "ADVERTISING" },
  { key: "planning",        label: "PLANNING" },
];

/* ── Navigation (subnav bar + sub-views row) ── */
function NavSystem({ permissions, mfaProtected, userMfaEnabled, division, customer }) {
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
      <nav className="subnav" style={{ position: 'relative' }}>
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

        <div style={{ position: 'absolute', left: '50%', top: '50%', transform: 'translate(-50%, -50%)', zIndex: 2 }}>
          <AskClaude activeTab={activeCategory} division={division} customer={customer} />
        </div>
        <div style={{ marginLeft: 'auto', display: 'flex', alignItems: 'center', paddingRight: 12, flexShrink: 0 }}>
          <ThemeSelector />
        </div>
      </nav>

      {/* Secondary: sub-views for current category (only when > 1 view) */}
      {visibleViews.length > 1 && (
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
  const isAmazon = location.pathname === '/amazon-analytics';
  const [amazonPage, setAmazonPage] = useState('exec-summary');
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
              <div className="hdr-title" style={{textAlign:'center'}}>GolfGen / EGB Analytics</div>
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
                <a href="/account/security/mfa-setup" className="hnav">Security</a>
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

        {/* ── Filter Bar — Tier 2 (hierarchy filter; hidden on Amazon) ── */}
        {!isAmazon && (
          <div className="filter-bar">
            <span className="filter-lbl">View:</span>
            <HierarchyFilter division={division} customer={customer} onChange={handleFilterChange} compact />
          </div>
        )}

        {/* ── Sub-nav — Tier 3 ── */}
        <NavSystem permissions={allowed} mfaProtected={mfaProtected} userMfaEnabled={userMfaEnabled} division={division} customer={customer} />


        {/* ── Accent stripe — below all header tiers ── */}
        <div className="stripe" />

        {/* ── Amazon Analytics tab nav — sticky under stripe, only on /amazon-analytics ── */}
        {isAmazon && (
          <div className="amazon-tab-nav">
            {AMAZON_TABS.map((t) => (
              <button
                key={t.key}
                onClick={() => setAmazonPage(t.key)}
                className={`amazon-tab-btn${amazonPage === t.key ? ' active' : ''}`}
              >
                {t.label}
              </button>
            ))}
            <div style={{ marginLeft: 'auto', display: 'flex', alignItems: 'center', gap: 8, flexShrink: 0 }}>
              <HierarchyFilter division={division} customer={customer} onChange={handleFilterChange} compact />
              <div style={{ display: 'flex', alignItems: 'center' }}>
                <button
                  onClick={() => handleMarketplaceChange('US')}
                  style={{
                    height: 26, padding: '0 9px', borderRadius: '5px 0 0 5px',
                    border: '1px solid var(--brd)', borderRight: 'none',
                    background: marketplace === 'US' ? 'var(--acc1)' : 'var(--ibg)',
                    color: marketplace === 'US' ? '#fff' : 'var(--txt3)',
                    fontSize: 10, fontWeight: 700, fontFamily: "'Space Grotesk',monospace",
                    cursor: 'pointer', transition: 'all .2s', letterSpacing: '.03em',
                    display: 'inline-flex', alignItems: 'center', gap: 4,
                  }}
                >🇺🇸 US</button>
                <button
                  onClick={() => handleMarketplaceChange('CA')}
                  style={{
                    height: 26, padding: '0 9px', borderRadius: '0 5px 5px 0',
                    border: '1px solid var(--brd)',
                    background: marketplace === 'CA' ? 'var(--acc1)' : 'var(--ibg)',
                    color: marketplace === 'CA' ? '#fff' : 'var(--txt3)',
                    fontSize: 10, fontWeight: 700, fontFamily: "'Space Grotesk',monospace",
                    cursor: 'pointer', transition: 'all .2s', letterSpacing: '.03em',
                    display: 'inline-flex', alignItems: 'center', gap: 4,
                  }}
                >🇨🇦 CA</button>
              </div>
            </div>
          </div>
        )}

      </div>

      {/* ── Main Content ── */}
      <main className="page">
        <Routes>
          {/* ── Master Exec Summary ── */}
          <Route path="/exec-summary" element={<ExecSummary filters={filters} />} />

          {/* ── Amazon Analytics (wrapper with sub-tabs) ── */}
          <Route path="/amazon-analytics" element={<AmazonAnalytics filters={filters} onMarketplaceChange={handleMarketplaceChange} page={amazonPage} setPage={setAmazonPage} />} />

          {/* ── Walmart Analytics ── */}
          {allowed["retail-reporting"] !== false && <Route path="/walmart-analytics" element={<WalmartAnalytics filters={filters} />} />}
          {allowed["retail-reporting"] !== false && <Route path="/retail-reporting" element={<RetailReporting filters={filters} />} />}

          {/* ── Shopify Analytics (placeholder) ── */}
          <Route path="/shopify-analytics" element={
            <div style={{ padding: "60px 40px", textAlign: "center", color: "var(--txt3)", border: "1px dashed var(--brd)", borderRadius: 8, background: "var(--card)", margin: 24 }}>
              <h2 style={{ fontFamily: "'DM Serif Display',Georgia,serif", fontSize: 22, color: "var(--txt2)", margin: "0 0 8px" }}>Shopify Analytics</h2>
              <p style={{ fontFamily: "'Space Grotesk',monospace", fontSize: 12, color: "var(--txt3)" }}>Shopify API integration coming in Phase 5.</p>
            </div>
          } />

          {/* ── GolfGen Command Center (wrapper with sub-tabs) ── */}
          <Route path="/golfgen-command-center" element={<GolfgenCommandCenter filters={filters} />} />

          {/* ── Legacy routes (backward compat, redirects) ── */}
          {allowed["dashboard"] !== false && <Route path="/" element={<Navigate to="/amazon-analytics" replace />} />}
          <Route path="/sales" element={<Navigate to="/amazon-analytics" replace />} />
          <Route path="/products" element={<Navigate to="/amazon-analytics" replace />} />
          <Route path="/profitability" element={<Navigate to="/amazon-analytics" replace />} />
          <Route path="/inventory" element={<Navigate to="/amazon-analytics" replace />} />
          <Route path="/golfgen-inventory" element={<Navigate to="/golfgen-command-center" replace />} />
          <Route path="/advertising" element={<Navigate to="/amazon-analytics" replace />} />
          <Route path="/supply-chain" element={<Navigate to="/golfgen-command-center" replace />} />
          <Route path="/fba-shipments" element={<Navigate to="/amazon-analytics" replace />} />
          <Route path="/item-planning" element={<Navigate to="/amazon-analytics" replace />} />
          <Route path="/item-master" element={<Navigate to="/amazon-analytics" replace />} />

          {/* ── Admin routes ── */}
          <Route path="/account/security/mfa-setup" element={<MfaSetup />} />
          {isAdmin && <Route path="/permissions" element={<Permissions />} />}
          {isAdmin && <Route path="/system" element={<System />} />}
          {isAdmin && <Route path="/audit-log" element={<AuditLog />} />}
          <Route path="*" element={<Navigate to="/amazon-analytics" replace />} />
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
      // Check MFA BEFORE marking as authed — prevents dashboard flash
      await loadMfaState();
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
