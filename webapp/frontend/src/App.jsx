import { useState, useEffect, useRef } from "react";
import { BrowserRouter, Routes, Route, useLocation, useNavigate, Navigate } from "react-router-dom";
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
import HierarchyFilter from "./components/HierarchyFilter";
import "./App.css";

/* ── Category definitions with view mappings ── */
const CATEGORIES = [
  {
    key: "exec-summary", label: "Exec Summary",
    views: [
      { key: "dashboard", path: "/", label: "Exec Summary", end: true },
    ],
  },
  {
    key: "sales", label: "Sales",
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

// Build a flat map of path -> category key for auto-detecting active category
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

/* ── Dropdown + View Tabs Navigation ── */
function NavSystem({ permissions, mfaProtected, userMfaEnabled }) {
  const location = useLocation();
  const navigate = useNavigate();
  const [dropdownOpen, setDropdownOpen] = useState(false);
  const dropdownRef = useRef(null);
  const detected = detectCategory(location.pathname);
  const [activeCategory, setActiveCategory] = useState(detected);

  // Sync category when route changes externally
  useEffect(() => {
    setActiveCategory(detectCategory(location.pathname));
  }, [location.pathname]);

  // Close dropdown on outside click
  useEffect(() => {
    const handler = (e) => {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target)) {
        setDropdownOpen(false);
      }
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, []);

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
    setDropdownOpen(false);
    // Navigate to the first view of the category
    const firstVisible = cat.views.find(v => isViewVisible(v));
    if (firstVisible) {
      navigate(firstVisible.path);
    }
  };

  const handleViewClick = (v) => {
    navigate(v.path);
  };

  return (
    <nav className="nav-bar-unified">
      <div className="nav-inner-unified">
        {/* Category Dropdown */}
        <div className="category-dropdown" ref={dropdownRef}>
          <button className="category-dropdown-btn" onClick={() => setDropdownOpen(!dropdownOpen)}>
            {activeCat.label}
            <span className="dropdown-arrow">{dropdownOpen ? "▴" : "▾"}</span>
          </button>
          {dropdownOpen && (
            <div className="category-dropdown-menu">
              {CATEGORIES.map(cat => (
                <button
                  key={cat.key}
                  className={`category-dropdown-item ${cat.key === activeCategory ? "active" : ""}`}
                  onClick={() => handleCategorySelect(cat)}
                >
                  {cat.label}
                </button>
              ))}
            </div>
          )}
        </div>

        {/* View Tabs for Selected Category */}
        {visibleViews.length > 1 && (
          <div className="view-tabs">
            {visibleViews.map(v => {
              const isActive = location.pathname === v.path || (v.end && location.pathname === "/");
              return (
                <button
                  key={v.key}
                  className={`view-tab ${isActive ? "active" : ""}`}
                  onClick={() => handleViewClick(v)}
                >
                  {v.label}
                </button>
              );
            })}
          </div>
        )}
      </div>
    </nav>
  );
}

export default function App() {
  const [authed, setAuthed] = useState(null);
  const [user, setUser] = useState(null);
  const [permissions, setPermissions] = useState(null);
  const [mfaNeeded, setMfaNeeded] = useState(false);
  const [mfaProtected, setMfaProtected] = useState({});
  const [userMfaEnabled, setUserMfaEnabled] = useState(false);

  // Global hierarchy filter state
  const [division, setDivision] = useState("");
  const [customer, setCustomer] = useState("");
  const handleFilterChange = ({ division: d, customer: c }) => {
    setDivision(d);
    setCustomer(c);
  };
  const filters = { division, customer };

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
        {/* ── Sticky Header + Nav ── */}
        <div className="sticky-header-wrapper">
          <header className="header">
            <div className="header-inner">
              {/* Left: Logo */}
              <div className="header-brand">
                <div className="brand-logo">
                  <span className="golf">Golf</span>
                  <span className="gen">Gen</span>
                </div>
                <div className="brand-tagline">GOLF FOR EVERYONE. SERIOUSLY EVERYONE.</div>
              </div>

              {/* Center: Title + Filters */}
              <div className="header-center">
                <div className="brand-title">
                  <h1>Golfgen/EGB Analytics</h1>
                </div>
                <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
                  <HierarchyFilter division={division} customer={customer} onChange={handleFilterChange} compact />
                  {user && <span className="user-name">{user.name}{isAdmin && <span className="admin-badge">Admin</span>}</span>}
                </div>
              </div>

              {/* Right: Buttons Grid */}
              <div className="header-right">
                <div className="header-buttons-grid">
                  <a href="/account/security/mfa-setup" className="header-grid-btn">MFA Setup</a>
                  {isAdmin ? (
                    <a href="/permissions" className="header-grid-btn">Permissions</a>
                  ) : (
                    <span className="header-grid-btn header-grid-btn-hidden"></span>
                  )}
                  {isAdmin ? (
                    <a href="/system" className="header-grid-btn">System</a>
                  ) : (
                    <span className="header-grid-btn header-grid-btn-hidden"></span>
                  )}
                  <button className="header-grid-btn header-grid-btn-signout" onClick={handleLogout}>Sign Out</button>
                </div>
                <div className="live-badge-row">
                  <span className="live-badge header-grid-btn">LIVE DATA</span>
                </div>
              </div>
            </div>
            <div className="gradient-bar" />
          </header>

          {/* ── Navigation (single row with dropdown + view tabs) ── */}
          <NavSystem permissions={allowed} mfaProtected={mfaProtected} userMfaEnabled={userMfaEnabled} />
        </div>

        {/* ── Main Content ── */}
        <main className="main-content">
          <Routes>
            {allowed["dashboard"] !== false && <Route path="/" element={<Dashboard filters={filters} />} />}
            {allowed["products"] !== false && <Route path="/products" element={<Products filters={filters} />} />}
            {allowed["profitability"] !== false && <Route path="/profitability" element={<Profitability filters={filters} />} />}
            {allowed["inventory"] !== false && <Route path="/inventory" element={<Inventory filters={filters} />} />}
            {allowed["golfgen-inventory"] !== false && <Route path="/golfgen-inventory" element={<GolfGenInventory filters={filters} />} />}
            {allowed["advertising"] !== false && <Route path="/advertising" element={<Advertising filters={filters} />} />}
            {allowed["item-master"] !== false && <Route path="/item-master" element={<ItemMaster filters={filters} />} />}
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
