import { useState, useEffect } from "react";
import { BrowserRouter, Routes, Route, NavLink, useLocation } from "react-router-dom";
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
import "./App.css";

const ANALYTICS_PATHS = ["/", "/products", "/profitability", "/advertising"];
const LOGISTICS_PATHS = ["/inventory", "/golfgen-inventory", "/item-master", "/factory-po", "/logistics", "/item-planning"];

function NavBars() {
  const location = useLocation();
  const path = location.pathname;
  const isLogistics = LOGISTICS_PATHS.some(p => path === p || path.startsWith(p + "/"));

  return (
    <>
      {/* ── Row 1: Analytics ── */}
      <nav className={`nav-bar ${!isLogistics ? "" : "nav-bar-inactive"}`}>
        <div className="nav-inner">
          <div className="nav-section-label">Analytics</div>
          <NavLink to="/" end className={({ isActive }) => isActive ? "nav-tab active" : "nav-tab"}>
            <span>📊</span> Dashboard
          </NavLink>
          <NavLink to="/products" className={({ isActive }) => isActive ? "nav-tab active" : "nav-tab"}>
            <span>📦</span> Products
          </NavLink>
          <NavLink to="/profitability" className={({ isActive }) => isActive ? "nav-tab active" : "nav-tab"}>
            <span>💰</span> Profitability
          </NavLink>
          <NavLink to="/advertising" className={({ isActive }) => isActive ? "nav-tab active" : "nav-tab"}>
            <span>📣</span> Advertising
          </NavLink>
        </div>
      </nav>

      {/* ── Row 2: Logistics & Inventory ── */}
      <nav className={`nav-bar nav-bar-logistics ${isLogistics ? "" : "nav-bar-inactive"}`}>
        <div className="nav-inner">
          <div className="nav-section-label">Logistics &amp; Inventory</div>
          <NavLink to="/inventory" className={({ isActive }) => isActive ? "nav-tab active" : "nav-tab"}>
            <span>🏭</span> Amazon FBA
          </NavLink>
          <NavLink to="/golfgen-inventory" className={({ isActive }) => isActive ? "nav-tab active" : "nav-tab"}>
            <span>📦</span> GolfGen Inventory
          </NavLink>
          <NavLink to="/item-master" className={({ isActive }) => isActive ? "nav-tab active" : "nav-tab"}>
            <span>📋</span> Item Master
          </NavLink>
          <NavLink to="/factory-po" className={({ isActive }) => isActive ? "nav-tab active" : "nav-tab"}>
            <span>🏭</span> Factory PO
          </NavLink>
          <NavLink to="/logistics" className={({ isActive }) => isActive ? "nav-tab active" : "nav-tab"}>
            <span>🚢</span> OTW / Logistics
          </NavLink>
          <NavLink to="/item-planning" className={({ isActive }) => isActive ? "nav-tab active" : "nav-tab"}>
            <span>📋</span> Item Planning
          </NavLink>
        </div>
      </nav>
    </>
  );
}

export default function App() {
  const [authed, setAuthed] = useState(null); // null = checking, true/false

  useEffect(() => {
    api.authCheck()
      .then(() => setAuthed(true))
      .catch(() => setAuthed(false));
  }, []);

  if (authed === null) {
    return <div className="loading"><div className="spinner" /> Checking session...</div>;
  }

  if (!authed) {
    return <Login onLogin={() => setAuthed(true)} />;
  }

  const handleLogout = async () => {
    await api.logout();
    setAuthed(false);
  };

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
              <button className="logout-btn" onClick={handleLogout}>Sign Out</button>
            </div>
          </div>
          <div className="gradient-bar" />
        </header>

        {/* ── Navigation Tabs (Two Rows) ── */}
        <NavBars />

        {/* ── Main Content ── */}
        <main className="main-content">
          <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/products" element={<Products />} />
            <Route path="/profitability" element={<Profitability />} />
            <Route path="/inventory" element={<Inventory />} />
            <Route path="/golfgen-inventory" element={<GolfGenInventory />} />
            <Route path="/advertising" element={<Advertising />} />
            <Route path="/item-master" element={<ItemMaster />} />
            <Route path="/factory-po" element={<FactoryPO />} />
            <Route path="/logistics" element={<LogisticsTracking />} />
            <Route path="/item-planning" element={<ItemPlanning />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  );
}
