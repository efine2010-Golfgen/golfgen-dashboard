import { BrowserRouter, Routes, Route, NavLink } from "react-router-dom";
import Dashboard from "./pages/Dashboard";
import Profitability from "./pages/Profitability";
import Products from "./pages/Products";
import Inventory from "./pages/Inventory";
import Advertising from "./pages/Advertising";
import "./App.css";

export default function App() {
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
            </div>
          </div>
          <div className="gradient-bar" />
        </header>

        {/* ── Navigation Tabs ── */}
        <nav className="nav-bar">
          <div className="nav-inner">
            <NavLink to="/" end className={({ isActive }) => isActive ? "nav-tab active" : "nav-tab"}>
              <span>📊</span> Dashboard
            </NavLink>
            <NavLink to="/profitability" className={({ isActive }) => isActive ? "nav-tab active" : "nav-tab"}>
              <span>💰</span> Profitability
            </NavLink>
            <NavLink to="/products" className={({ isActive }) => isActive ? "nav-tab active" : "nav-tab"}>
              <span>📦</span> Products
            </NavLink>
            <NavLink to="/inventory" className={({ isActive }) => isActive ? "nav-tab active" : "nav-tab"}>
              <span>🏭</span> Inventory
            </NavLink>
            <NavLink to="/advertising" className={({ isActive }) => isActive ? "nav-tab active" : "nav-tab"}>
              <span>📣</span> Advertising
            </NavLink>
          </div>
        </nav>

        {/* ── Main Content ── */}
        <main className="main-content">
          <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/profitability" element={<Profitability />} />
            <Route path="/products" element={<Products />} />
            <Route path="/inventory" element={<Inventory />} />
            <Route path="/advertising" element={<Advertising />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  );
}
