import { useState, useEffect } from "react";
import { BrowserRouter, Routes, Route, NavLink } from "react-router-dom";
import { api } from "./lib/api";
import Login from "./pages/Login";
import Dashboard from "./pages/Dashboard";
import Profitability from "./pages/Profitability";
import Products from "./pages/Products";
import Inventory from "./pages/Inventory";
import GolfGenInventory from "./pages/GolfGenInventory";
import Advertising from "./pages/Advertising";
import ItemMaster from "./pages/ItemMaster";
import "./App.css";

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

        {/* ── Navigation Tabs ── */}
        <nav className="nav-bar">
          <div className="nav-inner">
            <NavLink to="/" end className={({ isActive }) => isActive ? "nav-tab active" : "nav-tab"}>
              <span>📊</span> Dashboard
            </NavLink>
            <NavLink to="/products" className={({ isActive }) => isActive ? "nav-tab active" : "nav-tab"}>
              <span>📦</span> Products
            </NavLink>
            <NavLink to="/profitability" className={({ isActive }) => isActive ? "nav-tab active" : "nav-tab"}>
              <span>💰</span> Profitability
            </NavLink>
            <NavLink to="/inventory" className={({ isActive }) => isActive ? "nav-tab active" : "nav-tab"}>
              <span>🏭</span> Amazon Inventory
            </NavLink>
            <NavLink to="/golfgen-inventory" className={({ isActive }) => isActive ? "nav-tab active" : "nav-tab"}>
              <span>📦</span> GolfGen Inventory
            </NavLink>
            <NavLink to="/advertising" className={({ isActive }) => isActive ? "nav-tab active" : "nav-tab"}>
              <span>📣</span> Advertising
            </NavLink>
            <NavLink to="/item-master" className={({ isActive }) => isActive ? "nav-tab active" : "nav-tab"}>
              <span>📋</span> Item Master
            </NavLink>
          </div>
        </nav>

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
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  );
}
