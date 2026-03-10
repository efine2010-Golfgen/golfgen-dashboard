import { BrowserRouter, Routes, Route, NavLink } from "react-router-dom";
import Dashboard from "./pages/Dashboard";
import Products from "./pages/Products";
import Inventory from "./pages/Inventory";
import Advertising from "./pages/Advertising";
import "./App.css";

export default function App() {
  return (
    <BrowserRouter>
      <div className="app">
        <nav className="sidebar">
          <div className="logo">
            <span className="logo-icon">⛳</span>
            <span className="logo-text">GolfGen</span>
          </div>
          <div className="nav-links">
            <NavLink to="/" end>
              <span className="nav-icon">📊</span> Dashboard
            </NavLink>
            <NavLink to="/products">
              <span className="nav-icon">📦</span> Products
            </NavLink>
            <NavLink to="/inventory">
              <span className="nav-icon">🏭</span> Inventory
            </NavLink>
            <NavLink to="/advertising">
              <span className="nav-icon">📣</span> Advertising
            </NavLink>
          </div>
          <div className="nav-footer">
            <div className="nav-badge">LIVE DATA</div>
            <span className="nav-sub">Amazon SP-API + Ads API</span>
          </div>
        </nav>
        <main className="main-content">
          <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/products" element={<Products />} />
            <Route path="/inventory" element={<Inventory />} />
            <Route path="/advertising" element={<Advertising />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  );
}
