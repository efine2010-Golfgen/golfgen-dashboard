import { createContext, useContext, useEffect, useState } from 'react';

const THEMES = {
  midnight: { name: 'Midnight', desc: 'Deep unified dark — header to data, one environment',      sw: 'linear-gradient(135deg,#0E1F2D 35%,#2ECFAA)' },
  night:    { name: 'Night',    desc: 'Rich dark blue — navy-forward, higher contrast',            sw: 'linear-gradient(135deg,#0d1e2e 35%,#2ECFAA)' },
  fairway:  { name: 'Fairway',  desc: 'Cool dark slate — navy tones with warm orange accent lead', sw: 'linear-gradient(135deg,#101820 35%,#E87830)' },
  slate:    { name: 'Slate',    desc: 'Navy header, cool white content — executive bi-tone',       sw: 'linear-gradient(135deg,#0E1F2D 35%,#e8eef6)' },
  warm:     { name: 'Warm',     desc: 'Navy header, warm sand content — rich and inviting',        sw: 'linear-gradient(135deg,#0E1F2D 35%,#f5efe8)' },
  fresh:    { name: 'Fresh',    desc: 'Navy header, mint-green content — brand-forward, crisp',    sw: 'linear-gradient(135deg,#0E1F2D 35%,#eaf4ef)' },
};

const ThemeContext = createContext(null);

export function ThemeProvider({ children }) {
  const [theme, setThemeState] = useState(
    () => localStorage.getItem('gg_theme') || 'midnight'
  );

  useEffect(() => {
    document.body.className = theme === 'midnight' ? '' : theme;
    localStorage.setItem('gg_theme', theme);
  }, [theme]);

  const setTheme = (t) => setThemeState(t);

  return (
    <ThemeContext.Provider value={{ theme, setTheme, themes: THEMES }}>
      {children}
    </ThemeContext.Provider>
  );
}

export const useTheme = () => useContext(ThemeContext);
