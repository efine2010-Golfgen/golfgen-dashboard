import { useState, useEffect, useRef, useCallback, Fragment } from "react";
import { api } from "../../lib/api";
import {
  SG,
  DM,
  Card,
  CardHdr,
  fN,
  f$,
  fPct,
  COLORS,
  KPICard,
  ChartCanvas,
} from "./WalmartHelpers";

// ═════════════════════════════════════════════════════════════════════════════
// BRAND HEATMAP GRADIENT (9 tiers)
// ═════════════════════════════════════════════════════════════════════════════
const TIER_COLORS = [
  "#1A2D42", "#2a4a6e", "#3E658C", "#7BAED0",
  "#2ECFAA", "#22A387", "#F5B731", "#E87830", "#D03030",
];

const CC = {
  teal: "#2ECFAA", orange: "#E87830", blue: "#7BAED0",
  purple: "#a78bfa", red: "#f87171", amber: "#F5B731",
  txt3: "#4d6d8a", txt2: "#8daec8",
};

// ═════════════════════════════════════════════════════════════════════════════
// STATIC GEO DATA — from Walmart Scintilla store geography
// ═════════════════════════════════════════════════════════════════════════════
const WM_DATA = {
  "48":{name:"Texas",abbr:"TX",pos:41403,qty:1660,traited:604,zero_oh:4,one_oh:4,risk:3,tier:8,pct:22.77,dps:68.55},
  "12":{name:"Florida",abbr:"FL",pos:26608,qty:1123,traited:400,zero_oh:2,one_oh:4,risk:2,tier:6,pct:14.63,dps:66.52},
  "01":{name:"Alabama",abbr:"AL",pos:14360,qty:568,traited:202,zero_oh:2,one_oh:4,risk:2,tier:4,pct:7.9,dps:71.09},
  "29":{name:"Missouri",abbr:"MO",pos:13700,qty:543,traited:182,zero_oh:2,one_oh:4,risk:1,tier:4,pct:7.53,dps:75.28},
  "40":{name:"Oklahoma",abbr:"OK",pos:11782,qty:456,traited:190,zero_oh:0,one_oh:1,risk:0,tier:4,pct:6.48,dps:62.01},
  "39":{name:"Ohio",abbr:"OH",pos:11449,qty:421,traited:160,zero_oh:1,one_oh:3,risk:0,tier:4,pct:6.3,dps:71.56},
  "53":{name:"Washington",abbr:"WA",pos:9617,qty:348,traited:128,zero_oh:0,one_oh:0,risk:0,tier:4,pct:5.29,dps:75.13},
  "37":{name:"North Carolina",abbr:"NC",pos:8460,qty:373,traited:139,zero_oh:3,one_oh:2,risk:3,tier:3,pct:4.65,dps:60.86},
  "06":{name:"California",abbr:"CA",pos:8380,qty:331,traited:145,zero_oh:4,one_oh:2,risk:4,tier:3,pct:4.61,dps:57.8},
  "47":{name:"Tennessee",abbr:"TN",pos:7571,qty:303,traited:100,zero_oh:2,one_oh:2,risk:2,tier:3,pct:4.16,dps:75.71},
  "05":{name:"Arkansas",abbr:"AR",pos:5319,qty:260,traited:102,zero_oh:1,one_oh:2,risk:1,tier:3,pct:2.92,dps:52.14},
  "04":{name:"Arizona",abbr:"AZ",pos:5068,qty:186,traited:58,zero_oh:1,one_oh:0,risk:1,tier:3,pct:2.79,dps:87.38},
  "13":{name:"Georgia",abbr:"GA",pos:4661,qty:168,traited:67,zero_oh:3,one_oh:0,risk:3,tier:2,pct:2.56,dps:69.57},
  "21":{name:"Kentucky",abbr:"KY",pos:1769,qty:50,traited:13,zero_oh:0,one_oh:0,risk:0,tier:1,pct:0.97,dps:136.07},
  "19":{name:"Iowa",abbr:"IA",pos:1608,qty:37,traited:7,zero_oh:0,one_oh:0,risk:0,tier:1,pct:0.88,dps:229.7},
  "32":{name:"Nevada",abbr:"NV",pos:1470,qty:34,traited:12,zero_oh:0,one_oh:0,risk:0,tier:1,pct:0.81,dps:122.53},
  "55":{name:"Wisconsin",abbr:"WI",pos:1350,qty:40,traited:9,zero_oh:0,one_oh:0,risk:0,tier:1,pct:0.74,dps:150.03},
  "15":{name:"Hawaii",abbr:"HI",pos:1200,qty:27,traited:3,zero_oh:0,one_oh:0,risk:0,tier:1,pct:0.66,dps:400.11},
  "11":{name:"Washington D.C.",abbr:"DC",pos:947,qty:31,traited:9,zero_oh:0,one_oh:0,risk:0,tier:1,pct:0.52,dps:105.21},
  "08":{name:"Colorado",abbr:"CO",pos:881,qty:26,traited:7,zero_oh:0,one_oh:1,risk:0,tier:1,pct:0.48,dps:125.91},
  "34":{name:"New Jersey",abbr:"NJ",pos:776,qty:24,traited:6,zero_oh:0,one_oh:0,risk:0,tier:0,pct:0.43,dps:129.3},
  "38":{name:"North Dakota",abbr:"ND",pos:750,qty:17,traited:2,zero_oh:0,one_oh:0,risk:0,tier:0,pct:0.41,dps:375.1},
  "20":{name:"Kansas",abbr:"KS",pos:718,qty:23,traited:7,zero_oh:0,one_oh:0,risk:0,tier:0,pct:0.39,dps:102.51},
  "49":{name:"Utah",abbr:"UT",pos:525,qty:17,traited:4,zero_oh:0,one_oh:1,risk:0,tier:0,pct:0.29,dps:131.18},
  "45":{name:"South Carolina",abbr:"SC",pos:469,qty:8,traited:2,zero_oh:1,one_oh:0,risk:1,tier:0,pct:0.26,dps:234.74},
  "28":{name:"Mississippi",abbr:"MS",pos:418,qty:13,traited:2,zero_oh:1,one_oh:0,risk:0,tier:0,pct:0.23,dps:208.88},
  "10":{name:"Delaware",abbr:"DE",pos:415,qty:12,traited:1,zero_oh:0,one_oh:0,risk:0,tier:0,pct:0.23,dps:414.76},
  "18":{name:"Indiana",abbr:"IN",pos:120,qty:4,traited:5,zero_oh:0,one_oh:0,risk:0,tier:0,pct:0.07,dps:23.99},
  "35":{name:"New Mexico",abbr:"NM",pos:44,qty:4,traited:1,zero_oh:1,one_oh:0,risk:1,tier:0,pct:0.02,dps:43.88},
  "22":{name:"Louisiana",abbr:"LA",pos:0,qty:0,traited:1,zero_oh:1,one_oh:0,risk:1,tier:0,pct:0.0,dps:0.0},
  "41":{name:"Oregon",abbr:"OR",pos:0,qty:0,traited:1,zero_oh:1,one_oh:0,risk:1,tier:0,pct:0.0,dps:0.0},
  "42":{name:"Pennsylvania",abbr:"PA",pos:0,qty:0,traited:1,zero_oh:1,one_oh:0,risk:1,tier:0,pct:0.0,dps:0.0},
};

const TOTAL_POS = 181838;

const CITY_DATA = [
  {city:"LAS VEGAS",state:"NV",pos:1261,qty:28,stores:8,dps:157.56,pct:0.693,zero_oh:0,one_oh:0,risk:0,lat:36.17,lng:-115.14},
  {city:"NAPLES",state:"FL",pos:1198,qty:30,stores:5,dps:239.67,pct:0.659,zero_oh:0,one_oh:0,risk:0,lat:26.14,lng:-81.80},
  {city:"LEXINGTON",state:"KY",pos:1119,qty:32,stores:7,dps:159.83,pct:0.615,zero_oh:0,one_oh:0,risk:0,lat:38.04,lng:-84.50},
  {city:"MESA",state:"AZ",pos:1088,qty:36,stores:7,dps:155.38,pct:0.598,zero_oh:0,one_oh:0,risk:0,lat:33.42,lng:-111.83},
  {city:"AURORA",state:"CO",pos:859,qty:24,stores:6,dps:143.24,pct:0.473,zero_oh:0,one_oh:0,risk:0,lat:39.73,lng:-104.83},
  {city:"AZLE",state:"TX",pos:859,qty:27,stores:1,dps:858.7,pct:0.472,zero_oh:0,one_oh:0,risk:0,lat:32.90,lng:-97.54},
  {city:"MADISON",state:"WI",pos:853,qty:32,stores:7,dps:121.91,pct:0.469,zero_oh:0,one_oh:0,risk:0,lat:43.07,lng:-89.40},
  {city:"SAINT JOHNS",state:"AZ",pos:785,qty:27,stores:2,dps:392.46,pct:0.432,zero_oh:0,one_oh:0,risk:0,lat:34.50,lng:-109.37},
  {city:"WASHINGTON",state:"DC",pos:777,qty:27,stores:7,dps:110.99,pct:0.427,zero_oh:0,one_oh:0,risk:0,lat:38.91,lng:-77.04},
  {city:"MIDDLETOWN",state:"OH",pos:768,qty:16,stores:2,dps:383.79,pct:0.422,zero_oh:0,one_oh:0,risk:0,lat:39.52,lng:-84.39},
  {city:"CUMMING",state:"GA",pos:759,qty:17,stores:3,dps:252.9,pct:0.417,zero_oh:0,one_oh:0,risk:0,lat:34.21,lng:-84.14},
  {city:"YUMA",state:"AZ",pos:730,qty:19,stores:3,dps:243.18,pct:0.401,zero_oh:0,one_oh:0,risk:0,lat:32.69,lng:-114.62},
  {city:"PRINCETON",state:"NJ",pos:714,qty:21,stores:5,dps:142.77,pct:0.393,zero_oh:0,one_oh:0,risk:0,lat:40.36,lng:-74.66},
  {city:"FREMONT",state:"CA",pos:689,qty:16,stores:4,dps:172.2,pct:0.379,zero_oh:0,one_oh:1,risk:0,lat:37.55,lng:-121.98},
  {city:"WOODSTOCK",state:"GA",pos:688,qty:15,stores:3,dps:229.43,pct:0.379,zero_oh:0,one_oh:0,risk:0,lat:34.10,lng:-84.52},
  {city:"JACKSONVILLE",state:"FL",pos:672,qty:24,stores:7,dps:95.93,pct:0.369,zero_oh:1,one_oh:0,risk:1,lat:30.33,lng:-81.66},
  {city:"WICHITA",state:"KS",pos:663,qty:18,stores:6,dps:110.45,pct:0.364,zero_oh:0,one_oh:0,risk:0,lat:37.69,lng:-97.33},
  {city:"SPRINGFIELD",state:"MO",pos:661,qty:26,stores:8,dps:82.59,pct:0.363,zero_oh:1,one_oh:0,risk:0,lat:37.21,lng:-93.29},
  {city:"LUBBOCK",state:"TX",pos:644,qty:20,stores:5,dps:128.71,pct:0.354,zero_oh:0,one_oh:0,risk:0,lat:33.58,lng:-101.86},
  {city:"N RICHLAND HILLS",state:"TX",pos:641,qty:20,stores:2,dps:320.39,pct:0.352,zero_oh:0,one_oh:0,risk:0,lat:32.83,lng:-97.23},
];

const REGIONS = {
  "South Central": {states:["TX","OK","AR","LA","NM"],color:"#f0b800"},
  "Southeast":     {states:["FL","AL","GA","TN","NC","SC","MS"],color:"#d46e00"},
  "Midwest":       {states:["MO","OH","IA","WI","KS","ND","IN"],color:"#1aad5e"},
  "West":          {states:["CA","WA","AZ","CO","NV","HI","OR","UT"],color:"#0568b0"},
  "East":          {states:["KY","PA","DE","DC","NJ"],color:"#7BAED0"},
};

// Compute region totals
Object.keys(REGIONS).forEach((r) => {
  let pos = 0, traited = 0, qty = 0, zero_oh = 0, risk = 0, one_oh = 0;
  REGIONS[r].states.forEach((st) => {
    const d = Object.values(WM_DATA).find((x) => x.abbr === st);
    if (d) { pos += d.pos; traited += d.traited; qty += d.qty; zero_oh += d.zero_oh; risk += d.risk; one_oh += d.one_oh; }
  });
  Object.assign(REGIONS[r], { pos, traited, qty, zero_oh, one_oh, risk, dps: traited ? Math.round(pos / traited * 100) / 100 : 0 });
});

const STATE_TO_REGION = {};
Object.entries(REGIONS).forEach(([r, d]) => d.states.forEach((st) => (STATE_TO_REGION[st] = r)));

// Store risk data
const ZERO_OH = [
  {s:5262,n:"PELHAM",st:"AL",pos:55,oh:0,oo:0,it:0,ins:0,risk:1},
  {s:758,n:"AMERICUS",st:"GA",pos:44,oh:0,oo:0,it:0,ins:0,risk:1},
  {s:3427,n:"ARTESIA",st:"NM",pos:44,oh:0,oo:0,it:0,ins:0,risk:1},
  {s:1519,n:"JACKSON",st:"MS",pos:44,oh:0,oo:1,it:1,ins:0,risk:0},
  {s:1516,n:"BORGER",st:"TX",pos:33,oh:0,oo:0,it:0,ins:0,risk:1},
  {s:2440,n:"SYLVA",st:"NC",pos:33,oh:0,oo:0,it:0,ins:0,risk:1},
  {s:1408,n:"TALLAHASSEE W TENNESSEE",st:"FL",pos:33,oh:0,oo:0,it:0,ins:0,risk:1},
  {s:2099,n:"PASO ROBLES",st:"CA",pos:23,oh:0,oo:0,it:0,ins:0,risk:1},
  {s:313,n:"HIGH RIDGE",st:"MO",pos:22,oh:0,oo:0,it:0,ins:0,risk:1},
  {s:2306,n:"NORTHPORT",st:"AL",pos:22,oh:0,oo:0,it:0,ins:0,risk:1},
  {s:7281,n:"SAN ANGELO S BRYANT BLVD",st:"TX",pos:11,oh:0,oo:0,it:0,ins:0,risk:1},
  {s:5151,n:"ROME CARTERSVILLE HWY SE",st:"GA",pos:11,oh:0,oo:0,it:0,ins:0,risk:1},
  {s:1358,n:"WALTERBORO",st:"SC",pos:11,oh:0,oo:0,it:0,ins:0,risk:1},
  {s:1502,n:"ROANOKE RAPIDS",st:"NC",pos:5,oh:0,oo:0,it:0,ins:0,risk:1},
  {s:690,n:"ELIZABETHTON",st:"TN",pos:0,oh:0,oo:0,it:0,ins:0,risk:1},
  {s:3641,n:"SPRINGFIELD",st:"MO",pos:0,oh:0,oo:1,it:1,ins:0,risk:0},
  {s:5244,n:"LITTLE ROCK CANTRELL RD",st:"AR",pos:0,oh:0,oo:0,it:0,ins:0,risk:1},
  {s:3493,n:"MARTINEZ",st:"CA",pos:0,oh:0,oo:0,it:0,ins:0,risk:1},
  {s:542,n:"HOUMA",st:"LA",pos:0,oh:0,oo:0,it:0,ins:0,risk:1},
  {s:1842,n:"GREENSBORO W WENDOVER AVE",st:"NC",pos:0,oh:0,oo:0,it:0,ins:0,risk:1},
  {s:1325,n:"TUCSON E WETMORE RD",st:"AZ",pos:0,oh:0,oo:0,it:0,ins:0,risk:1},
  {s:2792,n:"DULUTH",st:"GA",pos:0,oh:0,oo:0,it:0,ins:0,risk:1},
  {s:2492,n:"PENDLETON",st:"OR",pos:0,oh:0,oo:0,it:0,ins:0,risk:1},
  {s:1886,n:"MECHANICSBURG",st:"PA",pos:0,oh:0,oo:0,it:0,ins:0,risk:1},
  {s:1250,n:"CELINA TX",st:"TX",pos:0,oh:0,oo:0,it:4,ins:0,risk:0},
  {s:3522,n:"BALDWIN PARK",st:"CA",pos:0,oh:0,oo:0,it:0,ins:0,risk:1},
  {s:2400,n:"CHILLICOTHE",st:"OH",pos:0,oh:0,oo:2,it:1,ins:0,risk:0},
  {s:1215,n:"CALHOUN",st:"GA",pos:0,oh:0,oo:0,it:0,ins:0,risk:1},
  {s:3480,n:"LEBANON",st:"TN",pos:0,oh:0,oo:0,it:0,ins:0,risk:1},
  {s:1173,n:"JACKSONVILLE BEACH BLVD",st:"FL",pos:0,oh:0,oo:0,it:0,ins:0,risk:1},
  {s:5032,n:"BUENA PARK",st:"CA",pos:0,oh:0,oo:0,it:0,ins:0,risk:1},
];

const ONE_OH = [
  {s:163,n:"NACOGDOCHES N STREET",st:"AR",pos:88,oh:1,oo:0,it:0,ins:100,risk:1},
  {s:767,n:"LAKE CITY",st:"FL",pos:66,oh:1,oo:0,it:0,ins:100,risk:1},
  {s:2753,n:"CLAYTON",st:"UT",pos:55,oh:1,oo:0,it:0,ins:100,risk:1},
  {s:5394,n:"DINUBA",st:"AL",pos:45,oh:1,oo:0,it:0,ins:100,risk:1},
  {s:5296,n:"BARBOURSVILLE",st:"AL",pos:43,oh:1,oo:1,it:1,ins:100,risk:0},
  {s:5215,n:"DELANO",st:"AL",pos:34,oh:1,oo:0,it:0,ins:100,risk:1},
  {s:5139,n:"DIXON",st:"AL",pos:34,oh:1,oo:0,it:0,ins:100,risk:1},
  {s:360,n:"CUSHING",st:"TX",pos:33,oh:1,oo:0,it:0,ins:100,risk:1},
  {s:2809,n:"BUCKHANNON",st:"OH",pos:33,oh:1,oo:0,it:0,ins:100,risk:1},
  {s:1666,n:"CHARLOTTE",st:"NC",pos:33,oh:1,oo:0,it:0,ins:100,risk:1},
  {s:1450,n:"RIPLEY",st:"OK",pos:26,oh:1,oo:1,it:0,ins:100,risk:0},
  {s:2704,n:"RANDLEMAN",st:"OH",pos:22,oh:1,oo:1,it:1,ins:100,risk:0},
  {s:5121,n:"CLINTON",st:"OH",pos:22,oh:1,oo:0,it:0,ins:100,risk:1},
  {s:2569,n:"FLORENCE",st:"AL",pos:22,oh:1,oo:0,it:0,ins:100,risk:1},
  {s:2960,n:"GULFPORT",st:"FL",pos:22,oh:1,oo:0,it:0,ins:100,risk:1},
  {s:4467,n:"CROWLEY",st:"TX",pos:18,oh:1,oo:0,it:0,ins:100,risk:1},
  {s:2614,n:"TULLAHOMA",st:"TN",pos:11,oh:1,oo:0,it:0,ins:100,risk:1},
  {s:2718,n:"WESLACO",st:"MO",pos:11,oh:1,oo:0,it:0,ins:100,risk:1},
  {s:730,n:"PRATTVILLE",st:"MO",pos:11,oh:1,oo:0,it:0,ins:100,risk:1},
  {s:3284,n:"WAYNESVILLE",st:"NC",pos:11,oh:1,oo:0,it:0,ins:100,risk:1},
  {s:3462,n:"OAK GROVE",st:"MO",pos:0,oh:1,oo:0,it:0,ins:100,risk:1},
  {s:2110,n:"COLUMBIA",st:"TN",pos:0,oh:1,oo:1,it:0,ins:100,risk:0},
  {s:3248,n:"GALLATIN",st:"TN",pos:0,oh:1,oo:0,it:0,ins:100,risk:1},
  {s:2167,n:"LAKE WALES",st:"FL",pos:0,oh:1,oo:1,it:0,ins:100,risk:0},
  {s:2765,n:"PEORIA",st:"AZ",pos:0,oh:1,oo:0,it:0,ins:0,risk:1},
  {s:2820,n:"COLORADO SPRINGS S ACADEMY BLVD",st:"CO",pos:0,oh:1,oo:0,it:0,ins:100,risk:1},
  {s:1032,n:"BAYTOWN E FREEWAY",st:"TX",pos:0,oh:1,oo:0,it:0,ins:100,risk:1},
  {s:3279,n:"LAWRENCEBURG",st:"TN",pos:0,oh:1,oo:0,it:0,ins:100,risk:1},
  {s:510,n:"GRIFFIN",st:"CA",pos:0,oh:1,oo:0,it:0,ins:100,risk:1},
  {s:298,n:"JACKSONVILLE",st:"FL",pos:0,oh:1,oo:0,it:0,ins:100,risk:1},
];

// ═════════════════════════════════════════════════════════════════════════════
// HELPER FUNCTIONS
// ═════════════════════════════════════════════════════════════════════════════

const salesColor = (s) => {
  if (s.pos >= 10000) return CC.teal;
  if (s.pos >= 5000) return CC.blue;
  if (s.pos >= 1000) return CC.amber;
  if (s.pos > 0) return CC.txt2;
  return CC.txt3;
};

const fmtK = (v) => v >= 1000 ? "$" + (v / 1000).toFixed(1) + "K" : "$" + v;

const statesSorted = Object.values(WM_DATA).sort((a, b) => b.pos - a.pos);
const maxStatePos = statesSorted[0]?.pos || 1;

// ═════════════════════════════════════════════════════════════════════════════
// SMALL COMPONENTS
// ═════════════════════════════════════════════════════════════════════════════

function Badge({ type, children }) {
  const styles = {
    ok:   { background: "rgba(46,207,170,.14)", color: "#2ECFAA" },
    warn: { background: "rgba(245,183,49,.14)", color: "#F5B731" },
    risk: { background: "rgba(248,113,113,.14)", color: "#f87171" },
    blue: { background: "rgba(123,174,208,.14)", color: "#7BAED0" },
  };
  return (
    <span style={{ ...SG(9, 700), padding: "2px 8px", borderRadius: 99, ...(styles[type] || styles.ok) }}>
      {children}
    </span>
  );
}

function OhBadge({ oh }) {
  if (oh === 0) return <span style={{ ...SG(9, 700), padding: "1px 7px", borderRadius: 4, background: "rgba(248,113,113,.15)", color: CC.red }}>0</span>;
  if (oh === 1) return <span style={{ ...SG(9, 700), padding: "1px 7px", borderRadius: 4, background: "rgba(245,183,49,.15)", color: CC.amber }}>1</span>;
  return <span style={{ ...SG(9, 700), padding: "1px 7px", borderRadius: 4, background: "rgba(46,207,170,.1)", color: CC.teal }}>{oh}</span>;
}

function RiskBadge({ risk, hasInbound }) {
  if (risk === 1) return <span style={{ ...SG(8, 700), padding: "1px 7px", borderRadius: 4, background: "rgba(248,113,113,.12)", color: CC.red }}>CRITICAL</span>;
  if (hasInbound) return <span style={{ ...SG(8, 700), padding: "1px 7px", borderRadius: 4, background: "rgba(123,174,208,.12)", color: CC.blue }}>INBOUND</span>;
  return <span style={{ ...SG(8, 700), padding: "1px 7px", borderRadius: 4, background: "rgba(46,207,170,.08)", color: CC.teal }}>OK</span>;
}

// ═════════════════════════════════════════════════════════════════════════════
// US MAP COMPONENT
// ═════════════════════════════════════════════════════════════════════════════

function USMap({ selectedState, onSelectState, showCities }) {
  const mapRef = useRef(null);
  const svgRef = useRef(null);

  useEffect(() => {
    if (!mapRef.current || !window.d3 || !window.topojson) return;
    if (svgRef.current) return; // already rendered

    const d3 = window.d3;
    const topojson = window.topojson;
    const container = mapRef.current;
    const w = container.clientWidth || 800;
    const h = Math.min(w * 0.58, 500);

    const svg = d3.select(container).append("svg")
      .attr("viewBox", `0 0 ${w} ${h}`)
      .attr("preserveAspectRatio", "xMidYMid meet")
      .style("width", "100%").style("height", "auto")
      .style("background", "#0E1F2D");

    svgRef.current = svg;

    const projection = d3.geoAlbersUsa().fitSize([w - 40, h - 20], { type: "Sphere" })
      .translate([w / 2, h / 2]);
    const path = d3.geoPath().projection(projection);

    d3.json("https://cdn.jsdelivr.net/npm/us-atlas@3/states-10m.json").then((us) => {
      const states = topojson.feature(us, us.objects.states).features;
      svg.selectAll("path.state")
        .data(states)
        .enter().append("path")
        .attr("class", "state")
        .attr("d", path)
        .attr("fill", (d) => {
          const sd = WM_DATA[String(d.id).padStart(2, "0")];
          return sd ? TIER_COLORS[sd.tier] : "rgba(14,31,45,0.8)";
        })
        .attr("stroke", "rgba(30,50,72,0.5)")
        .attr("stroke-width", 0.5)
        .style("cursor", "pointer")
        .on("click", function (event, d) {
          const sd = WM_DATA[String(d.id).padStart(2, "0")];
          if (sd && onSelectState) onSelectState(sd.abbr);
        })
        .on("mouseover", function () {
          d3.select(this).attr("stroke", "rgba(255,255,255,0.6)").attr("stroke-width", 1.0);
        })
        .on("mouseout", function () {
          d3.select(this).attr("stroke", "rgba(30,50,72,0.5)").attr("stroke-width", 0.5);
        })
        .append("title")
        .text((d) => {
          const sd = WM_DATA[String(d.id).padStart(2, "0")];
          return sd ? `${sd.name}: $${sd.pos.toLocaleString()} | ${sd.traited} stores | $${sd.dps.toFixed(0)}/store` : "";
        });

      // City dots
      if (showCities) {
        svg.selectAll("circle.city")
          .data(CITY_DATA.filter((c) => {
            const pt = projection([c.lng, c.lat]);
            return pt != null;
          }))
          .enter().append("circle")
          .attr("class", "city")
          .attr("cx", (d) => projection([d.lng, d.lat])?.[0])
          .attr("cy", (d) => projection([d.lng, d.lat])?.[1])
          .attr("r", (d) => Math.max(2, Math.min(6, d.pos / 300)))
          .attr("fill", CC.blue)
          .attr("opacity", 0.7)
          .attr("stroke", "#fff")
          .attr("stroke-width", 0.3)
          .append("title")
          .text((d) => `${d.city}, ${d.state}: $${d.pos.toLocaleString()} | ${d.stores} stores`);
      }
    });

    return () => {
      if (svgRef.current) {
        svgRef.current.remove();
        svgRef.current = null;
      }
    };
  }, [showCities]);

  return (
    <div ref={mapRef} style={{ width: "100%", minHeight: 300, background: "#0E1F2D", position: "relative" }} />
  );
}

// ═════════════════════════════════════════════════════════════════════════════
// SUB-TAB 1: GEOGRAPHY MAP
// ═════════════════════════════════════════════════════════════════════════════

function GeographyMapTab() {
  const [selectedState, setSelectedState] = useState(null);
  const [sideTab, setSideTab] = useState("states");
  const [showCities, setShowCities] = useState(true);

  const totalZeroOh = ZERO_OH.length;
  const totalCritical = ZERO_OH.filter((s) => s.risk === 1).length;
  const statesActive = Object.values(WM_DATA).filter((s) => s.pos > 0).length;
  const topState = statesSorted[0];
  const avgDps = (TOTAL_POS / Object.values(WM_DATA).reduce((a, s) => a + s.traited, 0)).toFixed(1);

  const regNames = Object.keys(REGIONS);
  const regSorted = regNames.sort((a, b) => REGIONS[b].pos - REGIONS[a].pos);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
      {/* KPI Cards */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(6, 1fr)", gap: 8 }}>
        <KPICard label="L4W POS Sales" value={fmtK(TOTAL_POS)} color={CC.teal} />
        <KPICard label="States Active" value={statesActive} color={CC.blue} />
        <KPICard label="Top State" value={topState?.abbr || "—"} color="#f0b800" />
        <KPICard label="Avg $/Store" value={"$" + avgDps} color={CC.orange} />
        <KPICard label="0-OH Stores" value={totalZeroOh} color={CC.amber} />
        <KPICard label="Critical · No Order" value={totalCritical} color={CC.red} />
      </div>

      {/* Map + Side Panel */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 300px", gap: 12 }}>
        {/* Map Card */}
        <div style={{ background: "var(--surf, #0c1a2e)", border: "1px solid var(--brd)", borderRadius: 13, overflow: "hidden" }}>
          {/* Map Controls */}
          <div style={{ display: "flex", alignItems: "center", gap: 7, padding: "9px 16px", borderBottom: "1px solid var(--brd)", flexWrap: "wrap", background: "var(--card2, #1A2D42)" }}>
            <span style={{ ...SG(8.5, 700), textTransform: "uppercase", letterSpacing: ".1em", color: "var(--txt3)" }}>View</span>
            <span style={{ ...SG(10, 700), padding: "5px 12px", borderRadius: 8, background: "var(--atab, #1a4060)", color: "#fff" }}>State: Sales $</span>
            <div style={{ width: 1, height: 20, background: "var(--brd)", flexShrink: 0 }} />
            <span style={{ ...SG(8.5, 700), textTransform: "uppercase", letterSpacing: ".1em", color: "var(--txt3)" }}>Overlay</span>
            <button
              onClick={() => setShowCities(!showCities)}
              style={{
                ...SG(9, 700), padding: "4px 10px", borderRadius: 6,
                border: showCities ? "1px solid var(--acc1)" : "1px solid var(--brd2, #2a4060)",
                background: showCities ? "rgba(46,207,170,.1)" : "transparent",
                color: showCities ? CC.teal : "var(--txt3)", cursor: "pointer",
              }}
            >
              Cities {showCities ? "ON" : "OFF"}
            </button>
            <Badge type="ok">● Live</Badge>
          </div>

          <USMap selectedState={selectedState} onSelectState={setSelectedState} showCities={showCities} />

          {/* Legend */}
          <div style={{ display: "flex", alignItems: "center", gap: 5, padding: "7px 16px", borderTop: "1px solid var(--brd)", background: "var(--card2, #1A2D42)", flexWrap: "wrap" }}>
            <span style={{ ...SG(8), color: "var(--txt3)" }}>$0</span>
            <div style={{ display: "flex", height: 10, borderRadius: 5, overflow: "hidden", width: 200, border: "1px solid rgba(255,255,255,.05)" }}>
              {TIER_COLORS.map((c, i) => <div key={i} style={{ flex: 1, background: c }} />)}
            </div>
            <span style={{ ...SG(8), color: "var(--txt3)" }}>$41K+</span>
            <div style={{ display: "flex", alignItems: "center", gap: 4, marginLeft: 10 }}>
              <div style={{ width: 7, height: 7, borderRadius: "50%", background: "#f0b800" }} /><span style={{ ...SG(8), color: "var(--txt3)" }}>Highest</span>
            </div>
            <div style={{ display: "flex", alignItems: "center", gap: 4, marginLeft: 10 }}>
              <div style={{ width: 7, height: 7, borderRadius: "50%", background: CC.red }} /><span style={{ ...SG(8), color: "var(--txt3)" }}>Risk</span>
            </div>
            <div style={{ display: "flex", alignItems: "center", gap: 4, marginLeft: 10 }}>
              <div style={{ width: 7, height: 7, borderRadius: "50%", background: CC.blue }} /><span style={{ ...SG(8), color: "var(--txt3)" }}>City</span>
            </div>
          </div>
        </div>

        {/* Side Panel */}
        <div style={{ background: "var(--surf, #0c1a2e)", border: "1px solid var(--brd)", borderRadius: 13, overflow: "hidden", display: "flex", flexDirection: "column" }}>
          {/* Side Tabs */}
          <div style={{ display: "flex", borderBottom: "1px solid var(--brd)", background: "var(--card2, #1A2D42)" }}>
            {["states", "cities", "regions"].map((t) => (
              <div
                key={t}
                onClick={() => setSideTab(t)}
                style={{
                  flex: 1, padding: 7, textAlign: "center", ...SG(9, 700),
                  textTransform: "uppercase", letterSpacing: ".07em",
                  color: sideTab === t ? CC.teal : "var(--txt3)",
                  cursor: "pointer", borderBottom: sideTab === t ? "2px solid " + CC.teal : "2px solid transparent",
                }}
              >
                {t.charAt(0).toUpperCase() + t.slice(1)}
              </div>
            ))}
          </div>

          {/* States Panel */}
          {sideTab === "states" && (
            <div style={{ overflowY: "auto", maxHeight: 600 }}>
              {statesSorted.slice(0, 20).map((s, i) => {
                const bw = (s.pos / maxStatePos * 100).toFixed(0);
                const bc = s.risk >= 3 ? CC.red : s.risk >= 1 ? CC.amber : salesColor(s);
                return (
                  <div
                    key={s.abbr}
                    onClick={() => setSelectedState(s.abbr)}
                    style={{
                      display: "grid", gridTemplateColumns: "16px 1fr 70px 42px", gap: 4,
                      alignItems: "center", padding: "6px 12px",
                      borderBottom: "1px solid rgba(30,50,72,.5)", cursor: "pointer",
                      background: selectedState === s.abbr ? "rgba(46,207,170,.06)" : "transparent",
                      borderLeft: selectedState === s.abbr ? "2px solid " + CC.teal : "2px solid transparent",
                    }}
                  >
                    <span style={{ ...SG(8.5, 700), color: "var(--txt3)" }}>{i + 1}</span>
                    <div>
                      <div style={{ fontSize: 10.5, fontWeight: 600, color: "var(--txt)" }}>
                        {s.abbr}{s.risk > 0 && <span style={{ color: CC.red, fontSize: 7.5 }}> ⚠{s.risk}</span>}
                      </div>
                      <div style={{ height: 3, background: "var(--brd)", borderRadius: 2, overflow: "hidden", marginTop: 3 }}>
                        <div style={{ height: 3, borderRadius: 2, width: bw + "%", background: bc, transition: "width .5s" }} />
                      </div>
                      <div style={{ ...SG(7.5), color: "var(--txt3)", marginTop: 1 }}>{s.traited} str · <strong style={{ color: bc }}>${s.dps.toFixed(0)}/str</strong></div>
                    </div>
                    <span style={{ ...DM(13), textAlign: "right", color: bc }}>
                      {s.pos >= 1000 ? "$" + (s.pos / 1000).toFixed(0) + "K" : "$" + s.pos}
                    </span>
                    <span style={{ ...SG(8, 700), textAlign: "right", color: "var(--txt3)" }}>{s.pct.toFixed(1)}%</span>
                  </div>
                );
              })}
            </div>
          )}

          {/* Cities Panel */}
          {sideTab === "cities" && (
            <div style={{ overflowY: "auto", maxHeight: 600 }}>
              {CITY_DATA.slice(0, 20).map((c, i) => {
                const bc = salesColor({ pos: c.pos });
                return (
                  <div
                    key={i}
                    style={{
                      display: "grid", gridTemplateColumns: "16px 1fr 68px 44px", gap: 4,
                      alignItems: "center", padding: "6px 12px",
                      borderBottom: "1px solid rgba(30,50,72,.5)",
                    }}
                  >
                    <span style={{ ...SG(8.5, 700), color: "var(--txt3)" }}>{i + 1}</span>
                    <div>
                      <div style={{ fontSize: 10.5, fontWeight: 600, color: "var(--txt)" }}>{c.city}</div>
                      <div style={{ ...SG(7.5), color: "var(--txt3)" }}>{c.state} · {c.stores} stores</div>
                    </div>
                    <span style={{ ...DM(13), textAlign: "right", color: bc }}>${c.pos >= 1000 ? (c.pos / 1000).toFixed(1) + "K" : c.pos}</span>
                    <span style={{ ...SG(8, 700), textAlign: "right", color: "var(--txt3)" }}>${c.dps.toFixed(0)}</span>
                  </div>
                );
              })}
            </div>
          )}

          {/* Regions Panel */}
          {sideTab === "regions" && (
            <div style={{ overflowY: "auto", maxHeight: 600, padding: "8px 12px" }}>
              {regSorted.map((r) => {
                const d = REGIONS[r];
                return (
                  <div key={r} style={{ padding: "10px 12px", borderBottom: "1px solid rgba(30,50,72,.5)", cursor: "pointer", borderRadius: 8, marginBottom: 4 }}>
                    <div style={{ ...SG(10, 700), color: d.color }}>{r}</div>
                    <div style={{ ...SG(7.5), color: "var(--txt3)", marginBottom: 6 }}>{d.states.join(" · ")}</div>
                    <div style={{ ...DM(18), color: d.color }}>${(d.pos / 1000).toFixed(1)}K</div>
                    <div style={{ ...SG(7.5), color: "var(--txt3)", marginTop: 2 }}>
                      {d.traited} stores · <strong style={{ color: d.color }}>${d.dps.toFixed(0)}/store</strong>
                      {d.risk > 0 ? <span style={{ color: CC.red }}> · ⚠ {d.risk} critical</span> : " · ✓ OK"}
                    </div>
                    <div style={{ height: 3, borderRadius: 2, marginTop: 8, overflow: "hidden" }}>
                      <div style={{ height: 3, borderRadius: 2, background: d.color, width: (d.pos / REGIONS[regSorted[0]].pos * 100).toFixed(0) + "%" }} />
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </div>
      </div>

      {/* Bottom Charts */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 12 }}>
        <Card>
          <CardHdr title="Top 20 States · L4W Sales" right={<Badge type="ok">Sales + $/Store</Badge>} />
          <ChartCanvas
            type="bar"
            height={240}
            configKey="geo-state-sales"
            labels={statesSorted.slice(0, 20).map((s) => s.abbr)}
            datasets={[
              {
                label: "L4W Sales $",
                data: statesSorted.slice(0, 20).map((s) => s.pos),
                backgroundColor: statesSorted.slice(0, 20).map((s) => TIER_COLORS[s.tier] + "55"),
                borderColor: statesSorted.slice(0, 20).map((s) => TIER_COLORS[s.tier]),
                borderWidth: 2, borderRadius: 5,
              },
            ]}
          />
        </Card>
        <Card>
          <CardHdr title="Top 20 Cities · L4W Sales" right={<Badge type="blue">Avg $/Store</Badge>} />
          <ChartCanvas
            type="bar"
            height={240}
            configKey="geo-city-sales"
            labels={CITY_DATA.slice(0, 20).map((c) => c.city.length > 10 ? c.city.slice(0, 10) + "…" : c.city)}
            datasets={[
              {
                label: "L4W Sales $",
                data: CITY_DATA.slice(0, 20).map((c) => c.pos),
                backgroundColor: CC.blue + "33",
                borderColor: CC.blue,
                borderWidth: 2, borderRadius: 5,
              },
            ]}
          />
        </Card>
        <Card>
          <CardHdr title="Regional Sales Mix" right={<Badge type="ok">5 Regions</Badge>} />
          <ChartCanvas
            type="doughnut"
            height={240}
            configKey="geo-region-mix"
            labels={regSorted}
            datasets={[
              {
                data: regSorted.map((r) => REGIONS[r].pos),
                backgroundColor: regSorted.map((r) => REGIONS[r].color),
                borderColor: "#0E1F2D",
                borderWidth: 2,
              },
            ]}
          />
        </Card>
      </div>
    </div>
  );
}

// ═════════════════════════════════════════════════════════════════════════════
// SUB-TAB 2: REGIONS
// ═════════════════════════════════════════════════════════════════════════════

function RegionsTab() {
  const [activeRegion, setActiveRegion] = useState(null);
  const regNames = Object.keys(REGIONS);
  const regSorted = regNames.sort((a, b) => REGIONS[b].pos - REGIONS[a].pos);

  const filteredStates = activeRegion
    ? Object.values(WM_DATA).filter((s) => REGIONS[activeRegion]?.states.includes(s.abbr)).sort((a, b) => b.pos - a.pos)
    : statesSorted;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
      {/* Region Filter Cards */}
      <div style={{ background: "var(--surf, #0c1a2e)", border: "1px solid var(--brd)", borderRadius: 13, overflow: "hidden" }}>
        <div style={{ padding: "9px 16px", display: "flex", justifyContent: "space-between", alignItems: "center", borderBottom: "1px solid var(--brd)" }}>
          <span style={{ ...SG(12, 700), color: "var(--txt)" }}>Select Region to Filter</span>
          <Badge type="blue">Click any card</Badge>
        </div>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(5, 1fr)", gap: 8, padding: "12px 16px" }}>
          {regSorted.map((r) => {
            const d = REGIONS[r];
            return (
              <div
                key={r}
                onClick={() => setActiveRegion(activeRegion === r ? null : r)}
                style={{
                  background: activeRegion === r ? "rgba(46,207,170,.05)" : "rgba(255,255,255,.05)",
                  border: activeRegion === r ? "1px solid " + CC.teal : "1px solid var(--brd)",
                  borderRadius: 9, padding: "10px 12px", cursor: "pointer", transition: "all .15s",
                }}
              >
                <div style={{ ...SG(10, 700), color: d.color }}>{r}</div>
                <div style={{ ...SG(7.5), color: "var(--txt3)", marginBottom: 6, lineHeight: 1.5 }}>{d.states.join(" · ")}</div>
                <div style={{ ...DM(18), color: d.color }}>${(d.pos / 1000).toFixed(1)}K</div>
                <div style={{ ...SG(7.5), color: "var(--txt3)", marginTop: 2 }}>
                  {d.traited} stores · <strong style={{ color: d.color }}>${d.dps.toFixed(0)}/store</strong>
                  {d.risk > 0 ? <span style={{ color: CC.red }}> · ⚠ {d.risk} critical</span> : " · ✓ OK"}
                </div>
                <div style={{ height: 3, borderRadius: 2, marginTop: 8, overflow: "hidden" }}>
                  <div style={{ height: 3, borderRadius: 2, background: d.color, width: (d.pos / REGIONS[regSorted[0]].pos * 100).toFixed(0) + "%" }} />
                </div>
              </div>
            );
          })}
        </div>
      </div>

      {/* Region Charts */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
        <Card>
          <CardHdr title="Region Sales · TY L4W" right={<Badge type="ok">With Avg $/Store line</Badge>} />
          <ChartCanvas
            type="bar"
            height={250}
            configKey={`reg-sales-${activeRegion || "all"}`}
            labels={regSorted}
            datasets={[
              {
                label: "L4W Sales $",
                data: regSorted.map((r) => REGIONS[r].pos),
                backgroundColor: regSorted.map((r) => REGIONS[r].color + "33"),
                borderColor: regSorted.map((r) => REGIONS[r].color),
                borderWidth: 2, borderRadius: 5,
              },
              {
                label: "Avg $/Store",
                data: regSorted.map((r) => REGIONS[r].dps),
                type: "line",
                borderColor: CC.amber,
                backgroundColor: "transparent",
                borderWidth: 2, pointRadius: 5, pointBackgroundColor: CC.amber,
                yAxisID: "y1",
                tension: 0.3,
              },
            ]}
          />
        </Card>
        <Card>
          <CardHdr title="Traited Stores + Risk by Region" right={<Badge type="warn">0-OH exposure</Badge>} />
          <ChartCanvas
            type="bar"
            height={250}
            configKey="reg-stores"
            labels={regSorted}
            datasets={[
              {
                label: "Traited",
                data: regSorted.map((r) => REGIONS[r].traited),
                backgroundColor: CC.blue + "55",
                borderColor: CC.blue,
                borderWidth: 2, borderRadius: 5,
              },
              {
                label: "0-OH",
                data: regSorted.map((r) => REGIONS[r].zero_oh),
                backgroundColor: CC.red + "55",
                borderColor: CC.red,
                borderWidth: 2, borderRadius: 5,
              },
            ]}
          />
        </Card>
      </div>

      {/* State Detail Table */}
      <Card>
        <CardHdr
          title={activeRegion ? `${activeRegion} States` : "All States · Regional Detail"}
          right={<Badge type="ok">Sorted by L4W Sales</Badge>}
        />
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
            <thead>
              <tr style={{ borderBottom: "1px solid var(--brd)" }}>
                {["State", "Region", "L4W Sales", "% Total", "Traited", "$/Store", "Units", "0-OH", "1-OH", "Critical", "Risk"].map((h, i) => (
                  <th key={h} style={{ ...SG(7.5, 700), textTransform: "uppercase", letterSpacing: ".07em", color: "var(--txt3)", padding: "8px 12px", textAlign: i >= 2 ? "right" : "left", whiteSpace: "nowrap", background: "var(--card2, #1A2D42)" }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {filteredStates.map((s) => {
                const region = STATE_TO_REGION[s.abbr] || "—";
                const regColor = REGIONS[region]?.color || "var(--txt3)";
                return (
                  <tr key={s.abbr} style={{ borderBottom: "1px solid rgba(30,50,72,.5)" }}>
                    <td style={{ padding: "7px 12px", fontWeight: 700, color: "var(--txt)" }}>{s.abbr}</td>
                    <td style={{ padding: "7px 12px", color: regColor, fontWeight: 700, fontSize: 10 }}>{region}</td>
                    <td style={{ padding: "7px 12px", textAlign: "right", fontWeight: 700, color: salesColor(s) }}>{fmtK(s.pos)}</td>
                    <td style={{ padding: "7px 12px", textAlign: "right", color: "var(--txt2)" }}>{s.pct.toFixed(1)}%</td>
                    <td style={{ padding: "7px 12px", textAlign: "right", color: "var(--txt)" }}>{s.traited}</td>
                    <td style={{ padding: "7px 12px", textAlign: "right", fontWeight: 700, color: CC.teal }}>${s.dps.toFixed(0)}</td>
                    <td style={{ padding: "7px 12px", textAlign: "right", color: "var(--txt2)" }}>{fN(s.qty)}</td>
                    <td style={{ padding: "7px 12px", textAlign: "right", color: s.zero_oh > 0 ? CC.red : "var(--txt3)", fontWeight: s.zero_oh > 0 ? 700 : 400 }}>{s.zero_oh}</td>
                    <td style={{ padding: "7px 12px", textAlign: "right", color: s.one_oh > 0 ? CC.amber : "var(--txt3)", fontWeight: s.one_oh > 0 ? 700 : 400 }}>{s.one_oh}</td>
                    <td style={{ padding: "7px 12px", textAlign: "right", color: s.risk > 0 ? CC.red : "var(--txt3)", fontWeight: s.risk > 0 ? 700 : 400 }}>{s.risk}</td>
                    <td style={{ padding: "7px 12px" }}>
                      {s.risk >= 3 ? <span style={{ ...SG(8, 700), padding: "1px 7px", borderRadius: 4, background: "rgba(248,113,113,.12)", color: CC.red }}>HIGH</span>
                        : s.risk >= 1 ? <span style={{ ...SG(8, 700), padding: "1px 7px", borderRadius: 4, background: "rgba(245,183,49,.12)", color: CC.amber }}>WATCH</span>
                        : <span style={{ ...SG(8, 700), padding: "1px 7px", borderRadius: 4, background: "rgba(46,207,170,.08)", color: CC.teal }}>OK</span>}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </Card>
    </div>
  );
}

// ═════════════════════════════════════════════════════════════════════════════
// SUB-TAB 3: STORE DETAIL
// ═════════════════════════════════════════════════════════════════════════════

function StoreDetailTab({ filters }) {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [data, setData] = useState(null);
  const [search, setSearch] = useState("");
  const [stateFilter, setStateFilter] = useState("");
  const [ohFilter, setOhFilter] = useState("");
  const [sortBy, setSortBy] = useState("posSalesTy");
  const [sortDir, setSortDir] = useState("desc");

  useEffect(() => {
    (async () => {
      try {
        setLoading(true);
        setError(null);
        const result = await api.walmartStoreGeography(filters);
        setData(result);
      } catch (err) {
        setError(err.message);
      } finally {
        setLoading(false);
      }
    })();
  }, [filters.division, filters.customer]);

  if (loading) return <Card><p style={{ ...SG(12), color: "var(--txt3)" }}>Loading...</p></Card>;
  if (error) return <Card><p style={{ ...SG(12), color: CC.red }}>Error: {error}</p></Card>;
  if (!data) return <Card><p style={{ ...SG(12), color: "var(--txt3)" }}>No data</p></Card>;

  const allStores = data.stores || [];
  const totalPosSales = data.totalPosSales || 0;

  // Filter
  let filtered = allStores;
  if (search) {
    const s = search.toLowerCase();
    filtered = filtered.filter((st) => st.storeName.toLowerCase().includes(s) || st.storeNumber.includes(s));
  }
  if (ohFilter === "0") filtered = filtered.filter((st) => st.ohTy === 0);
  else if (ohFilter === "1") filtered = filtered.filter((st) => st.ohTy === 1);
  else if (ohFilter === "risk") filtered = filtered.filter((st) => st.ohTy <= 1);

  // Sort
  filtered.sort((a, b) => {
    const av = a[sortBy] ?? 0;
    const bv = b[sortBy] ?? 0;
    if (typeof av === "string") return sortDir === "asc" ? av.localeCompare(bv) : bv.localeCompare(av);
    return sortDir === "asc" ? av - bv : bv - av;
  });

  const handleSort = (col) => {
    if (sortBy === col) setSortDir(sortDir === "asc" ? "desc" : "asc");
    else { setSortBy(col); setSortDir("desc"); }
  };

  const si = (col) => sortBy === col ? (sortDir === "asc" ? " ▲" : " ▼") : "";

  const uniqueStates = [...new Set(allStores.map((s) => {
    // try to extract state from store name patterns
    return "";
  }))].filter(Boolean).sort();

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
      {/* Filters */}
      <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
        <div style={{ position: "relative", flex: 1, maxWidth: 280 }}>
          <span style={{ position: "absolute", left: 8, top: "50%", transform: "translateY(-50%)", fontSize: 11, color: "var(--txt3)" }}>🔍</span>
          <input
            type="text"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Store name or #..."
            style={{
              width: "100%", height: 28, padding: "0 10px 0 28px", borderRadius: 7,
              border: "1px solid var(--brd2, #2a4060)", background: "rgba(255,255,255,.05)",
              fontSize: 11, color: "var(--txt2)",
            }}
          />
        </div>
        <select
          value={ohFilter}
          onChange={(e) => setOhFilter(e.target.value)}
          style={{
            height: 28, padding: "0 9px", borderRadius: 7,
            border: "1px solid var(--brd2, #2a4060)", background: "rgba(255,255,255,.05)",
            fontSize: 11, color: "var(--txt2)", cursor: "pointer",
          }}
        >
          <option value="">All OH</option>
          <option value="0">0 OH only</option>
          <option value="1">1 OH only</option>
          <option value="risk">Risk only</option>
        </select>
        <Badge type="blue">{filtered.length} stores</Badge>
      </div>

      {/* Store Table */}
      <Card>
        <CardHdr title={`All Stores · ${data.latestWeek || ""}`} right={
          <div style={{ display: "flex", gap: 6 }}>
            <Badge type="warn">🟡=1 OH</Badge>
            <Badge type="risk">🔴=0 OH</Badge>
          </div>
        } />
        <div style={{ overflowX: "auto", fontSize: 11 }}>
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead>
              <tr style={{ borderBottom: "1px solid var(--brd)" }}>
                {[
                  { key: "ohTy", label: "OH", align: "left" },
                  { key: "storeNumber", label: "Store #", align: "left" },
                  { key: "storeName", label: "Store Name", align: "left" },
                  { key: "posSalesTy", label: "L4W Sales", align: "right" },
                  { key: "posQtyTy", label: "Units", align: "right" },
                  { key: "ohTy", label: "Curr OH", align: "right" },
                  { key: "inWarehouseTy", label: "On-Order", align: "right" },
                  { key: "inTransitTy", label: "In-Transit", align: "right" },
                  { key: "instockPctTy", label: "Instock %", align: "right" },
                ].map((col) => (
                  <th
                    key={col.key + col.label}
                    onClick={() => handleSort(col.key)}
                    style={{
                      ...SG(7.5, 700), textTransform: "uppercase", letterSpacing: ".07em",
                      color: "var(--txt3)", padding: "8px 12px", textAlign: col.align,
                      whiteSpace: "nowrap", background: "var(--card2, #1A2D42)",
                      cursor: "pointer", userSelect: "none",
                    }}
                  >
                    {col.label}{si(col.key)}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {filtered.slice(0, 200).map((store, idx) => (
                <tr key={idx} style={{ borderBottom: "1px solid rgba(30,50,72,.5)", background: idx % 2 === 0 ? "rgba(0,0,0,.02)" : "transparent" }}>
                  <td style={{ padding: "7px 12px" }}><OhBadge oh={store.ohTy} /></td>
                  <td style={{ padding: "7px 12px", color: "var(--txt3)", fontWeight: 700 }}>#{store.storeNumber}</td>
                  <td style={{ padding: "7px 12px", fontWeight: 700, color: "var(--txt)" }}>{store.storeName}</td>
                  <td style={{ padding: "7px 12px", textAlign: "right", fontWeight: 700, color: CC.teal }}>{f$(store.posSalesTy)}</td>
                  <td style={{ padding: "7px 12px", textAlign: "right", color: "var(--txt2)" }}>{fN(store.posQtyTy)}</td>
                  <td style={{ padding: "7px 12px", textAlign: "right", fontWeight: 700, color: store.ohTy === 0 ? CC.red : store.ohTy <= 1 ? CC.amber : "var(--txt)" }}>{fN(store.ohTy)}</td>
                  <td style={{ padding: "7px 12px", textAlign: "right", color: "var(--txt2)" }}>{fN(store.inWarehouseTy)}</td>
                  <td style={{ padding: "7px 12px", textAlign: "right", color: store.inTransitTy > 0 ? CC.blue : "var(--txt3)" }}>{fN(store.inTransitTy)}</td>
                  <td style={{ padding: "7px 12px", textAlign: "right", color: store.instockPctTy >= 90 ? CC.teal : store.instockPctTy >= 70 ? CC.amber : CC.red }}>
                    {store.instockPctTy != null ? Number(store.instockPctTy).toFixed(1) + "%" : "—"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {filtered.length > 200 && (
          <div style={{ padding: "8px 12px", ...SG(10), color: "var(--txt3)" }}>
            Showing first 200 of {filtered.length} stores
          </div>
        )}
      </Card>
    </div>
  );
}

// ═════════════════════════════════════════════════════════════════════════════
// SUB-TAB 4: RISK (0 & 1 OH)
// ═════════════════════════════════════════════════════════════════════════════

function RiskTab() {
  const criticalStores = ZERO_OH.filter((s) => s.risk === 1);
  const inboundStores = ZERO_OH.filter((s) => s.risk === 0);
  const oneOhNoInbound = ONE_OH.filter((s) => s.risk === 1);
  const oneOhInbound = ONE_OH.filter((s) => s.risk === 0);

  // 0-OH by state
  const zBySt = {};
  ZERO_OH.forEach((s) => { zBySt[s.st] = (zBySt[s.st] || 0) + 1; });
  const zSorted = Object.entries(zBySt).sort((a, b) => b[1] - a[1]);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
      {/* KPI Cards */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(5, 1fr)", gap: 8 }}>
        <KPICard label="0-OH Traited Stores" value={ZERO_OH.length} color={CC.red} />
        <KPICard label="Critical · No Inbound" value={criticalStores.length} color={CC.red} />
        <KPICard label="0-OH Has Inbound" value={inboundStores.length} color={CC.blue} />
        <KPICard label="1-OH Stores" value={ONE_OH.length} color={CC.amber} />
        <KPICard label="1-OH No Inbound" value={oneOhNoInbound.length} color={CC.amber} />
      </div>

      {/* Charts */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
        <Card>
          <CardHdr title="0-OH Risk by State" right={<Badge type="risk">Count</Badge>} />
          <ChartCanvas
            type="bar"
            height={240}
            configKey="risk-by-state"
            labels={zSorted.map(([s]) => s)}
            datasets={[
              {
                label: "0-OH Stores",
                data: zSorted.map(([, v]) => v),
                backgroundColor: CC.red + "55",
                borderColor: CC.red,
                borderWidth: 2, borderRadius: 5,
              },
            ]}
          />
        </Card>
        <Card>
          <CardHdr title="Risk Category Breakdown" right={<Badge type="warn">All risk stores</Badge>} />
          <ChartCanvas
            type="doughnut"
            height={240}
            configKey="risk-breakdown"
            labels={["0-OH Critical", "0-OH Inbound", "1-OH No Order", "1-OH Inbound"]}
            datasets={[
              {
                data: [criticalStores.length, inboundStores.length, oneOhNoInbound.length, oneOhInbound.length],
                backgroundColor: [CC.red, "rgba(123,174,208,.6)", CC.amber, "rgba(46,207,170,.5)"],
                borderColor: "#0E1F2D",
                borderWidth: 2,
              },
            ]}
          />
        </Card>
      </div>

      {/* Critical Table */}
      <Card>
        <CardHdr title={`🔴 Critical — 0 OH, No Inbound (${criticalStores.length} stores)`} right={<Badge type="risk">Raise PO / Expedite</Badge>} />
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
            <thead>
              <tr style={{ borderBottom: "1px solid var(--brd)" }}>
                {["Store #", "Name", "State", "L4W Sales", "OH", "On-Order", "In-Transit", "Action"].map((h, i) => (
                  <th key={h} style={{ ...SG(7.5, 700), textTransform: "uppercase", letterSpacing: ".07em", color: "var(--txt3)", padding: "8px 12px", textAlign: i >= 3 && i <= 6 ? "right" : "left", whiteSpace: "nowrap", background: "var(--card2, #1A2D42)" }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {criticalStores.map((s) => (
                <tr key={s.s} style={{ borderBottom: "1px solid rgba(30,50,72,.5)" }}>
                  <td style={{ padding: "7px 12px", color: "var(--txt3)", fontWeight: 700 }}>#{s.s}</td>
                  <td style={{ padding: "7px 12px", fontWeight: 700, color: "var(--txt)" }}>{s.n}</td>
                  <td style={{ padding: "7px 12px", color: "var(--txt2)" }}>{s.st}</td>
                  <td style={{ padding: "7px 12px", textAlign: "right", fontWeight: 700, color: CC.teal }}>${s.pos}</td>
                  <td style={{ padding: "7px 12px", textAlign: "right", fontWeight: 700, color: CC.red }}>0</td>
                  <td style={{ padding: "7px 12px", textAlign: "right", color: "var(--txt3)" }}>0</td>
                  <td style={{ padding: "7px 12px", textAlign: "right", color: "var(--txt3)" }}>0</td>
                  <td style={{ padding: "7px 12px" }}>
                    <span style={{ ...SG(8, 700), padding: "1px 7px", borderRadius: 4, background: "rgba(248,113,113,.12)", color: CC.red }}>RAISE PO</span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Card>

      {/* Inbound + 1-OH Tables */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
        <Card>
          <CardHdr title={`🔵 0 OH — Has Inbound (${inboundStores.length})`} right={<Badge type="blue">Monitor</Badge>} />
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
              <thead>
                <tr style={{ borderBottom: "1px solid var(--brd)" }}>
                  {["Store #", "Name", "State", "Sales", "OH", "On-Ord", "In-Tr", "Status"].map((h, i) => (
                    <th key={h} style={{ ...SG(7.5, 700), textTransform: "uppercase", letterSpacing: ".07em", color: "var(--txt3)", padding: "8px 12px", textAlign: i >= 3 && i <= 6 ? "right" : "left", whiteSpace: "nowrap", background: "var(--card2, #1A2D42)" }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {inboundStores.map((s) => (
                  <tr key={s.s} style={{ borderBottom: "1px solid rgba(30,50,72,.5)" }}>
                    <td style={{ padding: "7px 12px", color: "var(--txt3)", fontWeight: 700 }}>#{s.s}</td>
                    <td style={{ padding: "7px 12px", fontWeight: 700, color: "var(--txt)" }}>{s.n}</td>
                    <td style={{ padding: "7px 12px", color: "var(--txt2)" }}>{s.st}</td>
                    <td style={{ padding: "7px 12px", textAlign: "right", fontWeight: 700, color: CC.teal }}>${s.pos}</td>
                    <td style={{ padding: "7px 12px", textAlign: "right", fontWeight: 700, color: CC.red }}>0</td>
                    <td style={{ padding: "7px 12px", textAlign: "right", color: CC.blue }}>{s.oo}</td>
                    <td style={{ padding: "7px 12px", textAlign: "right", color: CC.blue }}>{s.it}</td>
                    <td style={{ padding: "7px 12px" }}>
                      <span style={{ ...SG(8, 700), padding: "1px 7px", borderRadius: 4, background: "rgba(123,174,208,.12)", color: CC.blue }}>INBOUND</span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>

        <Card>
          <CardHdr title={`🟡 1 OH — Last Unit (${ONE_OH.length})`} right={<Badge type="warn">Watch</Badge>} />
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
              <thead>
                <tr style={{ borderBottom: "1px solid var(--brd)" }}>
                  {["Store #", "Name", "State", "Sales", "OH", "On-Ord", "In-Tr", "Risk"].map((h, i) => (
                    <th key={h} style={{ ...SG(7.5, 700), textTransform: "uppercase", letterSpacing: ".07em", color: "var(--txt3)", padding: "8px 12px", textAlign: i >= 3 && i <= 6 ? "right" : "left", whiteSpace: "nowrap", background: "var(--card2, #1A2D42)" }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {ONE_OH.map((s) => (
                  <tr key={s.s} style={{ borderBottom: "1px solid rgba(30,50,72,.5)" }}>
                    <td style={{ padding: "7px 12px", color: "var(--txt3)", fontWeight: 700 }}>#{s.s}</td>
                    <td style={{ padding: "7px 12px", fontWeight: 700, color: "var(--txt)" }}>{s.n}</td>
                    <td style={{ padding: "7px 12px", color: "var(--txt2)" }}>{s.st}</td>
                    <td style={{ padding: "7px 12px", textAlign: "right", fontWeight: 700, color: CC.teal }}>${s.pos}</td>
                    <td style={{ padding: "7px 12px", textAlign: "right", fontWeight: 700, color: CC.amber }}>1</td>
                    <td style={{ padding: "7px 12px", textAlign: "right", color: s.oo > 0 ? CC.blue : "var(--txt3)" }}>{s.oo}</td>
                    <td style={{ padding: "7px 12px", textAlign: "right", color: s.it > 0 ? CC.blue : "var(--txt3)" }}>{s.it}</td>
                    <td style={{ padding: "7px 12px" }}>
                      {s.risk === 1 ? (
                        <span style={{ ...SG(8, 700), padding: "1px 7px", borderRadius: 4, background: "rgba(245,183,49,.12)", color: CC.amber }}>NO INBOUND</span>
                      ) : (
                        <span style={{ ...SG(8, 700), padding: "1px 7px", borderRadius: 4, background: "rgba(123,174,208,.12)", color: CC.blue }}>INBOUND</span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>
      </div>
    </div>
  );
}

// ═════════════════════════════════════════════════════════════════════════════
// MAIN EXPORT — Store Analytics with 4 Sub-Tabs
// ═════════════════════════════════════════════════════════════════════════════

const SUB_TABS = [
  { key: "map", label: "🗺 Geography Map" },
  { key: "regions", label: "📍 Regions" },
  { key: "stores", label: "🏪 Store Detail" },
  { key: "risk", label: "⚠ Risk: 0 & 1 OH" },
];

export function WalmartStoreAnalytics({ filters }) {
  const [subTab, setSubTab] = useState("map");

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 0 }}>
      {/* Sub-tab navigation */}
      <div style={{ display: "flex", gap: 2, marginBottom: 16, borderBottom: "1px solid var(--brd)", background: "var(--card2, #1A2D42)", borderRadius: "8px 8px 0 0", overflow: "hidden" }}>
        {SUB_TABS.map((t) => (
          <button
            key={t.key}
            onClick={() => setSubTab(t.key)}
            style={{
              ...SG(10, subTab === t.key ? 700 : 500),
              background: "none", border: "none", cursor: "pointer",
              padding: "10px 16px", whiteSpace: "nowrap",
              color: subTab === t.key ? CC.teal : "var(--txt3)",
              borderBottom: subTab === t.key ? "2px solid " + CC.teal : "2px solid transparent",
              textTransform: "uppercase", letterSpacing: ".08em",
            }}
          >
            {t.label}
          </button>
        ))}
      </div>

      {/* Page header */}
      <div style={{ marginBottom: 12 }}>
        <div style={{ ...DM(22), color: CC.teal }}>
          {subTab === "map" && "Sales Geography Heatmap"}
          {subTab === "regions" && "Regional Performance"}
          {subTab === "stores" && "Store Detail"}
          {subTab === "risk" && "Inventory Risk Dashboard"}
        </div>
        <div style={{ ...SG(11), color: "var(--txt3)", marginTop: 2 }}>
          {subTab === "map" && "Walmart POS · L4W · Click any state to select"}
          {subTab === "regions" && "Click a region to filter · Avg $/Store shown per region and state"}
          {subTab === "stores" && "All traited stores · L4W performance"}
          {subTab === "risk" && "0 & 1 OH traited stores · Immediate action items"}
        </div>
      </div>

      {/* Tab content */}
      {subTab === "map" && <GeographyMapTab />}
      {subTab === "regions" && <RegionsTab />}
      {subTab === "stores" && <StoreDetailTab filters={filters} />}
      {subTab === "risk" && <RiskTab />}
    </div>
  );
}
