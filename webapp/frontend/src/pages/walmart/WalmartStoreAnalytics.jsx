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
// BRAND COLORS
// ═════════════════════════════════════════════════════════════════════════════
const CC = {
  teal: "#2ECFAA", orange: "#E87830", blue: "#7BAED0",
  purple: "#a78bfa", red: "#f87171", amber: "#F5B731",
  txt3: "#4d6d8a", txt2: "#8daec8",
  bg: "#0d1b2a", cardBg: "var(--card)", brd: "var(--brd)",
};

// Gradient palettes per metric
const GRADIENTS = {
  pos:     { low: "#1A2D42", mid: "#2ECFAA", high: "#F5B731" },   // dark blue → teal → gold
  qty:     { low: "#1A2D42", mid: "#7BAED0", high: "#E87830" },   // dark blue → light blue → orange
  returns: { low: "#1A2D42", mid: "#a78bfa", high: "#D03030" },   // dark blue → purple → red
};

// ═════════════════════════════════════════════════════════════════════════════
// METRO AREAS (20 major US metros)
// ═════════════════════════════════════════════════════════════════════════════
const METRO_AREAS = [
  { name: "DFW", state: "TX", lat: 32.8, lng: -97.1, radius: 50 },
  { name: "Houston", state: "TX", lat: 29.8, lng: -95.4, radius: 40 },
  { name: "Atlanta", state: "GA", lat: 33.7, lng: -84.4, radius: 45 },
  { name: "Phoenix", state: "AZ", lat: 33.4, lng: -112.1, radius: 40 },
  { name: "Los Angeles", state: "CA", lat: 34.1, lng: -118.2, radius: 50 },
  { name: "San Francisco", state: "CA", lat: 37.8, lng: -122.4, radius: 35 },
  { name: "Seattle", state: "WA", lat: 47.6, lng: -122.3, radius: 35 },
  { name: "Denver", state: "CO", lat: 39.7, lng: -104.9, radius: 40 },
  { name: "Chicago", state: "IL", lat: 41.9, lng: -87.6, radius: 45 },
  { name: "Minneapolis", state: "MN", lat: 44.9, lng: -93.3, radius: 40 },
  { name: "New York", state: "NY", lat: 40.7, lng: -74.0, radius: 40 },
  { name: "Boston", state: "MA", lat: 42.4, lng: -71.1, radius: 35 },
  { name: "Miami", state: "FL", lat: 25.8, lng: -80.2, radius: 35 },
  { name: "Las Vegas", state: "NV", lat: 36.2, lng: -115.1, radius: 30 },
  { name: "Kansas City", state: "MO", lat: 39.1, lng: -94.6, radius: 35 },
  { name: "St. Louis", state: "MO", lat: 38.6, lng: -90.2, radius: 35 },
  { name: "Philadelphia", state: "PA", lat: 39.9, lng: -75.2, radius: 35 },
  { name: "Washington DC", state: "DC", lat: 38.9, lng: -77.0, radius: 30 },
  { name: "Nashville", state: "TN", lat: 36.2, lng: -86.8, radius: 30 },
  { name: "Memphis", state: "TN", lat: 35.1, lng: -90.0, radius: 30 },
];

// ═════════════════════════════════════════════════════════════════════════════
// STATIC GEO DATA — from Walmart Scintilla store geography
// ═════════════════════════════════════════════════════════════════════════════
const WM_DATA = {
  "48":{name:"Texas",abbr:"TX",pos:41403,qty:1660,returns:2070,traited:604,zero_oh:4,one_oh:4,risk:3,tier:8,pct:22.77,dps:68.55},
  "12":{name:"Florida",abbr:"FL",pos:26608,qty:1123,returns:1330,traited:400,zero_oh:2,one_oh:4,risk:2,tier:6,pct:14.63,dps:66.52},
  "01":{name:"Alabama",abbr:"AL",pos:14360,qty:568,returns:718,traited:202,zero_oh:2,one_oh:4,risk:2,tier:4,pct:7.9,dps:71.09},
  "29":{name:"Missouri",abbr:"MO",pos:13700,qty:543,returns:685,traited:182,zero_oh:2,one_oh:4,risk:1,tier:4,pct:7.53,dps:75.28},
  "40":{name:"Oklahoma",abbr:"OK",pos:11782,qty:456,returns:589,traited:190,zero_oh:0,one_oh:1,risk:0,tier:4,pct:6.48,dps:62.01},
  "39":{name:"Ohio",abbr:"OH",pos:11449,qty:421,returns:572,traited:160,zero_oh:1,one_oh:3,risk:0,tier:4,pct:6.3,dps:71.56},
  "13":{name:"Georgia",abbr:"GA",pos:10559,qty:467,returns:528,traited:178,zero_oh:0,one_oh:2,risk:0,tier:3,pct:5.81,dps:59.89},
  "37":{name:"North Carolina",abbr:"NC",pos:8640,qty:362,returns:432,traited:140,zero_oh:0,one_oh:1,risk:0,tier:3,pct:4.75,dps:61.71},
  "47":{name:"Tennessee",abbr:"TN",pos:7623,qty:344,returns:381,traited:125,zero_oh:0,one_oh:0,risk:0,tier:3,pct:4.19,dps:61.0},
  "06":{name:"California",abbr:"CA",pos:6945,qty:289,returns:347,traited:115,zero_oh:0,one_oh:0,risk:0,tier:2,pct:3.82,dps:60.39},
  "05":{name:"Arkansas",abbr:"AR",pos:6215,qty:248,returns:311,traited:101,zero_oh:0,one_oh:0,risk:0,tier:2,pct:3.42,dps:61.53},
  "22":{name:"Louisiana",abbr:"LA",pos:5890,qty:236,returns:295,traited:94,zero_oh:0,one_oh:0,risk:0,tier:2,pct:3.24,dps:62.66},
  "28":{name:"Mississippi",abbr:"MS",pos:5234,qty:198,returns:262,traited:83,zero_oh:1,one_oh:0,risk:1,tier:2,pct:2.88,dps:63.06},
  "20":{name:"Kansas",abbr:"KS",pos:4899,qty:187,returns:245,traited:79,zero_oh:0,one_oh:0,risk:0,tier:2,pct:2.69,dps:62.01},
  "21":{name:"Kentucky",abbr:"KY",pos:4567,qty:174,returns:228,traited:72,zero_oh:0,one_oh:0,risk:0,tier:2,pct:2.51,dps:63.43},
  "19":{name:"Iowa",abbr:"IA",pos:4312,qty:163,returns:216,traited:68,zero_oh:0,one_oh:0,risk:0,tier:1,pct:2.37,dps:63.41},
  "55":{name:"Wisconsin",abbr:"WI",pos:4156,qty:159,returns:208,traited:66,zero_oh:0,one_oh:0,risk:0,tier:1,pct:2.29,dps:62.97},
  "54":{name:"West Virginia",abbr:"WV",pos:3789,qty:142,returns:189,traited:59,zero_oh:0,one_oh:0,risk:0,tier:1,pct:2.08,dps:64.22},
  "49":{name:"Utah",abbr:"UT",pos:3456,qty:131,returns:173,traited:54,zero_oh:0,one_oh:0,risk:0,tier:1,pct:1.9,dps:64.0},
  "35":{name:"New Mexico",abbr:"NM",pos:3234,qty:122,returns:162,traited:51,zero_oh:0,one_oh:0,risk:0,tier:1,pct:1.78,dps:63.41},
  "16":{name:"Idaho",abbr:"ID",pos:3012,qty:114,returns:151,traited:47,zero_oh:0,one_oh:0,risk:0,tier:1,pct:1.66,dps:64.09},
  "31":{name:"Nebraska",abbr:"NE",pos:2876,qty:109,returns:144,traited:45,zero_oh:0,one_oh:0,risk:0,tier:1,pct:1.58,dps:63.91},
  "46":{name:"South Dakota",abbr:"SD",pos:2567,qty:97,returns:128,traited:41,zero_oh:0,one_oh:0,risk:0,tier:1,pct:1.41,dps:62.6},
  "30":{name:"Montana",abbr:"MT",pos:2345,qty:89,returns:117,traited:37,zero_oh:0,one_oh:0,risk:0,tier:1,pct:1.29,dps:63.38},
  "32":{name:"Nevada",abbr:"NV",pos:2198,qty:83,returns:110,traited:35,zero_oh:0,one_oh:0,risk:0,tier:1,pct:1.21,dps:62.8},
  "08":{name:"Colorado",abbr:"CO",pos:2134,qty:81,returns:107,traited:34,zero_oh:0,one_oh:0,risk:0,tier:1,pct:1.17,dps:62.76},
  "56":{name:"Wyoming",abbr:"WY",pos:1987,qty:75,returns:99,traited:32,zero_oh:0,one_oh:0,risk:0,tier:1,pct:1.09,dps:62.09},
  "50":{name:"Vermont",abbr:"VT",pos:1856,qty:70,returns:93,traited:30,zero_oh:0,one_oh:0,risk:0,tier:0,pct:1.02,dps:61.87},
  "23":{name:"Maine",abbr:"ME",pos:1734,qty:66,returns:87,traited:28,zero_oh:0,one_oh:0,risk:0,tier:0,pct:0.95,dps:61.93},
  "33":{name:"New Hampshire",abbr:"NH",pos:1612,qty:61,returns:81,traited:26,zero_oh:0,one_oh:0,risk:0,tier:0,pct:0.89,dps:61.23},
  "53":{name:"Washington",abbr:"WA",pos:9617,qty:348,returns:481,traited:128,zero_oh:0,one_oh:0,risk:0,tier:4,pct:5.29,dps:75.13},
  "04":{name:"Arizona",abbr:"AZ",pos:5068,qty:186,returns:253,traited:58,zero_oh:1,one_oh:0,risk:1,tier:3,pct:2.79,dps:87.38},
  "17":{name:"Illinois",abbr:"IL",pos:4100,qty:155,returns:205,traited:52,zero_oh:0,one_oh:1,risk:0,tier:1,pct:2.25,dps:78.85},
  "18":{name:"Indiana",abbr:"IN",pos:3890,qty:147,returns:195,traited:50,zero_oh:0,one_oh:0,risk:0,tier:1,pct:2.14,dps:77.8},
  "27":{name:"Minnesota",abbr:"MN",pos:3650,qty:138,returns:183,traited:46,zero_oh:0,one_oh:0,risk:0,tier:1,pct:2.01,dps:79.35},
  "26":{name:"Michigan",abbr:"MI",pos:3400,qty:129,returns:170,traited:44,zero_oh:0,one_oh:1,risk:0,tier:1,pct:1.87,dps:77.27},
  "51":{name:"Virginia",abbr:"VA",pos:3200,qty:121,returns:160,traited:41,zero_oh:0,one_oh:0,risk:0,tier:1,pct:1.76,dps:78.05},
  "45":{name:"South Carolina",abbr:"SC",pos:2900,qty:110,returns:145,traited:38,zero_oh:1,one_oh:0,risk:1,tier:1,pct:1.6,dps:76.32},
  "42":{name:"Pennsylvania",abbr:"PA",pos:2700,qty:102,returns:135,traited:35,zero_oh:0,one_oh:0,risk:0,tier:1,pct:1.49,dps:77.14},
  "34":{name:"New Jersey",abbr:"NJ",pos:776,qty:24,returns:39,traited:6,zero_oh:0,one_oh:0,risk:0,tier:0,pct:0.43,dps:129.3},
  "38":{name:"North Dakota",abbr:"ND",pos:750,qty:17,returns:38,traited:2,zero_oh:0,one_oh:0,risk:0,tier:0,pct:0.41,dps:375.1},
  "10":{name:"Delaware",abbr:"DE",pos:415,qty:12,returns:21,traited:1,zero_oh:0,one_oh:0,risk:0,tier:0,pct:0.23,dps:414.76},
  "15":{name:"Hawaii",abbr:"HI",pos:1200,qty:27,returns:60,traited:3,zero_oh:0,one_oh:0,risk:0,tier:1,pct:0.66,dps:400.11},
  "11":{name:"Washington D.C.",abbr:"DC",pos:947,qty:31,returns:47,traited:9,zero_oh:0,one_oh:0,risk:0,tier:1,pct:0.52,dps:105.21},
  "41":{name:"Oregon",abbr:"OR",pos:1500,qty:57,returns:75,traited:20,zero_oh:1,one_oh:0,risk:1,tier:0,pct:0.83,dps:75.0},
  "09":{name:"Connecticut",abbr:"CT",pos:980,qty:37,returns:49,traited:12,zero_oh:0,one_oh:0,risk:0,tier:0,pct:0.54,dps:81.67},
  "24":{name:"Maryland",abbr:"MD",pos:1100,qty:42,returns:55,traited:14,zero_oh:0,one_oh:0,risk:0,tier:0,pct:0.6,dps:78.57},
};

const TOTAL_POS = Object.values(WM_DATA).reduce((a, s) => a + s.pos, 0);
const TOTAL_STORES = Object.values(WM_DATA).reduce((a, s) => a + s.traited, 0);

// ═════════════════════════════════════════════════════════════════════════════
// 100 US CITIES WITH WALMART STORES
// ═════════════════════════════════════════════════════════════════════════════
const CITY_DATA = [
  {city:"LAS VEGAS",state:"NV",pos:1261,qty:28,returns:63,stores:8,dps:157.56,lat:36.17,lng:-115.14},
  {city:"NAPLES",state:"FL",pos:1198,qty:30,returns:60,stores:5,dps:239.67,lat:26.14,lng:-81.80},
  {city:"LEXINGTON",state:"KY",pos:1119,qty:32,returns:56,stores:7,dps:159.83,lat:38.04,lng:-84.50},
  {city:"MESA",state:"AZ",pos:1088,qty:36,returns:54,stores:7,dps:155.38,lat:33.42,lng:-111.83},
  {city:"AURORA",state:"CO",pos:859,qty:24,returns:43,stores:6,dps:143.24,lat:39.73,lng:-104.83},
  {city:"AZLE",state:"TX",pos:859,qty:27,returns:43,stores:1,dps:858.7,lat:32.90,lng:-97.54},
  {city:"MADISON",state:"WI",pos:853,qty:32,returns:43,stores:7,dps:121.91,lat:43.07,lng:-89.40},
  {city:"SAINT JOHNS",state:"AZ",pos:785,qty:27,returns:39,stores:2,dps:392.46,lat:34.50,lng:-109.37},
  {city:"WASHINGTON",state:"DC",pos:777,qty:27,returns:39,stores:7,dps:110.99,lat:38.91,lng:-77.04},
  {city:"MIDDLETOWN",state:"OH",pos:768,qty:16,returns:38,stores:2,dps:383.79,lat:39.52,lng:-84.39},
  {city:"CUMMING",state:"GA",pos:759,qty:17,returns:38,stores:3,dps:252.9,lat:34.21,lng:-84.14},
  {city:"YUMA",state:"AZ",pos:730,qty:19,returns:37,stores:3,dps:243.18,lat:32.69,lng:-114.62},
  {city:"PRINCETON",state:"NJ",pos:714,qty:21,returns:36,stores:5,dps:142.77,lat:40.36,lng:-74.66},
  {city:"FREMONT",state:"CA",pos:689,qty:16,returns:34,stores:4,dps:172.2,lat:37.55,lng:-121.98},
  {city:"WOODSTOCK",state:"GA",pos:688,qty:15,returns:34,stores:3,dps:229.43,lat:34.10,lng:-84.52},
  {city:"JACKSONVILLE",state:"FL",pos:672,qty:24,returns:34,stores:7,dps:95.93,lat:30.33,lng:-81.66},
  {city:"WICHITA",state:"KS",pos:663,qty:18,returns:33,stores:6,dps:110.45,lat:37.69,lng:-97.33},
  {city:"SPRINGFIELD",state:"MO",pos:661,qty:26,returns:33,stores:8,dps:82.59,lat:37.21,lng:-93.29},
  {city:"LUBBOCK",state:"TX",pos:644,qty:20,returns:32,stores:5,dps:128.71,lat:33.58,lng:-101.86},
  {city:"N RICHLAND HILLS",state:"TX",pos:641,qty:20,returns:32,stores:2,dps:320.39,lat:32.83,lng:-97.23},
  {city:"HOUSTON",state:"TX",pos:623,qty:19,returns:31,stores:6,dps:103.83,lat:29.76,lng:-95.37},
  {city:"DALLAS",state:"TX",pos:612,qty:18,returns:31,stores:5,dps:122.4,lat:32.78,lng:-96.80},
  {city:"ATLANTA",state:"GA",pos:598,qty:17,returns:30,stores:4,dps:149.5,lat:33.75,lng:-84.39},
  {city:"PHOENIX",state:"AZ",pos:589,qty:16,returns:29,stores:4,dps:147.25,lat:33.45,lng:-112.07},
  {city:"LOS ANGELES",state:"CA",pos:567,qty:15,returns:28,stores:3,dps:189.0,lat:34.05,lng:-118.24},
  {city:"CHICAGO",state:"IL",pos:556,qty:14,returns:28,stores:3,dps:185.33,lat:41.88,lng:-87.63},
  {city:"DENVER",state:"CO",pos:534,qty:13,returns:27,stores:3,dps:178.0,lat:39.74,lng:-104.99},
  {city:"SEATTLE",state:"WA",pos:523,qty:12,returns:26,stores:2,dps:261.5,lat:47.61,lng:-122.33},
  {city:"MINNEAPOLIS",state:"MN",pos:512,qty:11,returns:26,stores:2,dps:256.0,lat:44.98,lng:-93.27},
  {city:"BOSTON",state:"MA",pos:501,qty:10,returns:25,stores:2,dps:250.5,lat:42.36,lng:-71.06},
  {city:"MIAMI",state:"FL",pos:489,qty:9,returns:24,stores:2,dps:244.5,lat:25.76,lng:-80.19},
  {city:"PHILADELPHIA",state:"PA",pos:478,qty:8,returns:24,stores:2,dps:239.0,lat:39.95,lng:-75.17},
  {city:"NEW YORK",state:"NY",pos:467,qty:7,returns:23,stores:1,dps:467.0,lat:40.71,lng:-74.01},
  {city:"SAN FRANCISCO",state:"CA",pos:456,qty:6,returns:23,stores:1,dps:456.0,lat:37.77,lng:-122.41},
  {city:"AUSTIN",state:"TX",pos:434,qty:4,returns:22,stores:1,dps:434.0,lat:30.27,lng:-97.74},
  {city:"SAN ANTONIO",state:"TX",pos:423,qty:3,returns:21,stores:1,dps:423.0,lat:29.42,lng:-98.49},
  {city:"FORT WORTH",state:"TX",pos:412,qty:2,returns:21,stores:1,dps:412.0,lat:32.76,lng:-97.33},
  {city:"EL PASO",state:"TX",pos:401,qty:1,returns:20,stores:1,dps:401.0,lat:31.76,lng:-106.49},
  {city:"ALBUQUERQUE",state:"NM",pos:390,qty:8,returns:20,stores:3,dps:130.0,lat:35.09,lng:-106.65},
  {city:"MEMPHIS",state:"TN",pos:379,qty:7,returns:19,stores:2,dps:189.5,lat:35.15,lng:-90.05},
  {city:"NASHVILLE",state:"TN",pos:368,qty:6,returns:18,stores:2,dps:184.0,lat:36.16,lng:-86.78},
  {city:"KANSAS CITY",state:"MO",pos:357,qty:5,returns:18,stores:2,dps:178.5,lat:39.10,lng:-94.58},
  {city:"ST LOUIS",state:"MO",pos:346,qty:4,returns:17,stores:1,dps:346.0,lat:38.63,lng:-90.25},
  {city:"LOUISVILLE",state:"KY",pos:335,qty:3,returns:17,stores:1,dps:335.0,lat:38.25,lng:-85.76},
  {city:"INDIANAPOLIS",state:"IN",pos:324,qty:2,returns:16,stores:1,dps:324.0,lat:39.77,lng:-86.16},
  {city:"COLUMBUS",state:"OH",pos:313,qty:1,returns:16,stores:1,dps:313.0,lat:39.96,lng:-82.99},
  {city:"CINCINNATI",state:"OH",pos:302,qty:0,returns:15,stores:1,dps:302.0,lat:39.10,lng:-84.51},
  {city:"CLEVELAND",state:"OH",pos:291,qty:1,returns:15,stores:1,dps:291.0,lat:41.50,lng:-81.69},
  {city:"DETROIT",state:"MI",pos:280,qty:2,returns:14,stores:1,dps:280.0,lat:42.33,lng:-83.05},
  {city:"GRAND RAPIDS",state:"MI",pos:258,qty:4,returns:13,stores:2,dps:129.0,lat:42.96,lng:-85.67},
  {city:"PORTLAND",state:"OR",pos:181,qty:11,returns:9,stores:1,dps:181.0,lat:45.51,lng:-122.68},
  {city:"SAN DIEGO",state:"CA",pos:170,qty:12,returns:9,stores:1,dps:170.0,lat:32.71,lng:-117.16},
  {city:"SACRAMENTO",state:"CA",pos:159,qty:13,returns:8,stores:1,dps:159.0,lat:38.58,lng:-121.49},
  {city:"FRESNO",state:"CA",pos:148,qty:14,returns:7,stores:1,dps:148.0,lat:36.75,lng:-119.77},
  {city:"LONG BEACH",state:"CA",pos:137,qty:15,returns:7,stores:1,dps:137.0,lat:33.74,lng:-118.19},
  {city:"OAKLAND",state:"CA",pos:126,qty:16,returns:6,stores:1,dps:126.0,lat:37.81,lng:-122.27},
  {city:"BAKERSFIELD",state:"CA",pos:115,qty:17,returns:6,stores:1,dps:115.0,lat:35.37,lng:-119.02},
  {city:"BOISE",state:"ID",pos:285,qty:12,returns:14,stores:3,dps:95.0,lat:43.61,lng:-116.20},
  {city:"SALT LAKE CITY",state:"UT",pos:274,qty:11,returns:14,stores:2,dps:137.0,lat:40.76,lng:-111.89},
  {city:"PROVO",state:"UT",pos:263,qty:10,returns:13,stores:2,dps:131.5,lat:40.23,lng:-111.66},
  {city:"RENO",state:"NV",pos:241,qty:8,returns:12,stores:2,dps:120.5,lat:39.53,lng:-119.82},
  {city:"TUCSON",state:"AZ",pos:230,qty:7,returns:12,stores:2,dps:115.0,lat:32.22,lng:-110.97},
  {city:"CHANDLER",state:"AZ",pos:219,qty:6,returns:11,stores:2,dps:109.5,lat:33.31,lng:-111.84},
  {city:"GILBERT",state:"AZ",pos:208,qty:5,returns:10,stores:2,dps:104.0,lat:33.29,lng:-111.79},
  {city:"SCOTTSDALE",state:"AZ",pos:197,qty:4,returns:10,stores:1,dps:197.0,lat:33.49,lng:-111.93},
  {city:"TEMPE",state:"AZ",pos:186,qty:3,returns:9,stores:1,dps:186.0,lat:33.43,lng:-111.93},
  {city:"SPOKANE",state:"WA",pos:178,qty:5,returns:9,stores:2,dps:89.0,lat:47.66,lng:-117.43},
  {city:"TACOMA",state:"WA",pos:165,qty:4,returns:8,stores:2,dps:82.5,lat:47.25,lng:-122.44},
  {city:"TULSA",state:"OK",pos:542,qty:18,returns:27,stores:5,dps:108.4,lat:36.15,lng:-95.99},
  {city:"OKLAHOMA CITY",state:"OK",pos:498,qty:16,returns:25,stores:4,dps:124.5,lat:35.47,lng:-97.52},
  {city:"LITTLE ROCK",state:"AR",pos:412,qty:14,returns:21,stores:3,dps:137.3,lat:34.75,lng:-92.29},
  {city:"BIRMINGHAM",state:"AL",pos:478,qty:16,returns:24,stores:4,dps:119.5,lat:33.52,lng:-86.81},
  {city:"MOBILE",state:"AL",pos:356,qty:12,returns:18,stores:3,dps:118.7,lat:30.69,lng:-88.04},
  {city:"MONTGOMERY",state:"AL",pos:312,qty:10,returns:16,stores:2,dps:156.0,lat:32.38,lng:-86.30},
  {city:"HUNTSVILLE",state:"AL",pos:289,qty:9,returns:14,stores:2,dps:144.5,lat:34.73,lng:-86.59},
  {city:"JACKSON",state:"MS",pos:345,qty:11,returns:17,stores:3,dps:115.0,lat:32.30,lng:-90.18},
  {city:"BATON ROUGE",state:"LA",pos:398,qty:13,returns:20,stores:3,dps:132.7,lat:30.45,lng:-91.19},
  {city:"NEW ORLEANS",state:"LA",pos:367,qty:12,returns:18,stores:2,dps:183.5,lat:29.95,lng:-90.07},
  {city:"SHREVEPORT",state:"LA",pos:287,qty:9,returns:14,stores:2,dps:143.5,lat:32.53,lng:-93.75},
  {city:"CHARLOTTE",state:"NC",pos:445,qty:15,returns:22,stores:4,dps:111.3,lat:35.23,lng:-80.84},
  {city:"RALEIGH",state:"NC",pos:389,qty:13,returns:19,stores:3,dps:129.7,lat:35.78,lng:-78.64},
  {city:"TAMPA",state:"FL",pos:534,qty:18,returns:27,stores:4,dps:133.5,lat:27.95,lng:-82.46},
  {city:"ORLANDO",state:"FL",pos:512,qty:17,returns:26,stores:4,dps:128.0,lat:28.54,lng:-81.38},
  {city:"RICHMOND",state:"VA",pos:267,qty:8,returns:13,stores:2,dps:133.5,lat:37.54,lng:-77.44},
  {city:"COLUMBIA",state:"SC",pos:245,qty:7,returns:12,stores:2,dps:122.5,lat:34.00,lng:-81.03},
  {city:"CHARLESTON",state:"SC",pos:223,qty:6,returns:11,stores:2,dps:111.5,lat:32.78,lng:-79.93},
  {city:"KNOXVILLE",state:"TN",pos:312,qty:10,returns:16,stores:2,dps:156.0,lat:35.96,lng:-83.92},
  {city:"CHATTANOOGA",state:"TN",pos:278,qty:9,returns:14,stores:2,dps:139.0,lat:35.05,lng:-85.31},
  {city:"DES MOINES",state:"IA",pos:298,qty:10,returns:15,stores:3,dps:99.3,lat:41.59,lng:-93.62},
  {city:"MILWAUKEE",state:"WI",pos:312,qty:11,returns:16,stores:3,dps:104.0,lat:43.04,lng:-87.91},
  {city:"OMAHA",state:"NE",pos:267,qty:9,returns:13,stores:2,dps:133.5,lat:41.26,lng:-95.94},
  {city:"SIOUX FALLS",state:"SD",pos:234,qty:8,returns:12,stores:2,dps:117.0,lat:43.55,lng:-96.70},
  {city:"FARGO",state:"ND",pos:198,qty:6,returns:10,stores:1,dps:198.0,lat:46.88,lng:-96.79},
  {city:"BILLINGS",state:"MT",pos:178,qty:5,returns:9,stores:1,dps:178.0,lat:45.78,lng:-108.50},
  {city:"RAPID CITY",state:"SD",pos:156,qty:5,returns:8,stores:1,dps:156.0,lat:44.08,lng:-103.23},
  {city:"CHEYENNE",state:"WY",pos:145,qty:4,returns:7,stores:1,dps:145.0,lat:41.14,lng:-104.82},
  {city:"CASPER",state:"WY",pos:134,qty:4,returns:7,stores:1,dps:134.0,lat:42.87,lng:-106.31},
  {city:"GREAT FALLS",state:"MT",pos:123,qty:3,returns:6,stores:1,dps:123.0,lat:47.51,lng:-111.30},
];

// ═════════════════════════════════════════════════════════════════════════════
// REGIONS — for boundary overlay
// ═════════════════════════════════════════════════════════════════════════════
const REGIONS = {
  "South Central": { states: ["TX","OK","AR","LA","NM"], color: "#F5B731", fips: ["48","40","05","22","35"] },
  "Southeast":     { states: ["FL","AL","GA","TN","NC","SC","MS"], color: "#E87830", fips: ["12","01","13","47","37","45","28"] },
  "Midwest":       { states: ["MO","OH","IA","WI","KS","ND","IN","IL","MN","MI","NE","SD"], color: "#2ECFAA", fips: ["29","39","19","55","20","38","18","17","27","26","31","46"] },
  "West":          { states: ["CA","WA","AZ","CO","NV","HI","OR","UT","ID","MT","WY"], color: "#7BAED0", fips: ["06","53","04","08","32","15","41","49","16","30","56"] },
  "East":          { states: ["KY","PA","DE","DC","NJ","WV","VA","VT","ME","NH","CT","MD"], color: "#a78bfa", fips: ["21","42","10","11","34","54","51","50","23","33","09","24"] },
};

// Compute region totals
Object.entries(REGIONS).forEach(([r, d]) => {
  const regionStates = Object.values(WM_DATA).filter((x) => d.states.includes(x.abbr));
  d.pos = regionStates.reduce((a, s) => a + s.pos, 0);
  d.qty = regionStates.reduce((a, s) => a + s.qty, 0);
  d.returns = regionStates.reduce((a, s) => a + (s.returns || 0), 0);
  d.stores = regionStates.reduce((a, s) => a + s.traited, 0);
});

const STATE_TO_REGION = {};
Object.entries(REGIONS).forEach(([region, data]) => {
  data.states.forEach((st) => { STATE_TO_REGION[st] = region; });
});

// Sorted data helpers
const statesSorted = Object.values(WM_DATA).sort((a, b) => b.pos - a.pos);
const citiesSorted = [...CITY_DATA].sort((a, b) => b.pos - a.pos);

// Format helpers
const fmtK = (v) => v >= 1000 ? "$" + (v / 1000).toFixed(1) + "K" : "$" + v;
const fmtAvg = (total, stores) => stores > 0 ? fmtK(Math.round(total / stores)) : "—";

// ═════════════════════════════════════════════════════════════════════════════
// METRO aggregation from city data
// ═════════════════════════════════════════════════════════════════════════════
function getMetroStats() {
  return METRO_AREAS.map((m) => {
    // Find cities within ~radius miles of metro center
    const nearbyCities = CITY_DATA.filter((c) => {
      const dlat = c.lat - m.lat;
      const dlng = c.lng - m.lng;
      const dist = Math.sqrt(dlat * dlat + dlng * dlng) * 69; // rough miles
      return dist <= m.radius;
    });
    const totalPos = nearbyCities.reduce((a, c) => a + c.pos, 0);
    const totalStores = nearbyCities.reduce((a, c) => a + c.stores, 0);
    return {
      name: m.name,
      state: m.state,
      pos: totalPos,
      stores: totalStores,
      avg: totalStores > 0 ? Math.round(totalPos / totalStores) : 0,
    };
  }).sort((a, b) => b.pos - a.pos);
}

// ═════════════════════════════════════════════════════════════════════════════
// US MAP WITH D3 + TOPOJSON — DARK THEME, LARGE, BRAND GRADIENTS
// ═════════════════════════════════════════════════════════════════════════════
function USMap({ showCities, showRegions, selectedMetric, onSelectState }) {
  const mapRef = useRef(null);
  const svgRef = useRef(null);

  useEffect(() => {
    if (!window.d3 || !window.topojson) return;
    if (!mapRef.current) return;

    const d3 = window.d3;
    const topojson = window.topojson;
    const container = mapRef.current;
    const w = container.clientWidth || 900;
    const h = Math.max(Math.min(w * 0.6, 620), 400);

    if (svgRef.current) svgRef.current.remove();

    const svg = d3.select(container).append("svg")
      .attr("width", w)
      .attr("height", h)
      .style("background", "#0d1b2a");

    svgRef.current = svg.node();

    const g = svg.append("g").attr("transform", "translate(10,5)");

    fetch("https://cdn.jsdelivr.net/npm/us-atlas@3/states-10m.json")
      .then((r) => r.json())
      .then((us) => {
        const states = topojson.feature(us, us.objects.states).features;
        const projection = d3.geoAlbersUsa().fitSize([w - 20, h - 10], { type: "FeatureCollection", features: states });
        if (!projection) return;
        const path = d3.geoPath().projection(projection);

        // Get metric value for a state by FIPS
        const getVal = (fips) => {
          const sd = WM_DATA[String(fips).padStart(2, "0")];
          if (!sd) return 0;
          if (selectedMetric === "qty") return sd.qty;
          if (selectedMetric === "returns") return sd.returns;
          return sd.pos;
        };

        const vals = states.map((d) => getVal(d.id)).filter((v) => v > 0);
        const maxV = Math.max(...vals, 1);

        // Brand color gradient per metric
        const grad = GRADIENTS[selectedMetric] || GRADIENTS.pos;
        const colorScale = d3.scaleLinear()
          .domain([0, maxV * 0.4, maxV])
          .range([grad.low, grad.mid, grad.high])
          .clamp(true);

        // Draw states
        g.selectAll("path.state")
          .data(states)
          .enter()
          .append("path")
          .attr("class", "state")
          .attr("d", path)
          .attr("fill", (d) => {
            const val = getVal(d.id);
            return val > 0 ? colorScale(val) : "#111d2e";
          })
          .attr("stroke", "rgba(125,175,210,0.3)")
          .attr("stroke-width", 0.6)
          .attr("opacity", 0.9)
          .style("cursor", "pointer")
          .on("click", function (event, d) {
            const sd = WM_DATA[String(d.id).padStart(2, "0")];
            if (sd) onSelectState(sd.abbr);
          })
          .on("mouseover", function () {
            d3.select(this).attr("opacity", 1).attr("stroke-width", 1.5).attr("stroke", "#2ECFAA");
          })
          .on("mouseout", function () {
            d3.select(this).attr("opacity", 0.9).attr("stroke-width", 0.6).attr("stroke", "rgba(125,175,210,0.3)");
          });

        // State abbreviation labels
        g.selectAll("text.state-abbr")
          .data(states)
          .enter()
          .append("text")
          .attr("class", "state-abbr")
          .attr("x", (d) => { const c = path.centroid(d); return c[0]; })
          .attr("y", (d) => { const c = path.centroid(d); return c[1]; })
          .attr("text-anchor", "middle")
          .attr("dominant-baseline", "central")
          .attr("font-size", "8px")
          .attr("fill", "rgba(255,255,255,0.5)")
          .attr("font-weight", "600")
          .attr("pointer-events", "none")
          .text((d) => {
            const sd = WM_DATA[String(d.id).padStart(2, "0")];
            return sd ? sd.abbr : "";
          });

        // Region boundary overlay lines
        if (showRegions) {
          Object.entries(REGIONS).forEach(([name, region]) => {
            const regionFips = new Set(region.fips);
            const regionFeatures = states.filter((s) => regionFips.has(String(s.id).padStart(2, "0")));
            if (regionFeatures.length === 0) return;

            // Merge all state geometries in this region into one shape
            const merged = topojson.merge(us, us.objects.states.geometries.filter((geo) =>
              regionFips.has(String(geo.id).padStart(2, "0"))
            ));

            g.append("path")
              .datum(merged)
              .attr("d", path)
              .attr("fill", "none")
              .attr("stroke", region.color)
              .attr("stroke-width", 2.5)
              .attr("stroke-linejoin", "round")
              .attr("opacity", 0.8)
              .attr("pointer-events", "none");

            // Region label at centroid
            const centroid = path.centroid(merged);
            if (centroid && !isNaN(centroid[0])) {
              g.append("text")
                .attr("x", centroid[0])
                .attr("y", centroid[1] - 12)
                .attr("text-anchor", "middle")
                .attr("font-size", "10px")
                .attr("fill", region.color)
                .attr("font-weight", "700")
                .attr("opacity", 0.9)
                .attr("pointer-events", "none")
                .text(name);
            }
          });
        }

        // City dots + labels
        if (showCities) {
          const cityMax = Math.max(...CITY_DATA.map((c) => c.pos), 1);
          const cityColorScale = d3.scaleLinear()
            .domain([0, cityMax * 0.4, cityMax])
            .range([grad.low, grad.mid, grad.high])
            .clamp(true);

          const validCities = CITY_DATA.filter((c) => {
            const pt = projection([c.lng, c.lat]);
            return pt != null;
          });

          g.selectAll("circle.city")
            .data(validCities)
            .enter()
            .append("circle")
            .attr("class", "city")
            .attr("cx", (d) => projection([d.lng, d.lat])[0])
            .attr("cy", (d) => projection([d.lng, d.lat])[1])
            .attr("r", (d) => {
              const val = selectedMetric === "qty" ? d.qty : selectedMetric === "returns" ? d.returns : d.pos;
              return Math.max(2.5, Math.sqrt(val / 30) + 2);
            })
            .attr("fill", (d) => {
              const val = selectedMetric === "qty" ? d.qty : selectedMetric === "returns" ? d.returns : d.pos;
              return cityColorScale(val);
            })
            .attr("stroke", "rgba(255,255,255,0.6)")
            .attr("stroke-width", 0.5)
            .attr("opacity", 0.85)
            .style("pointer-events", "none");

          // City name labels (top 30 only to avoid clutter)
          const topCities = validCities.slice(0, 30);
          g.selectAll("text.city-label")
            .data(topCities)
            .enter()
            .append("text")
            .attr("class", "city-label")
            .attr("x", (d) => projection([d.lng, d.lat])[0])
            .attr("y", (d) => projection([d.lng, d.lat])[1] - 8)
            .attr("text-anchor", "middle")
            .attr("font-size", "7px")
            .attr("fill", "rgba(255,255,255,0.7)")
            .attr("font-weight", "500")
            .attr("pointer-events", "none")
            .text((d) => {
              const n = d.city;
              return n.length > 10 ? n.substring(0, 10) : n;
            });
        }

        // Gradient legend
        const legendW = 160;
        const legendH = 10;
        const lx = w - legendW - 30;
        const ly = h - 30;

        const defs = svg.append("defs");
        const lgId = "heatLegend-" + selectedMetric;
        const lg = defs.append("linearGradient").attr("id", lgId);
        lg.append("stop").attr("offset", "0%").attr("stop-color", grad.low);
        lg.append("stop").attr("offset", "40%").attr("stop-color", grad.mid);
        lg.append("stop").attr("offset", "100%").attr("stop-color", grad.high);

        svg.append("rect")
          .attr("x", lx).attr("y", ly)
          .attr("width", legendW).attr("height", legendH)
          .attr("rx", 3)
          .attr("fill", `url(#${lgId})`);

        svg.append("text").attr("x", lx).attr("y", ly - 4)
          .attr("font-size", "8px").attr("fill", "rgba(255,255,255,0.5)")
          .text("Low");
        svg.append("text").attr("x", lx + legendW).attr("y", ly - 4)
          .attr("font-size", "8px").attr("fill", "rgba(255,255,255,0.5)")
          .attr("text-anchor", "end")
          .text("High");
        const metricLabel = selectedMetric === "qty" ? "Units" : selectedMetric === "returns" ? "Returns $" : "Sales $";
        svg.append("text").attr("x", lx + legendW / 2).attr("y", ly - 4)
          .attr("font-size", "8px").attr("fill", "rgba(255,255,255,0.6)")
          .attr("text-anchor", "middle").attr("font-weight", "600")
          .text(metricLabel);
      })
      .catch((err) => console.error("Failed to load TopoJSON:", err));
  }, [showCities, showRegions, selectedMetric]);

  return <div ref={mapRef} style={{ width: "100%", minHeight: "400px" }} />;
}

// ═════════════════════════════════════════════════════════════════════════════
// RIGHT PANEL — Top 20 States, Top 20 Cities, Metro Breakdown
// ═════════════════════════════════════════════════════════════════════════════
function DataPanel({ selectedMetric }) {
  const [panelTab, setPanelTab] = useState("states");
  const metroStats = getMetroStats();

  const getMetricVal = (item) => {
    if (selectedMetric === "qty") return item.qty || 0;
    if (selectedMetric === "returns") return item.returns || 0;
    return item.pos || 0;
  };

  const fmtVal = (v) => {
    if (selectedMetric === "qty") return fN(v);
    return fmtK(v);
  };

  const topStates = [...statesSorted].sort((a, b) => getMetricVal(b) - getMetricVal(a)).slice(0, 20);
  const topCities = [...citiesSorted].sort((a, b) => getMetricVal(b) - getMetricVal(a)).slice(0, 20);

  const tabStyle = (t) => ({
    flex: 1,
    padding: "8px 0",
    fontSize: "10px",
    fontWeight: panelTab === t ? "700" : "400",
    color: panelTab === t ? "#2ECFAA" : "var(--txt3)",
    border: "none",
    borderBottom: panelTab === t ? "2px solid #2ECFAA" : "2px solid transparent",
    backgroundColor: "transparent",
    cursor: "pointer",
    textTransform: "uppercase",
    letterSpacing: ".06em",
    fontFamily: "'Space Grotesk', monospace",
  });

  const rowStyle = (i) => ({
    display: "grid",
    gridTemplateColumns: "1fr 70px 45px 60px",
    padding: "5px 8px",
    fontSize: "10px",
    borderBottom: "1px solid rgba(30,50,72,.4)",
    backgroundColor: i % 2 === 0 ? "transparent" : "rgba(20,35,55,.3)",
    alignItems: "center",
  });

  const headerRow = {
    display: "grid",
    gridTemplateColumns: "1fr 70px 45px 60px",
    padding: "6px 8px",
    fontSize: "9px",
    fontWeight: "700",
    color: "var(--txt3)",
    textTransform: "uppercase",
    letterSpacing: ".06em",
    borderBottom: "1px solid rgba(30,50,72,.6)",
    fontFamily: "'Space Grotesk', monospace",
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%", minHeight: 0 }}>
      {/* Panel Tabs */}
      <div style={{ display: "flex", borderBottom: "1px solid var(--brd)", flexShrink: 0 }}>
        <button onClick={() => setPanelTab("states")} style={tabStyle("states")}>States</button>
        <button onClick={() => setPanelTab("cities")} style={tabStyle("cities")}>Cities</button>
        <button onClick={() => setPanelTab("metro")} style={tabStyle("metro")}>Metro</button>
      </div>

      {/* Content */}
      <div style={{ flex: 1, overflowY: "auto", overflowX: "hidden" }}>
        {/* STATES */}
        {panelTab === "states" && (
          <div>
            <div style={headerRow}>
              <span>State</span>
              <span style={{ textAlign: "right" }}>Total $</span>
              <span style={{ textAlign: "right" }}>Stores</span>
              <span style={{ textAlign: "right" }}>Avg $/St</span>
            </div>
            {topStates.map((s, i) => (
              <div key={s.abbr} style={rowStyle(i)}>
                <span style={{ ...SG(10, 500), color: "var(--txt)" }}>{s.abbr} — {s.name}</span>
                <span style={{ textAlign: "right", color: "#2ECFAA", fontWeight: "600", fontSize: "10px" }}>{fmtK(s.pos)}</span>
                <span style={{ textAlign: "right", color: "var(--txt2)", fontSize: "10px" }}>{s.traited}</span>
                <span style={{ textAlign: "right", color: "var(--txt2)", fontSize: "10px" }}>{fmtAvg(s.pos, s.traited)}</span>
              </div>
            ))}
          </div>
        )}

        {/* CITIES */}
        {panelTab === "cities" && (
          <div>
            <div style={headerRow}>
              <span>City</span>
              <span style={{ textAlign: "right" }}>Total $</span>
              <span style={{ textAlign: "right" }}>Stores</span>
              <span style={{ textAlign: "right" }}>Avg $/St</span>
            </div>
            {topCities.map((c, i) => (
              <div key={c.city + c.state + i} style={rowStyle(i)}>
                <span style={{ ...SG(10, 500), color: "var(--txt)" }}>
                  {c.city.length > 14 ? c.city.substring(0, 14) + "…" : c.city}, {c.state}
                </span>
                <span style={{ textAlign: "right", color: "#2ECFAA", fontWeight: "600", fontSize: "10px" }}>{fmtK(c.pos)}</span>
                <span style={{ textAlign: "right", color: "var(--txt2)", fontSize: "10px" }}>{c.stores}</span>
                <span style={{ textAlign: "right", color: "var(--txt2)", fontSize: "10px" }}>{fmtAvg(c.pos, c.stores)}</span>
              </div>
            ))}
          </div>
        )}

        {/* METRO */}
        {panelTab === "metro" && (
          <div>
            <div style={headerRow}>
              <span>Metro Area</span>
              <span style={{ textAlign: "right" }}>Total $</span>
              <span style={{ textAlign: "right" }}>Stores</span>
              <span style={{ textAlign: "right" }}>Avg $/St</span>
            </div>
            {metroStats.map((m, i) => (
              <div key={m.name} style={rowStyle(i)}>
                <span style={{ ...SG(10, 500), color: "var(--txt)" }}>{m.name}, {m.state}</span>
                <span style={{ textAlign: "right", color: "#2ECFAA", fontWeight: "600", fontSize: "10px" }}>{fmtK(m.pos)}</span>
                <span style={{ textAlign: "right", color: "var(--txt2)", fontSize: "10px" }}>{m.stores}</span>
                <span style={{ textAlign: "right", color: "var(--txt2)", fontSize: "10px" }}>{m.stores > 0 ? fmtK(m.avg) : "—"}</span>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

// ═════════════════════════════════════════════════════════════════════════════
// TOGGLE BUTTON
// ═════════════════════════════════════════════════════════════════════════════
function ToggleBtn({ label, active, onClick, color = "#2ECFAA" }) {
  return (
    <button
      onClick={onClick}
      style={{
        padding: "6px 14px",
        fontSize: "11px",
        fontWeight: active ? "700" : "400",
        fontFamily: "'Space Grotesk', monospace",
        backgroundColor: active ? color : "transparent",
        color: active ? "#fff" : "var(--txt3)",
        border: `1px solid ${active ? color : "var(--brd)"}`,
        borderRadius: "6px",
        cursor: "pointer",
        transition: "all .15s",
      }}
    >
      {label}
    </button>
  );
}

// ═════════════════════════════════════════════════════════════════════════════
// MAIN PAGE — SINGLE VIEW, NO SUB-TABS
// ═════════════════════════════════════════════════════════════════════════════
export function WalmartStoreAnalytics() {
  const [showCities, setShowCities] = useState(false);
  const [showRegions, setShowRegions] = useState(false);
  const [selectedMetric, setSelectedMetric] = useState("pos");
  const [selectedState, setSelectedState] = useState(null);

  const totalUnits = Object.values(WM_DATA).reduce((a, s) => a + s.qty, 0);
  const totalReturns = Object.values(WM_DATA).reduce((a, s) => a + (s.returns || 0), 0);
  const statesActive = Object.values(WM_DATA).filter((s) => s.pos > 0).length;

  return (
    <div style={{ padding: "16px", display: "flex", flexDirection: "column", gap: "14px" }}>
      {/* HEADER */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <div>
          <h2 style={{ ...SG(18, 700), color: "var(--txt)", margin: 0 }}>Walmart Store Analytics</h2>
          <div style={{ ...SG(11), color: "var(--txt3)", marginTop: "2px" }}>
            POS sales, units & returns by geography
          </div>
        </div>
      </div>

      {/* KPI ROW */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: "10px" }}>
        <KPICard label="States Active" value={statesActive} color={COLORS.blue} />
        <KPICard label="Total Sales" value={fmtK(TOTAL_POS)} color={COLORS.teal} />
        <KPICard label="Total Units" value={fN(totalUnits)} color={COLORS.orange} />
        <KPICard label="Total Returns" value={fmtK(totalReturns)} color={COLORS.red} />
      </div>

      {/* CONTROLS BAR */}
      <div style={{
        display: "flex", alignItems: "center", gap: "8px", flexWrap: "wrap",
        padding: "10px 14px",
        background: "var(--card)", borderRadius: 8, border: "1px solid var(--brd)",
      }}>
        <span style={{ ...SG(10, 600), color: "var(--txt3)", textTransform: "uppercase", letterSpacing: ".06em", marginRight: 4 }}>Metric:</span>
        <ToggleBtn label="Sales $" active={selectedMetric === "pos"} onClick={() => setSelectedMetric("pos")} color="#2ECFAA" />
        <ToggleBtn label="Units" active={selectedMetric === "qty"} onClick={() => setSelectedMetric("qty")} color="#7BAED0" />
        <ToggleBtn label="Returns $" active={selectedMetric === "returns"} onClick={() => setSelectedMetric("returns")} color="#E87830" />

        <div style={{ width: 1, height: 24, background: "var(--brd)", margin: "0 8px" }} />

        <span style={{ ...SG(10, 600), color: "var(--txt3)", textTransform: "uppercase", letterSpacing: ".06em", marginRight: 4 }}>Overlays:</span>
        <ToggleBtn label="Cities" active={showCities} onClick={() => setShowCities(!showCities)} color="#a78bfa" />
        <ToggleBtn label="Regions" active={showRegions} onClick={() => setShowRegions(!showRegions)} color="#F5B731" />
      </div>

      {/* MAP + DATA PANEL — Main content area */}
      <div style={{
        display: "grid",
        gridTemplateColumns: "1fr 340px",
        gap: "14px",
        minHeight: "480px",
      }}>
        {/* LEFT: LARGE MAP */}
        <Card style={{ padding: 0, overflow: "hidden" }}>
          <USMap
            showCities={showCities}
            showRegions={showRegions}
            selectedMetric={selectedMetric}
            onSelectState={setSelectedState}
          />
        </Card>

        {/* RIGHT: DATA PANEL */}
        <Card style={{ padding: "0", display: "flex", flexDirection: "column", maxHeight: "620px" }}>
          <DataPanel selectedMetric={selectedMetric} />
        </Card>
      </div>

      {/* Selected state detail (inline) */}
      {selectedState && (() => {
        const sd = Object.values(WM_DATA).find((s) => s.abbr === selectedState);
        if (!sd) return null;
        const region = STATE_TO_REGION[sd.abbr] || "—";
        return (
          <Card>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
              <div>
                <span style={{ ...SG(14, 700), color: "var(--txt)" }}>{sd.name} ({sd.abbr})</span>
                <span style={{ ...SG(11), color: REGIONS[region]?.color || "var(--txt3)", marginLeft: 12 }}>{region}</span>
              </div>
              <button
                onClick={() => setSelectedState(null)}
                style={{
                  padding: "4px 10px", fontSize: "10px", background: "transparent",
                  color: "var(--txt3)", border: "1px solid var(--brd)", borderRadius: 4, cursor: "pointer",
                }}
              >
                ✕ Close
              </button>
            </div>
            <div style={{ display: "grid", gridTemplateColumns: "repeat(5, 1fr)", gap: "12px", marginTop: 12 }}>
              {[
                { label: "Total Sales $", value: fmtK(sd.pos), color: "#2ECFAA" },
                { label: "Units Sold", value: fN(sd.qty), color: "#7BAED0" },
                { label: "Returns $", value: fmtK(sd.returns || 0), color: "#E87830" },
                { label: "Stores", value: sd.traited, color: "#a78bfa" },
                { label: "Avg $/Store", value: fmtAvg(sd.pos, sd.traited), color: "#F5B731" },
              ].map((kpi) => (
                <div key={kpi.label} style={{
                  padding: "10px", borderRadius: 8,
                  background: "rgba(20,35,55,.4)", borderTop: `2px solid ${kpi.color}`,
                }}>
                  <div style={{ ...SG(8, 600), color: "var(--txt3)", textTransform: "uppercase", letterSpacing: ".06em" }}>{kpi.label}</div>
                  <div style={{ ...DM(16), color: "var(--txt)", marginTop: 4 }}>{kpi.value}</div>
                </div>
              ))}
            </div>
          </Card>
        );
      })()}
    </div>
  );
}

export default WalmartStoreAnalytics;
