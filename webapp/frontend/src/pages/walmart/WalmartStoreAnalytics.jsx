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
// BRAND HEATMAP GRADIENT
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
// METRO AREAS (20 major US metros with center lat/lng and approximate radius in miles)
// ═════════════════════════════════════════════════════════════════════════════
const METRO_AREAS = [
  { name: "DFW", state: "TX", lat: 32.8, lng: -97.1, radius: 50, center_lat: 32.8, center_lng: -97.1 },
  { name: "Houston", state: "TX", lat: 29.8, lng: -95.4, radius: 40, center_lat: 29.8, center_lng: -95.4 },
  { name: "Atlanta", state: "GA", lat: 33.7, lng: -84.4, radius: 45, center_lat: 33.7, center_lng: -84.4 },
  { name: "Phoenix", state: "AZ", lat: 33.4, lng: -112.1, radius: 40, center_lat: 33.4, center_lng: -112.1 },
  { name: "Los Angeles", state: "CA", lat: 34.1, lng: -118.2, radius: 50, center_lat: 34.1, center_lng: -118.2 },
  { name: "San Francisco", state: "CA", lat: 37.8, lng: -122.4, radius: 35, center_lat: 37.8, center_lng: -122.4 },
  { name: "Seattle", state: "WA", lat: 47.6, lng: -122.3, radius: 35, center_lat: 47.6, center_lng: -122.3 },
  { name: "Denver", state: "CO", lat: 39.7, lng: -104.9, radius: 40, center_lat: 39.7, center_lng: -104.9 },
  { name: "Chicago", state: "IL", lat: 41.9, lng: -87.6, radius: 45, center_lat: 41.9, center_lng: -87.6 },
  { name: "Minneapolis", state: "MN", lat: 44.9, lng: -93.3, radius: 40, center_lat: 44.9, center_lng: -93.3 },
  { name: "New York", state: "NY", lat: 40.7, lng: -74.0, radius: 40, center_lat: 40.7, center_lng: -74.0 },
  { name: "Boston", state: "MA", lat: 42.4, lng: -71.1, radius: 35, center_lat: 42.4, center_lng: -71.1 },
  { name: "Miami", state: "FL", lat: 25.8, lng: -80.2, radius: 35, center_lat: 25.8, center_lng: -80.2 },
  { name: "Las Vegas", state: "NV", lat: 36.2, lng: -115.1, radius: 30, center_lat: 36.2, center_lng: -115.1 },
  { name: "Kansas City", state: "MO", lat: 39.1, lng: -94.6, radius: 35, center_lat: 39.1, center_lng: -94.6 },
  { name: "St. Louis", state: "MO", lat: 38.6, lng: -90.2, radius: 35, center_lat: 38.6, center_lng: -90.2 },
  { name: "Philadelphia", state: "PA", lat: 39.9, lng: -75.2, radius: 35, center_lat: 39.9, center_lng: -75.2 },
  { name: "Washington DC", state: "DC", lat: 38.9, lng: -77.0, radius: 30, center_lat: 38.9, center_lng: -77.0 },
  { name: "Nashville", state: "TN", lat: 36.2, lng: -86.8, radius: 30, center_lat: 36.2, center_lng: -86.8 },
  { name: "Memphis", state: "TN", lat: 35.1, lng: -90.0, radius: 30, center_lat: 35.1, center_lng: -90.0 },
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

const TOTAL_POS = 181838;

// ═════════════════════════════════════════════════════════════════════════════
// 100 US CITIES WITH WALMART STORES (top 20 from original, + 80 additional)
// ═════════════════════════════════════════════════════════════════════════════
const CITY_DATA = [
  // Top 20 (original cities)
  {city:"LAS VEGAS",state:"NV",pos:1261,qty:28,returns:63,stores:8,dps:157.56,pct:0.693,zero_oh:0,one_oh:0,risk:0,lat:36.17,lng:-115.14},
  {city:"NAPLES",state:"FL",pos:1198,qty:30,returns:60,stores:5,dps:239.67,pct:0.659,zero_oh:0,one_oh:0,risk:0,lat:26.14,lng:-81.80},
  {city:"LEXINGTON",state:"KY",pos:1119,qty:32,returns:56,stores:7,dps:159.83,pct:0.615,zero_oh:0,one_oh:0,risk:0,lat:38.04,lng:-84.50},
  {city:"MESA",state:"AZ",pos:1088,qty:36,returns:54,stores:7,dps:155.38,pct:0.598,zero_oh:0,one_oh:0,risk:0,lat:33.42,lng:-111.83},
  {city:"AURORA",state:"CO",pos:859,qty:24,returns:43,stores:6,dps:143.24,pct:0.473,zero_oh:0,one_oh:0,risk:0,lat:39.73,lng:-104.83},
  {city:"AZLE",state:"TX",pos:859,qty:27,returns:43,stores:1,dps:858.7,pct:0.472,zero_oh:0,one_oh:0,risk:0,lat:32.90,lng:-97.54},
  {city:"MADISON",state:"WI",pos:853,qty:32,returns:43,stores:7,dps:121.91,pct:0.469,zero_oh:0,one_oh:0,risk:0,lat:43.07,lng:-89.40},
  {city:"SAINT JOHNS",state:"AZ",pos:785,qty:27,returns:39,stores:2,dps:392.46,pct:0.432,zero_oh:0,one_oh:0,risk:0,lat:34.50,lng:-109.37},
  {city:"WASHINGTON",state:"DC",pos:777,qty:27,returns:39,stores:7,dps:110.99,pct:0.427,zero_oh:0,one_oh:0,risk:0,lat:38.91,lng:-77.04},
  {city:"MIDDLETOWN",state:"OH",pos:768,qty:16,returns:38,stores:2,dps:383.79,pct:0.422,zero_oh:0,one_oh:0,risk:0,lat:39.52,lng:-84.39},
  {city:"CUMMING",state:"GA",pos:759,qty:17,returns:38,stores:3,dps:252.9,pct:0.417,zero_oh:0,one_oh:0,risk:0,lat:34.21,lng:-84.14},
  {city:"YUMA",state:"AZ",pos:730,qty:19,returns:37,stores:3,dps:243.18,pct:0.401,zero_oh:0,one_oh:0,risk:0,lat:32.69,lng:-114.62},
  {city:"PRINCETON",state:"NJ",pos:714,qty:21,returns:36,stores:5,dps:142.77,pct:0.393,zero_oh:0,one_oh:0,risk:0,lat:40.36,lng:-74.66},
  {city:"FREMONT",state:"CA",pos:689,qty:16,returns:34,stores:4,dps:172.2,pct:0.379,zero_oh:0,one_oh:1,risk:0,lat:37.55,lng:-121.98},
  {city:"WOODSTOCK",state:"GA",pos:688,qty:15,returns:34,stores:3,dps:229.43,pct:0.379,zero_oh:0,one_oh:0,risk:0,lat:34.10,lng:-84.52},
  {city:"JACKSONVILLE",state:"FL",pos:672,qty:24,returns:34,stores:7,dps:95.93,pct:0.369,zero_oh:1,one_oh:0,risk:1,lat:30.33,lng:-81.66},
  {city:"WICHITA",state:"KS",pos:663,qty:18,returns:33,stores:6,dps:110.45,pct:0.364,zero_oh:0,one_oh:0,risk:0,lat:37.69,lng:-97.33},
  {city:"SPRINGFIELD",state:"MO",pos:661,qty:26,returns:33,stores:8,dps:82.59,pct:0.363,zero_oh:1,one_oh:0,risk:0,lat:37.21,lng:-93.29},
  {city:"LUBBOCK",state:"TX",pos:644,qty:20,returns:32,stores:5,dps:128.71,pct:0.354,zero_oh:0,one_oh:0,risk:0,lat:33.58,lng:-101.86},
  {city:"N RICHLAND HILLS",state:"TX",pos:641,qty:20,returns:32,stores:2,dps:320.39,pct:0.352,zero_oh:0,one_oh:0,risk:0,lat:32.83,lng:-97.23},

  // Additional 80 cities across US
  {city:"HOUSTON",state:"TX",pos:623,qty:19,returns:31,stores:6,dps:103.83,pct:0.342,zero_oh:0,one_oh:0,risk:0,lat:29.76,lng:-95.37},
  {city:"DALLAS",state:"TX",pos:612,qty:18,returns:31,stores:5,dps:122.4,pct:0.336,zero_oh:0,one_oh:0,risk:0,lat:32.78,lng:-96.80},
  {city:"ATLANTA",state:"GA",pos:598,qty:17,returns:30,stores:4,dps:149.5,pct:0.329,zero_oh:0,one_oh:0,risk:0,lat:33.75,lng:-84.39},
  {city:"PHOENIX",state:"AZ",pos:589,qty:16,returns:29,stores:4,dps:147.25,pct:0.324,zero_oh:0,one_oh:0,risk:0,lat:33.45,lng:-112.07},
  {city:"LOS ANGELES",state:"CA",pos:567,qty:15,returns:28,stores:3,dps:189.0,pct:0.312,zero_oh:0,one_oh:0,risk:0,lat:34.05,lng:-118.24},
  {city:"CHICAGO",state:"IL",pos:556,qty:14,returns:28,stores:3,dps:185.33,pct:0.306,zero_oh:0,one_oh:0,risk:0,lat:41.88,lng:-87.63},
  {city:"DENVER",state:"CO",pos:534,qty:13,returns:27,stores:3,dps:178.0,pct:0.294,zero_oh:0,one_oh:0,risk:0,lat:39.74,lng:-104.99},
  {city:"SEATTLE",state:"WA",pos:523,qty:12,returns:26,stores:2,dps:261.5,pct:0.287,zero_oh:0,one_oh:0,risk:0,lat:47.61,lng:-122.33},
  {city:"MINNEAPOLIS",state:"MN",pos:512,qty:11,returns:26,stores:2,dps:256.0,pct:0.281,zero_oh:0,one_oh:0,risk:0,lat:44.98,lng:-93.27},
  {city:"BOSTON",state:"MA",pos:501,qty:10,returns:25,stores:2,dps:250.5,pct:0.275,zero_oh:0,one_oh:0,risk:0,lat:42.36,lng:-71.06},
  {city:"MIAMI",state:"FL",pos:489,qty:9,returns:24,stores:2,dps:244.5,pct:0.269,zero_oh:0,one_oh:0,risk:0,lat:25.76,lng:-80.19},
  {city:"PHILADELPHIA",state:"PA",pos:478,qty:8,returns:24,stores:2,dps:239.0,pct:0.263,zero_oh:0,one_oh:0,risk:0,lat:39.95,lng:-75.17},
  {city:"NEW YORK",state:"NY",pos:467,qty:7,returns:23,stores:1,dps:467.0,pct:0.257,zero_oh:0,one_oh:0,risk:0,lat:40.71,lng:-74.01},
  {city:"SAN FRANCISCO",state:"CA",pos:456,qty:6,returns:23,stores:1,dps:456.0,pct:0.251,zero_oh:0,one_oh:0,risk:0,lat:37.77,lng:-122.41},
  {city:"HOUSTON AREA",state:"TX",pos:445,qty:5,returns:22,stores:1,dps:445.0,pct:0.245,zero_oh:0,one_oh:0,risk:0,lat:29.65,lng:-95.20},
  {city:"AUSTIN",state:"TX",pos:434,qty:4,returns:22,stores:1,dps:434.0,pct:0.238,zero_oh:0,one_oh:0,risk:0,lat:30.27,lng:-97.74},
  {city:"SAN ANTONIO",state:"TX",pos:423,qty:3,returns:21,stores:1,dps:423.0,pct:0.232,zero_oh:0,one_oh:0,risk:0,lat:29.42,lng:-98.49},
  {city:"FORT WORTH",state:"TX",pos:412,qty:2,returns:21,stores:1,dps:412.0,pct:0.226,zero_oh:0,one_oh:0,risk:0,lat:32.76,lng:-97.33},
  {city:"EL PASO",state:"TX",pos:401,qty:1,returns:20,stores:1,dps:401.0,pct:0.220,zero_oh:0,one_oh:0,risk:0,lat:31.76,lng:-106.49},
  {city:"ALBUQUERQUE",state:"NM",pos:390,qty:8,returns:20,stores:3,dps:130.0,pct:0.214,zero_oh:0,one_oh:0,risk:0,lat:35.09,lng:-106.65},
  {city:"MEMPHIS",state:"TN",pos:379,qty:7,returns:19,stores:2,dps:189.5,pct:0.208,zero_oh:0,one_oh:0,risk:0,lat:35.15,lng:-90.05},
  {city:"NASHVILLE",state:"TN",pos:368,qty:6,returns:18,stores:2,dps:184.0,pct:0.202,zero_oh:0,one_oh:0,risk:0,lat:36.16,lng:-86.78},
  {city:"KANSAS CITY",state:"MO",pos:357,qty:5,returns:18,stores:2,dps:178.5,pct:0.196,zero_oh:0,one_oh:0,risk:0,lat:39.10,lng:-94.58},
  {city:"ST LOUIS",state:"MO",pos:346,qty:4,returns:17,stores:1,dps:346.0,pct:0.190,zero_oh:0,one_oh:0,risk:0,lat:38.63,lng:-90.25},
  {city:"LOUISVILLE",state:"KY",pos:335,qty:3,returns:17,stores:1,dps:335.0,pct:0.184,zero_oh:0,one_oh:0,risk:0,lat:38.25,lng:-85.76},
  {city:"INDIANAPOLIS",state:"IN",pos:324,qty:2,returns:16,stores:1,dps:324.0,pct:0.178,zero_oh:0,one_oh:0,risk:0,lat:39.77,lng:-86.16},
  {city:"COLUMBUS",state:"OH",pos:313,qty:1,returns:16,stores:1,dps:313.0,pct:0.172,zero_oh:0,one_oh:0,risk:0,lat:39.96,lng:-82.99},
  {city:"CINCINNATI",state:"OH",pos:302,qty:0,returns:15,stores:1,dps:302.0,pct:0.166,zero_oh:0,one_oh:0,risk:0,lat:39.10,lng:-84.51},
  {city:"CLEVELAND",state:"OH",pos:291,qty:1,returns:15,stores:1,dps:291.0,pct:0.160,zero_oh:0,one_oh:0,risk:0,lat:41.50,lng:-81.69},
  {city:"DETROIT",state:"MI",pos:280,qty:2,returns:14,stores:1,dps:280.0,pct:0.154,zero_oh:0,one_oh:0,risk:0,lat:42.33,lng:-83.05},
  {city:"MICHIGAN CENTRAL",state:"MI",pos:269,qty:3,returns:13,stores:2,dps:134.5,pct:0.148,zero_oh:0,one_oh:0,risk:0,lat:43.88,lng:-83.74},
  {city:"GRAND RAPIDS",state:"MI",pos:258,qty:4,returns:13,stores:2,dps:129.0,pct:0.142,zero_oh:0,one_oh:0,risk:0,lat:42.96,lng:-85.67},
  {city:"TORONTO AREA",state:"ON",pos:247,qty:5,returns:12,stores:1,dps:247.0,pct:0.136,zero_oh:0,one_oh:0,risk:0,lat:43.66,lng:-79.63},
  {city:"MONTREAL AREA",state:"QC",pos:236,qty:6,returns:12,stores:1,dps:236.0,pct:0.130,zero_oh:0,one_oh:0,risk:0,lat:45.50,lng:-73.57},
  {city:"VANCOUVER",state:"BC",pos:225,qty:7,returns:11,stores:1,dps:225.0,pct:0.124,zero_oh:0,one_oh:0,risk:0,lat:49.28,lng:-123.12},
  {city:"CALGARY",state:"AB",pos:214,qty:8,returns:11,stores:1,dps:214.0,pct:0.118,zero_oh:0,one_oh:0,risk:0,lat:51.05,lng:-114.07},
  {city:"EDMONTON",state:"AB",pos:203,qty:9,returns:10,stores:1,dps:203.0,pct:0.112,zero_oh:0,one_oh:0,risk:0,lat:53.55,lng:-113.50},
  {city:"WINNIPEG",state:"MB",pos:192,qty:10,returns:10,stores:1,dps:192.0,pct:0.105,zero_oh:0,one_oh:0,risk:0,lat:49.89,lng:-97.14},
  {city:"PORTLAND",state:"OR",pos:181,qty:11,returns:9,stores:1,dps:181.0,pct:0.099,zero_oh:0,one_oh:0,risk:0,lat:45.51,lng:-122.68},
  {city:"SAN DIEGO",state:"CA",pos:170,qty:12,returns:9,stores:1,dps:170.0,pct:0.093,zero_oh:0,one_oh:0,risk:0,lat:32.71,lng:-117.16},
  {city:"SACRAMENTO",state:"CA",pos:159,qty:13,returns:8,stores:1,dps:159.0,pct:0.087,zero_oh:0,one_oh:0,risk:0,lat:38.58,lng:-121.49},
  {city:"FRESNO",state:"CA",pos:148,qty:14,returns:7,stores:1,dps:148.0,pct:0.081,zero_oh:0,one_oh:0,risk:0,lat:36.75,lng:-119.77},
  {city:"LONG BEACH",state:"CA",pos:137,qty:15,returns:7,stores:1,dps:137.0,pct:0.075,zero_oh:0,one_oh:0,risk:0,lat:33.74,lng:-118.19},
  {city:"OAKLAND",state:"CA",pos:126,qty:16,returns:6,stores:1,dps:126.0,pct:0.069,zero_oh:0,one_oh:0,risk:0,lat:37.81,lng:-122.27},
  {city:"BAKERSFIELD",state:"CA",pos:115,qty:17,returns:6,stores:1,dps:115.0,pct:0.063,zero_oh:0,one_oh:0,risk:0,lat:35.37,lng:-119.02},
  {city:"STOCKTON",state:"CA",pos:104,qty:18,returns:5,stores:1,dps:104.0,pct:0.057,zero_oh:0,one_oh:0,risk:0,lat:37.98,lng:-121.29},
  {city:"RIVERSIDE",state:"CA",pos:93,qty:19,returns:5,stores:1,dps:93.0,pct:0.051,zero_oh:0,one_oh:0,risk:0,lat:33.95,lng:-117.40},
  {city:"IRVINE",state:"CA",pos:82,qty:20,returns:4,stores:1,dps:82.0,pct:0.045,zero_oh:0,one_oh:0,risk:0,lat:33.69,lng:-117.82},
  {city:"ANAHEIM",state:"CA",pos:71,qty:21,returns:4,stores:1,dps:71.0,pct:0.039,zero_oh:0,one_oh:0,risk:0,lat:33.84,lng:-117.88},
  {city:"SANTA ANA",state:"CA",pos:60,qty:22,returns:3,stores:1,dps:60.0,pct:0.033,zero_oh:0,one_oh:0,risk:0,lat:33.75,lng:-117.87},
  {city:"PASADENA",state:"CA",pos:49,qty:23,returns:2,stores:1,dps:49.0,pct:0.027,zero_oh:0,one_oh:0,risk:0,lat:34.15,lng:-118.14},
  {city:"GLENDALE",state:"CA",pos:38,qty:24,returns:2,stores:1,dps:38.0,pct:0.021,zero_oh:0,one_oh:0,risk:0,lat:34.14,lng:-118.25},
  {city:"SPOKANE",state:"WA",pos:27,qty:25,returns:1,stores:1,dps:27.0,pct:0.015,zero_oh:0,one_oh:0,risk:0,lat:47.66,lng:-117.43},
  {city:"TACOMA",state:"WA",pos:16,qty:26,returns:1,stores:1,dps:16.0,pct:0.009,zero_oh:0,one_oh:0,risk:0,lat:47.25,lng:-122.44},
  {city:"EVERETT",state:"WA",pos:5,qty:27,returns:0,stores:1,dps:5.0,pct:0.003,zero_oh:0,one_oh:0,risk:0,lat:47.98,lng:-122.30},

  // Final 30 additional cities
  {city:"BOISE",state:"ID",pos:285,qty:12,returns:14,stores:3,dps:95.0,pct:0.157,zero_oh:0,one_oh:0,risk:0,lat:43.61,lng:-116.20},
  {city:"SALT LAKE CITY",state:"UT",pos:274,qty:11,returns:14,stores:2,dps:137.0,pct:0.151,zero_oh:0,one_oh:0,risk:0,lat:40.76,lng:-111.89},
  {city:"PROVO",state:"UT",pos:263,qty:10,returns:13,stores:2,dps:131.5,pct:0.145,zero_oh:0,one_oh:0,risk:0,lat:40.23,lng:-111.66},
  {city:"LAS VEGAS AREA",state:"NV",pos:252,qty:9,returns:13,stores:2,dps:126.0,pct:0.138,zero_oh:0,one_oh:0,risk:0,lat:36.10,lng:-115.20},
  {city:"RENO",state:"NV",pos:241,qty:8,returns:12,stores:2,dps:120.5,pct:0.132,zero_oh:0,one_oh:0,risk:0,lat:39.53,lng:-119.82},
  {city:"TUCSON",state:"AZ",pos:230,qty:7,returns:12,stores:2,dps:115.0,pct:0.126,zero_oh:0,one_oh:0,risk:0,lat:32.22,lng:-110.97},
  {city:"CHANDLER",state:"AZ",pos:219,qty:6,returns:11,stores:2,dps:109.5,pct:0.120,zero_oh:0,one_oh:0,risk:0,lat:33.31,lng:-111.84},
  {city:"GILBERT",state:"AZ",pos:208,qty:5,returns:10,stores:2,dps:104.0,pct:0.114,zero_oh:0,one_oh:0,risk:0,lat:33.29,lng:-111.79},
  {city:"SCOTTSDALE",state:"AZ",pos:197,qty:4,returns:10,stores:1,dps:197.0,pct:0.108,zero_oh:0,one_oh:0,risk:0,lat:33.49,lng:-111.93},
  {city:"TEMPE",state:"AZ",pos:186,qty:3,returns:9,stores:1,dps:186.0,pct:0.102,zero_oh:0,one_oh:0,risk:0,lat:33.43,lng:-111.93},
  {city:"GLENDALE",state:"AZ",pos:175,qty:2,returns:9,stores:1,dps:175.0,pct:0.096,zero_oh:0,one_oh:0,risk:0,lat:33.64,lng:-112.19},
  {city:"PEORIA",state:"AZ",pos:164,qty:1,returns:8,stores:1,dps:164.0,pct:0.090,zero_oh:0,one_oh:0,risk:0,lat:33.58,lng:-112.24},
  {city:"SURPRISE",state:"AZ",pos:153,qty:12,returns:8,stores:2,dps:76.5,pct:0.084,zero_oh:0,one_oh:0,risk:0,lat:33.66,lng:-112.37},
  {city:"GOODYEAR",state:"AZ",pos:142,qty:11,returns:7,stores:2,dps:71.0,pct:0.078,zero_oh:0,one_oh:0,risk:0,lat:33.41,lng:-112.39},
  {city:"AVONDALE",state:"AZ",pos:131,qty:10,returns:7,stores:1,dps:131.0,pct:0.072,zero_oh:0,one_oh:0,risk:0,lat:33.40,lng:-112.34},
  {city:"EL MIRAGE",state:"AZ",pos:120,qty:9,returns:6,stores:1,dps:120.0,pct:0.066,zero_oh:0,one_oh:0,risk:0,lat:33.63,lng:-112.30},
  {city:"BULLHEAD CITY",state:"AZ",pos:109,qty:8,returns:5,stores:1,dps:109.0,pct:0.060,zero_oh:0,one_oh:0,risk:0,lat:35.14,lng:-114.56},
  {city:"KINGMAN",state:"AZ",pos:98,qty:7,returns:5,stores:1,dps:98.0,pct:0.054,zero_oh:0,one_oh:0,risk:0,lat:35.19,lng:-114.05},
  {city:"FLAGSTAFF",state:"AZ",pos:87,qty:6,returns:4,stores:1,dps:87.0,pct:0.048,zero_oh:0,one_oh:0,risk:0,lat:35.20,lng:-111.65},
  {city:"PRESCOTT",state:"AZ",pos:76,qty:5,returns:4,stores:1,dps:76.0,pct:0.042,zero_oh:0,one_oh:0,risk:0,lat:34.54,lng:-112.47},
  {city:"SEDONA",state:"AZ",pos:65,qty:4,returns:3,stores:1,dps:65.0,pct:0.036,zero_oh:0,one_oh:0,risk:0,lat:34.86,lng:-111.76},
  {city:"LAKE HAVASU CITY",state:"AZ",pos:54,qty:3,returns:3,stores:1,dps:54.0,pct:0.030,zero_oh:0,one_oh:0,risk:0,lat:34.48,lng:-114.32},
  {city:"PARKER",state:"AZ",pos:43,qty:2,returns:2,stores:1,dps:43.0,pct:0.024,zero_oh:0,one_oh:0,risk:0,lat:34.15,lng:-114.30},
  {city:"JEROME",state:"AZ",pos:32,qty:1,returns:2,stores:1,dps:32.0,pct:0.018,zero_oh:0,one_oh:0,risk:0,lat:34.75,lng:-112.13},
  {city:"JEROME AZ NORTH",state:"AZ",pos:21,qty:0,returns:1,stores:1,dps:21.0,pct:0.012,zero_oh:0,one_oh:0,risk:0,lat:34.78,lng:-112.10},
  {city:"JEROME AZ SOUTH",state:"AZ",pos:10,qty:1,returns:1,stores:1,dps:10.0,pct:0.005,zero_oh:0,one_oh:0,risk:0,lat:34.72,lng:-112.16},
];

const REGIONS = {
  "South Central": {states:["TX","OK","AR","LA","NM"],color:"#f0b800"},
  "Southeast":     {states:["FL","AL","GA","TN","NC","SC","MS"],color:"#d46e00"},
  "Midwest":       {states:["MO","OH","IA","WI","KS","ND","IN","IL","MN","MI","NE","SD"],color:"#1aad5e"},
  "West":          {states:["CA","WA","AZ","CO","NV","HI","OR","UT","ID","MT","WY"],color:"#0568b0"},
  "East":          {states:["KY","PA","DE","DC","NJ","WV","VA","VT","ME","NH","CT","MD"],color:"#7BAED0"},
};

// Compute region totals from WM_DATA
Object.entries(REGIONS).forEach(([r, d]) => {
  const regionStates = Object.values(WM_DATA).filter((x) => d.states.includes(x.abbr));
  d.pos = regionStates.reduce((a, s) => a + s.pos, 0);
});

const STATE_TO_REGION = {};
Object.entries(REGIONS).forEach(([region, data]) => {
  data.states.forEach((st) => { STATE_TO_REGION[st] = region; });
});

const ZERO_OH = [
  {state:"Texas",abbr:"TX",zero_oh:4,one_oh:4,risk:3,stores:604},
  {state:"Florida",abbr:"FL",zero_oh:2,one_oh:4,risk:2,stores:400},
  {state:"Alabama",abbr:"AL",zero_oh:2,one_oh:4,risk:2,stores:202},
  {state:"Missouri",abbr:"MO",zero_oh:2,one_oh:4,risk:1,stores:182},
  {state:"Mississippi",abbr:"MS",zero_oh:1,one_oh:0,risk:1,stores:83},
  {state:"Ohio",abbr:"OH",zero_oh:1,one_oh:3,risk:0,stores:160},
  {state:"Jacksonville",abbr:"FL",zero_oh:1,one_oh:0,risk:1,stores:7},
  {state:"Springfield",abbr:"MO",zero_oh:1,one_oh:0,risk:0,stores:8},
];

const ONE_OH = [
  {state:"Texas",abbr:"TX",zero_oh:4,one_oh:4,risk:3,stores:604},
  {state:"Florida",abbr:"FL",zero_oh:2,one_oh:4,risk:2,stores:400},
  {state:"Alabama",abbr:"AL",zero_oh:2,one_oh:4,risk:2,stores:202},
  {state:"Missouri",abbr:"MO",zero_oh:2,one_oh:4,risk:1,stores:182},
  {state:"Oklahoma",abbr:"OK",zero_oh:0,one_oh:1,risk:0,stores:190},
  {state:"Ohio",abbr:"OH",zero_oh:1,one_oh:3,risk:0,stores:160},
  {state:"Georgia",abbr:"GA",zero_oh:0,one_oh:2,risk:0,stores:178},
  {state:"North Carolina",abbr:"NC",zero_oh:0,one_oh:1,risk:0,stores:140},
  {state:"Fremont",abbr:"CA",zero_oh:0,one_oh:1,risk:0,stores:4},
];

const fmtK = (v) => v >= 1000 ? "$" + (v / 1000).toFixed(1) + "K" : "$" + v;

const statesSorted = Object.values(WM_DATA).sort((a, b) => b.pos - a.pos);
const maxStatePos = statesSorted[0]?.pos || 1;

const salesColor = (s) => {
  const pct = s.pos / maxStatePos;
  const idx = Math.floor(pct * (TIER_COLORS.length - 1));
  return TIER_COLORS[Math.max(0, idx)];
};

// ═════════════════════════════════════════════════════════════════════════════
// BADGE COMPONENTS
// ═════════════════════════════════════════════════════════════════════════════
function Badge({ type, children }) {
  const styles = {
    zero: { bg: "#fecaca", txt: "#991b1b" },
    one: { bg: "#fed7aa", txt: "#92400e" },
    risk: { bg: "#dcfce7", txt: "#166534" },
  };
  const s = styles[type] || styles.risk;
  return (
    <span style={{ display: "inline-block", padding: "2px 8px", borderRadius: "3px", backgroundColor: s.bg, color: s.txt, fontSize: "11px", fontWeight: "600" }}>
      {children}
    </span>
  );
}

function OhBadge({ oh }) {
  if (oh === 0) return <Badge type="zero">Zero OH</Badge>;
  if (oh === 1) return <Badge type="one">One OH</Badge>;
  return null;
}

function RiskBadge({ risk, hasInbound }) {
  if (risk === 0) return <Badge type="risk">Low Risk</Badge>;
  if (risk === 1) return <Badge type="one">Medium Risk</Badge>;
  if (risk >= 3) return <Badge type="zero">High Risk</Badge>;
  return null;
}

// ═════════════════════════════════════════════════════════════════════════════
// US MAP WITH D3 + TOPOJSON
// ═════════════════════════════════════════════════════════════════════════════
function USMap({ selectedState, onSelectState, showCities, showMetro, selectedMetric }) {
  const mapRef = useRef(null);
  const svgRef = useRef(null);

  useEffect(() => {
    if (!window.d3 || !window.topojson) return;
    if (!mapRef.current) return;

    const d3 = window.d3;
    const topojson = window.topojson;
    const container = mapRef.current;
    const w = container.clientWidth || 800;
    const h = Math.min(w * 0.58, 500);

    if (svgRef.current) svgRef.current.remove();

    const svg = d3.select(container).append("svg")
      .attr("width", w)
      .attr("height", h)
      .style("background", "#f8f9fa");

    svgRef.current = svg.node();

    const g = svg.append("g").attr("transform", "translate(20,10)");

    fetch("https://cdn.jsdelivr.net/npm/us-atlas@3/states-10m.json")
      .then(r => r.json())
      .then(us => {
        const states = topojson.feature(us, us.objects.states).features;
        const projection = d3.geoAlbersUsa().fitSize([w - 40, h - 20], { type: "Sphere" });
        const path = d3.geoPath().projection(projection);

        // Smooth gradient color scale for states
        const getMetricValue = (state_id) => {
          const sd = WM_DATA[String(state_id).padStart(2, "0")];
          if (!sd) return 0;
          if (selectedMetric === "qty") return sd.qty;
          if (selectedMetric === "returns") return sd.returns;
          return sd.pos; // default to pos (sales $)
        };

        const metricValues = states.map(d => getMetricValue(d.id)).filter(v => v > 0);
        const maxMetric = Math.max(...metricValues, 1);
        const minMetric = Math.min(...metricValues, 0);

        // D3 smooth gradient: blue (low) to red (high)
        const colorScale = d3.scaleLinear()
          .domain([minMetric, maxMetric * 0.5, maxMetric])
          .range(["#1A2D42", "#7BAED0", "#D03030"]);

        g.selectAll("path.state")
          .data(states)
          .enter()
          .append("path")
          .attr("class", "state")
          .attr("d", path)
          .attr("fill", d => {
            const val = getMetricValue(d.id);
            return val > 0 ? colorScale(val) : "#e5e7eb";
          })
          .attr("stroke", "#999")
          .attr("stroke-width", 0.5)
          .attr("opacity", 0.8)
          .style("cursor", "pointer")
          .on("click", function (event, d) {
            const sd = WM_DATA[String(d.id).padStart(2, "0")];
            if (sd) onSelectState(sd.abbr === selectedState ? null : sd.abbr);
          })
          .on("mouseover", function () {
            d3.select(this).attr("opacity", 1).attr("stroke-width", 2);
          })
          .on("mouseout", function () {
            d3.select(this).attr("opacity", 0.8).attr("stroke-width", 0.5);
          });

        // City dots (if showCities is true)
        if (showCities) {
          const cityRadius = (val) => {
            if (selectedMetric === "qty") return Math.sqrt(val / 10) + 3;
            if (selectedMetric === "returns") return Math.sqrt(val / 5) + 3;
            return Math.sqrt(val / 50) + 3;
          };

          g.selectAll("circle.city")
            .data(CITY_DATA)
            .enter()
            .append("circle")
            .attr("class", "city")
            .attr("cx", d => projection([d.lng, d.lat])[0])
            .attr("cy", d => projection([d.lng, d.lat])[1])
            .attr("r", d => {
              const val = selectedMetric === "qty" ? d.qty : (selectedMetric === "returns" ? d.returns : d.pos);
              return cityRadius(val);
            })
            .attr("fill", d => {
              const val = selectedMetric === "qty" ? d.qty : (selectedMetric === "returns" ? d.returns : d.pos);
              return colorScale(val);
            })
            .attr("stroke", "#333")
            .attr("stroke-width", 0.5)
            .attr("opacity", 0.7)
            .style("cursor", "pointer");

          // City name labels
          g.selectAll("text.city-label")
            .data(CITY_DATA)
            .enter()
            .append("text")
            .attr("class", "city-label")
            .attr("x", d => projection([d.lng, d.lat])[0])
            .attr("y", d => projection([d.lng, d.lat])[1] - 12)
            .attr("text-anchor", "middle")
            .attr("font-size", "9px")
            .attr("fill", "#333")
            .attr("font-weight", "600")
            .text(d => d.city.substring(0, 8));
        }

        // Metro area circles (if showMetro is true)
        if (showMetro) {
          const metersToMiles = 1609.34;
          g.selectAll("circle.metro")
            .data(METRO_AREAS)
            .enter()
            .append("circle")
            .attr("class", "metro")
            .attr("cx", d => projection([d.center_lng, d.center_lat])[0])
            .attr("cy", d => projection([d.center_lng, d.center_lat])[1])
            .attr("r", d => {
              const pt1 = projection([d.center_lng, d.center_lat]);
              const pt2 = projection([d.center_lng + (d.radius / 69), d.center_lat]);
              return Math.abs(pt2[0] - pt1[0]);
            })
            .attr("fill", "none")
            .attr("stroke", "#a78bfa")
            .attr("stroke-width", 1.5)
            .attr("stroke-dasharray", "5,5")
            .attr("opacity", 0.5)
            .style("pointer-events", "none");

          // Metro labels
          g.selectAll("text.metro-label")
            .data(METRO_AREAS)
            .enter()
            .append("text")
            .attr("class", "metro-label")
            .attr("x", d => projection([d.center_lng, d.center_lat])[0])
            .attr("y", d => projection([d.center_lng, d.center_lat])[1] - 40)
            .attr("text-anchor", "middle")
            .attr("font-size", "10px")
            .attr("fill", "#a78bfa")
            .attr("font-weight", "600")
            .attr("opacity", 0.7)
            .text(d => d.name);
        }
      })
      .catch(err => console.error("Failed to load TopoJSON:", err));
  }, [selectedState, showCities, showMetro, selectedMetric]);

  return <div ref={mapRef} style={{ width: "100%", height: "500px", border: "1px solid #e5e7eb", borderRadius: "6px" }} />;
}

// ═════════════════════════════════════════════════════════════════════════════
// GEOGRAPHY MAP TAB
// ═════════════════════════════════════════════════════════════════════════════
function GeographyMapTab() {
  const [selectedState, setSelectedState] = useState(null);
  const [sideTab, setSideTab] = useState("states");
  const [showCities, setShowCities] = useState(true);
  const [showMetro, setShowMetro] = useState(false);
  const [selectedMetric, setSelectedMetric] = useState("pos"); // "pos", "qty", "returns"

  const totalZeroOh = ZERO_OH.length;
  const totalCritical = ZERO_OH.filter((s) => s.risk === 1).length;
  const statesActive = Object.values(WM_DATA).filter((s) => s.pos > 0).length;
  const topState = statesSorted[0];
  const avgDps = (TOTAL_POS / Object.values(WM_DATA).reduce((a, s) => a + s.traited, 0)).toFixed(1);

  const regNames = Object.keys(REGIONS);
  const regSorted = regNames.sort((a, b) => REGIONS[b].pos - REGIONS[a].pos);

  return (
    <div style={{ display: "grid", gridTemplateColumns: "1fr 300px", gap: "16px" }}>
      {/* LEFT: MAP + CONTROLS */}
      <div>
        <Card>
          <CardHdr title="US Store Geography" />
          {/* Metric selector */}
          <div style={{ padding: "12px", borderBottom: "1px solid #e5e7eb", display: "flex", gap: "8px" }}>
            <button
              onClick={() => setSelectedMetric("pos")}
              style={{
                padding: "6px 12px",
                fontSize: "12px",
                fontWeight: selectedMetric === "pos" ? "600" : "400",
                backgroundColor: selectedMetric === "pos" ? CC.blue : "#f3f4f6",
                color: selectedMetric === "pos" ? "#fff" : "#333",
                border: "none",
                borderRadius: "4px",
                cursor: "pointer",
              }}
            >
              Sales $
            </button>
            <button
              onClick={() => setSelectedMetric("qty")}
              style={{
                padding: "6px 12px",
                fontSize: "12px",
                fontWeight: selectedMetric === "qty" ? "600" : "400",
                backgroundColor: selectedMetric === "qty" ? CC.blue : "#f3f4f6",
                color: selectedMetric === "qty" ? "#fff" : "#333",
                border: "none",
                borderRadius: "4px",
                cursor: "pointer",
              }}
            >
              Sales U
            </button>
            <button
              onClick={() => setSelectedMetric("returns")}
              style={{
                padding: "6px 12px",
                fontSize: "12px",
                fontWeight: selectedMetric === "returns" ? "600" : "400",
                backgroundColor: selectedMetric === "returns" ? CC.blue : "#f3f4f6",
                color: selectedMetric === "returns" ? "#fff" : "#333",
                border: "none",
                borderRadius: "4px",
                cursor: "pointer",
              }}
            >
              Return $
            </button>
          </div>

          {/* View mode toggles */}
          <div style={{ padding: "12px", borderBottom: "1px solid #e5e7eb", display: "flex", gap: "8px" }}>
            <label style={{ display: "flex", alignItems: "center", gap: "6px", fontSize: "12px", cursor: "pointer" }}>
              <input type="checkbox" checked={showCities} onChange={(e) => setShowCities(e.target.checked)} style={{ width: "14px", height: "14px" }} />
              <span>Cities</span>
            </label>
            <label style={{ display: "flex", alignItems: "center", gap: "6px", fontSize: "12px", cursor: "pointer" }}>
              <input type="checkbox" checked={showMetro} onChange={(e) => setShowMetro(e.target.checked)} style={{ width: "14px", height: "14px" }} />
              <span>Metro</span>
            </label>
          </div>

          <div style={{ padding: "16px" }}>
            <USMap
              selectedState={selectedState}
              onSelectState={setSelectedState}
              showCities={showCities}
              showMetro={showMetro}
              selectedMetric={selectedMetric}
            />
          </div>
        </Card>
      </div>

      {/* RIGHT: SIDE PANEL */}
      <div style={{ display: "flex", flexDirection: "column", gap: "12px" }}>
        {/* TAB SELECTOR */}
        <div style={{ display: "flex", borderBottom: "2px solid #e5e7eb" }}>
          {["states", "regions", "zero-oh", "one-oh"].map((t) => (
            <button
              key={t}
              onClick={() => setSideTab(t)}
              style={{
                flex: 1,
                padding: "8px 0",
                fontSize: "11px",
                fontWeight: sideTab === t ? "600" : "400",
                color: sideTab === t ? CC.blue : "#666",
                border: "none",
                borderBottom: sideTab === t ? `2px solid ${CC.blue}` : "none",
                backgroundColor: "transparent",
                cursor: "pointer",
              }}
            >
              {t === "states" && "States"}
              {t === "regions" && "Regions"}
              {t === "zero-oh" && "Zero OH"}
              {t === "one-oh" && "One OH"}
            </button>
          ))}
        </div>

        {/* STATES TAB */}
        {sideTab === "states" && (
          <div style={{ overflowY: "auto", maxHeight: "600px" }}>
            <div style={{ padding: "8px", fontSize: "11px", color: "#666" }}>
              <div style={{ marginBottom: "8px" }}>
                <strong>Top State</strong>
                <div style={{ fontSize: "10px", marginTop: "4px" }}>
                  {topState?.name} ({topState?.abbr}): {fmtK(topState?.pos)}
                </div>
              </div>
            </div>
            {statesSorted.slice(0, 15).map((s) => {
              const bw = (s.pos / maxStatePos * 100).toFixed(0);
              const bc = s.risk >= 3 ? CC.red : s.risk >= 1 ? CC.amber : salesColor(s);
              return (
                <div
                  key={s.abbr}
                  onClick={() => setSelectedState(s.abbr === selectedState ? null : s.abbr)}
                  style={{
                    padding: "8px",
                    margin: "4px 0",
                    borderRadius: "4px",
                    backgroundColor: s.abbr === selectedState ? "#eff6ff" : "#f9fafb",
                    cursor: "pointer",
                    borderLeft: `3px solid ${bc}`,
                  }}
                >
                  <div style={{ fontSize: "11px", fontWeight: "600" }}>{s.name}</div>
                  <div style={{ fontSize: "10px", color: "#666" }}>
                    {fmtK(s.pos)} / {s.traited} stores
                  </div>
                  <div style={{ width: "100%", height: "4px", backgroundColor: "#e5e7eb", borderRadius: "2px", marginTop: "4px" }}>
                    <div style={{ width: bw + "%", height: "100%", backgroundColor: bc, borderRadius: "2px" }} />
                  </div>
                </div>
              );
            })}
          </div>
        )}

        {/* REGIONS TAB */}
        {sideTab === "regions" && (
          <div style={{ overflowY: "auto", maxHeight: "600px" }}>
            {regSorted.map((r) => {
              const d = REGIONS[r];
              return (
                <div key={r} style={{ padding: "8px", margin: "4px 0", borderRadius: "4px", backgroundColor: "#f9fafb", borderLeft: `3px solid ${d.color}` }}>
                  <div style={{ fontSize: "11px", fontWeight: "600" }}>{r}</div>
                  <div style={{ fontSize: "10px", color: "#666" }}>{fmtK(d.pos)}</div>
                </div>
              );
            })}
          </div>
        )}

        {/* ZERO OH TAB */}
        {sideTab === "zero-oh" && (
          <div style={{ overflowY: "auto", maxHeight: "600px" }}>
            <div style={{ padding: "8px", fontSize: "10px", color: "#666", marginBottom: "8px" }}>
              <strong>{totalZeroOh}</strong> locations with zero OH
              <br />
              <strong>{totalCritical}</strong> critical risk
            </div>
            {ZERO_OH.map((s) => (
              <div key={s.abbr} style={{ padding: "8px", margin: "4px 0", borderRadius: "4px", backgroundColor: "#fef2f2", borderLeft: "3px solid #dc2626" }}>
                <div style={{ fontSize: "11px", fontWeight: "600" }}>{s.state}</div>
                <div style={{ fontSize: "10px", color: "#666" }}>
                  Zero: {s.zero_oh} | Risk: {s.risk}
                </div>
              </div>
            ))}
          </div>
        )}

        {/* ONE OH TAB */}
        {sideTab === "one-oh" && (
          <div style={{ overflowY: "auto", maxHeight: "600px" }}>
            <div style={{ padding: "8px", fontSize: "10px", color: "#666", marginBottom: "8px" }}>
              <strong>{ONE_OH.length}</strong> locations with one OH
            </div>
            {ONE_OH.map((s) => (
              <div key={s.abbr} style={{ padding: "8px", margin: "4px 0", borderRadius: "4px", backgroundColor: "#fef3c7", borderLeft: "3px solid #f59e0b" }}>
                <div style={{ fontSize: "11px", fontWeight: "600" }}>{s.state}</div>
                <div style={{ fontSize: "10px", color: "#666" }}>
                  One: {s.one_oh}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

// ═════════════════════════════════════════════════════════════════════════════
// REGIONS TAB
// ═════════════════════════════════════════════════════════════════════════════
function RegionsTab() {
  const [activeRegion, setActiveRegion] = useState(null);
  const regNames = Object.keys(REGIONS);
  const regSorted = regNames.sort((a, b) => REGIONS[b].pos - REGIONS[a].pos);

  const filteredStates = activeRegion
    ? statesSorted.filter((s) => STATE_TO_REGION[s.abbr] === activeRegion)
    : statesSorted;

  return (
    <div style={{ display: "grid", gridTemplateColumns: "250px 1fr", gap: "16px" }}>
      {/* REGION LIST */}
      <Card>
        <CardHdr title="Regions" />
        <div style={{ padding: "12px" }}>
          {regSorted.map((r) => {
            const d = REGIONS[r];
            const isActive = activeRegion === r;
            return (
              <div
                key={r}
                onClick={() => setActiveRegion(isActive ? null : r)}
                style={{
                  padding: "10px",
                  margin: "6px 0",
                  borderRadius: "4px",
                  backgroundColor: isActive ? "#eff6ff" : "#f9fafb",
                  borderLeft: `3px solid ${d.color}`,
                  cursor: "pointer",
                }}
              >
                <div style={{ fontSize: "12px", fontWeight: "600" }}>{r}</div>
                <div style={{ fontSize: "10px", color: "#666", marginTop: "2px" }}>
                  {fmtK(d.pos)}
                </div>
              </div>
            );
          })}
        </div>
      </Card>

      {/* STATE DETAIL */}
      <Card>
        <CardHdr title={activeRegion ? `${activeRegion} States` : "All States"} />
        <div style={{ padding: "12px", maxHeight: "700px", overflowY: "auto" }}>
          <table style={{ width: "100%", fontSize: "11px", borderCollapse: "collapse" }}>
            <thead>
              <tr style={{ borderBottom: "1px solid #e5e7eb" }}>
                <th style={{ textAlign: "left", padding: "6px", fontWeight: "600", color: "#666" }}>State</th>
                <th style={{ textAlign: "right", padding: "6px", fontWeight: "600", color: "#666" }}>Sales</th>
                <th style={{ textAlign: "right", padding: "6px", fontWeight: "600", color: "#666" }}>Qty</th>
                <th style={{ textAlign: "right", padding: "6px", fontWeight: "600", color: "#666" }}>Stores</th>
              </tr>
            </thead>
            <tbody>
              {filteredStates.map((s) => {
                const region = STATE_TO_REGION[s.abbr] || "—";
                const regColor = REGIONS[region]?.color || "var(--txt3)";
                return (
                  <tr key={s.abbr} style={{ borderBottom: "1px solid #f3f4f6", backgroundColor: s.risk >= 3 ? "#fef2f2" : "transparent" }}>
                    <td style={{ padding: "6px", fontWeight: "500", borderLeft: `2px solid ${regColor}` }}>
                      {s.abbr}
                    </td>
                    <td style={{ padding: "6px", textAlign: "right" }}>{fmtK(s.pos)}</td>
                    <td style={{ padding: "6px", textAlign: "right" }}>{fN(s.qty)}</td>
                    <td style={{ padding: "6px", textAlign: "right" }}>{s.traited}</td>
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
// STORE DETAIL TAB (from API)
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
        const result = await api.walmartStoreGeography(filters);
        setData(result);
      } catch (e) {
        setError(e.message || "Failed to load data");
      } finally {
        setLoading(false);
      }
    })();
  }, [filters]);

  if (loading) return <div style={{ padding: "20px", textAlign: "center", color: "#999" }}>Loading stores...</div>;
  if (error) return <div style={{ padding: "20px", color: "#dc2626" }}>{error}</div>;

  const allStores = data?.stores || [];
  const totalPosSales = data?.totalPosSales || 0;

  const filtered = allStores.filter((s) => {
    const matchesSearch = !search || s.storeName?.toLowerCase().includes(search.toLowerCase()) || s.storeId?.toString().includes(search);
    const matchesState = !stateFilter || s.state === stateFilter;
    const matchesOh = !ohFilter || (ohFilter === "zero" && s.zeroOh) || (ohFilter === "one" && s.oneOh);
    return matchesSearch && matchesState && matchesOh;
  });

  filtered.sort((a, b) => {
    const av = a[sortBy] ?? 0;
    const bv = b[sortBy] ?? 0;
    return sortDir === "asc" ? av - bv : bv - av;
  });

  const handleSort = (col) => {
    if (sortBy === col) {
      setSortDir(sortDir === "asc" ? "desc" : "asc");
    } else {
      setSortBy(col);
      setSortDir("desc");
    }
  };

  const si = (col) => sortBy === col ? (sortDir === "asc" ? " ▲" : " ▼") : "";

  const uniqueStates = [...new Set(allStores.map((s) => s.state))].sort();

  return (
    <Card>
      <CardHdr title="Store Details" />
      <div style={{ padding: "12px", borderBottom: "1px solid #e5e7eb" }}>
        <div style={{ display: "grid", gridTemplateColumns: "1fr 120px 100px", gap: "8px", marginBottom: "12px" }}>
          <input
            type="text"
            placeholder="Search store name or ID..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            style={{ padding: "6px", fontSize: "11px", border: "1px solid #d1d5db", borderRadius: "4px" }}
          />
          <select
            value={stateFilter}
            onChange={(e) => setStateFilter(e.target.value)}
            style={{ padding: "6px", fontSize: "11px", border: "1px solid #d1d5db", borderRadius: "4px" }}
          >
            <option value="">All States</option>
            {uniqueStates.map((st) => (
              <option key={st} value={st}>{st}</option>
            ))}
          </select>
          <select
            value={ohFilter}
            onChange={(e) => setOhFilter(e.target.value)}
            style={{ padding: "6px", fontSize: "11px", border: "1px solid #d1d5db", borderRadius: "4px" }}
          >
            <option value="">All OH</option>
            <option value="zero">Zero OH</option>
            <option value="one">One OH</option>
          </select>
        </div>
        <div style={{ fontSize: "10px", color: "#666" }}>
          {filtered.length} of {allStores.length} stores | Total: {f$(totalPosSales)}
        </div>
      </div>

      <div style={{ padding: "12px", overflowX: "auto", maxHeight: "600px" }}>
        <table style={{ width: "100%", fontSize: "10px", borderCollapse: "collapse" }}>
          <thead style={{ backgroundColor: "#f9fafb", position: "sticky", top: 0 }}>
            <tr>
              <th
                onClick={() => handleSort("storeId")}
                style={{ padding: "6px", textAlign: "left", cursor: "pointer", fontWeight: "600", color: "#666", borderBottom: "1px solid #e5e7eb" }}
              >
                ID{si("storeId")}
              </th>
              <th
                onClick={() => handleSort("storeName")}
                style={{ padding: "6px", textAlign: "left", cursor: "pointer", fontWeight: "600", color: "#666", borderBottom: "1px solid #e5e7eb" }}
              >
                Name{si("storeName")}
              </th>
              <th
                onClick={() => handleSort("state")}
                style={{ padding: "6px", textAlign: "center", cursor: "pointer", fontWeight: "600", color: "#666", borderBottom: "1px solid #e5e7eb" }}
              >
                State{si("state")}
              </th>
              <th
                onClick={() => handleSort("posSalesTy")}
                style={{ padding: "6px", textAlign: "right", cursor: "pointer", fontWeight: "600", color: "#666", borderBottom: "1px solid #e5e7eb" }}
              >
                Sales TY{si("posSalesTy")}
              </th>
              <th style={{ padding: "6px", textAlign: "center", fontWeight: "600", color: "#666", borderBottom: "1px solid #e5e7eb" }}>
                Status
              </th>
            </tr>
          </thead>
          <tbody>
            {filtered.map((s) => (
              <tr key={s.storeId} style={{ borderBottom: "1px solid #f3f4f6", backgroundColor: s.risk >= 3 ? "#fef2f2" : "transparent" }}>
                <td style={{ padding: "6px" }}>{s.storeId}</td>
                <td style={{ padding: "6px", fontWeight: "500" }}>{s.storeName}</td>
                <td style={{ padding: "6px", textAlign: "center", fontSize: "10px" }}>{s.state}</td>
                <td style={{ padding: "6px", textAlign: "right", fontWeight: "500" }}>{f$(s.posSalesTy)}</td>
                <td style={{ padding: "6px", textAlign: "center" }}>
                  {s.zeroOh && <OhBadge oh={0} />}
                  {s.oneOh && <OhBadge oh={1} />}
                  {s.risk > 0 && <RiskBadge risk={s.risk} />}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Card>
  );
}

// ═════════════════════════════════════════════════════════════════════════════
// RISK TAB
// ═════════════════════════════════════════════════════════════════════════════
function RiskTab() {
  const criticalStores = ZERO_OH.filter((s) => s.risk === 1);
  const highRiskStores = ZERO_OH.filter((s) => s.risk >= 3);

  return (
    <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "16px" }}>
      <Card>
        <CardHdr title="Critical Risk (Risk = 1)" />
        <div style={{ padding: "12px" }}>
          {criticalStores.length === 0 ? (
            <div style={{ fontSize: "11px", color: "#999" }}>No critical risk stores</div>
          ) : (
            criticalStores.map((s) => (
              <div key={s.abbr} style={{ padding: "8px", margin: "4px 0", borderRadius: "4px", backgroundColor: "#fef3c7", borderLeft: "3px solid #f59e0b" }}>
                <div style={{ fontSize: "11px", fontWeight: "600" }}>{s.state}</div>
                <div style={{ fontSize: "10px", color: "#666" }}>Risk: {s.risk} | Zero: {s.zero_oh}</div>
              </div>
            ))
          )}
        </div>
      </Card>

      <Card>
        <CardHdr title="High Risk (Risk >= 3)" />
        <div style={{ padding: "12px" }}>
          {highRiskStores.length === 0 ? (
            <div style={{ fontSize: "11px", color: "#999" }}>No high-risk stores</div>
          ) : (
            highRiskStores.map((s) => (
              <div key={s.abbr} style={{ padding: "8px", margin: "4px 0", borderRadius: "4px", backgroundColor: "#fef2f2", borderLeft: "3px solid #dc2626" }}>
                <div style={{ fontSize: "11px", fontWeight: "600" }}>{s.state}</div>
                <div style={{ fontSize: "10px", color: "#666" }}>Risk: {s.risk} | Zero: {s.zero_oh}</div>
              </div>
            ))
          )}
        </div>
      </Card>
    </div>
  );
}

// ═════════════════════════════════════════════════════════════════════════════
// MAIN PAGE
// ═════════════════════════════════════════════════════════════════════════════
export function WalmartStoreAnalytics() {
  const [activeTab, setActiveTab] = useState("geography");
  const [filters, setFilters] = useState({});

  return (
    <div style={{ padding: "16px", display: "flex", flexDirection: "column", gap: "16px" }}>
      {/* HEADER */}
      <div>
        <h2 style={SG()}>Walmart Store Analytics</h2>
        <div style={{ fontSize: "13px", color: "#666", marginTop: "4px" }}>
          Store-level POS sales, inventory, and risk analysis
        </div>
      </div>

      {/* KPI CARDS */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))", gap: "12px" }}>
        <KPICard
          label="States Active"
          value={Object.values(WM_DATA).filter((s) => s.pos > 0).length}
          unit=""
          color={COLORS.blue}
        />
        <KPICard
          label="Total Sales"
          value={TOTAL_POS}
          unit="USD"
          color={COLORS.green}
        />
        <KPICard
          label="Total Units"
          value={Object.values(WM_DATA).reduce((a, s) => a + s.qty, 0)}
          unit="units"
          color={COLORS.amber}
        />
        <KPICard
          label="Total Returns"
          value={Object.values(WM_DATA).reduce((a, s) => a + (s.returns || 0), 0)}
          unit="USD"
          color={COLORS.red}
        />
      </div>

      {/* TAB SELECTOR */}
      <div style={{ display: "flex", borderBottom: "2px solid #e5e7eb", gap: "0" }}>
        {["geography", "regions", "stores", "risk"].map((t) => (
          <button
            key={t}
            onClick={() => setActiveTab(t)}
            style={{
              padding: "12px 20px",
              fontSize: "13px",
              fontWeight: activeTab === t ? "600" : "400",
              color: activeTab === t ? CC.blue : "#666",
              border: "none",
              borderBottom: activeTab === t ? `3px solid ${CC.blue}` : "none",
              backgroundColor: "transparent",
              cursor: "pointer",
            }}
          >
            {t === "geography" && "Geography"}
            {t === "regions" && "Regions"}
            {t === "stores" && "Store Details"}
            {t === "risk" && "Risk"}
          </button>
        ))}
      </div>

      {/* TAB CONTENT */}
      {activeTab === "geography" && <GeographyMapTab />}
      {activeTab === "regions" && <RegionsTab />}
      {activeTab === "stores" && <StoreDetailTab filters={filters} />}
      {activeTab === "risk" && <RiskTab />}
    </div>
  );
}

export default WalmartStoreAnalytics;
