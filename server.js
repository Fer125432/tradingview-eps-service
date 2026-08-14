import express from "express";
import WebSocket from "ws";
import fs from "fs";
import { initializeApp, cert } from "firebase-admin/app";
import { getFirestore } from "firebase-admin/firestore";
import { getMessaging } from "firebase-admin/messaging";
const app = express();
const PORT = Number(process.env.PORT || 10000);
const API_KEY = process.env.API_KEY || "";
const FIREBASE_SERVICE_ACCOUNT_PATH =
  "/etc/secrets/firebase-service-account.json";

let db = null;

try {
  const serviceAccount = JSON.parse(
    fs.readFileSync(FIREBASE_SERVICE_ACCOUNT_PATH, "utf8")
  );

  initializeApp({
    credential: cert(serviceAccount),
  });

  db = getFirestore();

  console.log("Firebase Admin conectado correctamente");
} catch (error) {
  console.error(
    "No se pudo inicializar Firebase Admin:",
    error instanceof Error ? error.message : String(error)
  );
}

app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader(
    "Access-Control-Allow-Methods",
    "GET, OPTIONS"
  );
  res.setHeader(
    "Access-Control-Allow-Headers",
    "Content-Type, Authorization, x-api-key"
  );

  if (req.method === "OPTIONS") {
    return res.sendStatus(204);
  }

  next();
});

app.use(express.json());

function tvFrame(method, params) {
  const payload = JSON.stringify({ m: method, p: params });
  return `~m~${Buffer.byteLength(payload, "utf8")}~m~${payload}`;
}

function parseFrames(buffer) {
  const text = Buffer.isBuffer(buffer) ? buffer.toString("utf8") : String(buffer);
  const frames = [];
  let cursor = 0;

  while (cursor < text.length) {
    const markerStart = text.indexOf("~m~", cursor);
    if (markerStart === -1) break;

    const lengthEnd = text.indexOf("~m~", markerStart + 3);
    if (lengthEnd === -1) break;

    const lengthText = text.slice(markerStart + 3, lengthEnd);
    const length = Number(lengthText);
    if (!Number.isInteger(length) || length < 0) {
      cursor = lengthEnd + 3;
      continue;
    }

    const payloadStart = lengthEnd + 3;
    const payloadEnd = payloadStart + length;
    if (payloadEnd > text.length) break;

    frames.push(text.slice(payloadStart, payloadEnd));
    cursor = payloadEnd;
  }

  return frames;
}

function numberOrNull(value) {
  if (value === null || value === undefined || value === "") return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function normalizeEpsEstimates(raw) {
  if (!Array.isArray(raw)) return [];

  return raw
    .map((item) => {
      if (!item || typeof item !== "object") return null;

      const year = Number(item.FiscalPeriod);
      if (!Number.isInteger(year)) return null;

      const estimate =
        item.Estimate &&
        typeof item.Estimate === "object" &&
        !Array.isArray(item.Estimate)
          ? item.Estimate
          : null;

      const eps = estimate
        ? numberOrNull(estimate.average)
        : numberOrNull(item.Estimate);

      if (eps === null) return null;

      return {
        year,
        eps,
        median: estimate ? numberOrNull(estimate.median) : null,
        high: estimate ? numberOrNull(estimate.high) : null,
        low: estimate ? numberOrNull(estimate.low) : null,
        analysts: estimate ? numberOrNull(estimate.est_num) : null,
        actual: numberOrNull(item.Actual),
        isReported: item.IsReported === true,
        estimateDate:
          estimate && Number.isFinite(Number(estimate.date))
            ? new Date(Number(estimate.date) * 1000).toISOString()
            : null,
      };
    })
    .filter(Boolean)
    .sort((a, b) => a.year - b.year);
}

function normalizeFinancialEstimates(raw, valueKey) {
  if (!Array.isArray(raw)) return [];

  return raw
    .map((item) => {
      if (!item || typeof item !== "object") return null;

      const year = Number(item.FiscalPeriod);
      if (!Number.isInteger(year)) return null;

      const estimate =
        item.Estimate &&
        typeof item.Estimate === "object" &&
        !Array.isArray(item.Estimate)
          ? item.Estimate
          : null;

      const value = estimate
        ? numberOrNull(estimate.average)
        : numberOrNull(item.Estimate);

      if (value === null) return null;

      return {
        year,
        [valueKey]: value,
        median: estimate ? numberOrNull(estimate.median) : null,
        high: estimate ? numberOrNull(estimate.high) : null,
        low: estimate ? numberOrNull(estimate.low) : null,
        analysts: estimate ? numberOrNull(estimate.est_num) : null,
        actual: numberOrNull(item.Actual),
        isReported: item.IsReported === true,
        estimateDate:
          estimate && Number.isFinite(Number(estimate.date))
            ? new Date(Number(estimate.date) * 1000).toISOString()
            : null,
      };
    })
    .filter(Boolean)
    .sort((a, b) => a.year - b.year);
}

function validSymbol(symbol) {
  return /^[A-Z0-9._-]{1,20}:[A-Z0-9.-]{1,20}$/.test(symbol);
}

function randomSession() {
  return `qs_${crypto.randomUUID().replaceAll("-", "").slice(0, 20)}`;
}

function buildSocketUrl(symbol) {
  const [exchange, ticker] = symbol.split(":");
  const pagePath = `symbols/${exchange}-${ticker}/forecast-price-target/`;

  const params = new URLSearchParams({
    from: pagePath,
    date: new Date().toISOString(),
    auth: "sessionid",
  });

  return `wss://data.tradingview.com/socket.io/websocket?${params}`;
}

function getAdjustedSymbol(symbol) {
  const [exchange, ticker] = symbol.split(":");
  const secondary = ["NASDAQ", "NYSE", "AMEX", "OTC"].includes(exchange)
    ? `BATS:${ticker}`
    : symbol;

  return `={"adjustment":"splits","symbol":"${secondary}"}`;
}

function candidateSymbols(input) {
  const value = input.trim().toUpperCase();

  if (value.includes(":")) {
    return [value];
  }

  return [
    `NASDAQ:${value}`,
    `NYSE:${value}`,
    `AMEX:${value}`,
    `OTC:${value}`,
    `BME:${value}`,
  ];
}

function getTradingViewEps(symbol, timeoutMs = 20000) {
  return new Promise((resolve, reject) => {
    const session = randomSession();
    const adjustedSymbol = getAdjustedSymbol(symbol);
    const socketUrl = buildSocketUrl(symbol);

    const ws = new WebSocket(socketUrl, {
      origin: "https://www.tradingview.com",
      headers: {
        "User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) " +
          "AppleWebKit/537.36 (KHTML, like Gecko) " +
          "Chrome/124.0.0.0 Safari/537.36",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
      },
      handshakeTimeout: 10000,
    });

    let finished = false;
    let accumulated = {};

    const timer = setTimeout(() => {
      finishReject(new Error("Tiempo agotado esperando eps_estimates_fy_h"));
    }, timeoutMs);

    function cleanup() {
      clearTimeout(timer);
      try {
        if (
          ws.readyState === WebSocket.OPEN ||
          ws.readyState === WebSocket.CONNECTING
        ) {
          ws.close(1000, "finished");
        }
      } catch {}
    }

    function finishResolve(value) {
      if (finished) return;
      finished = true;
      cleanup();
      resolve(value);
    }

    function finishReject(error) {
      if (finished) return;
      finished = true;
      cleanup();
      reject(error);
    }

    ws.on("open", () => {
      ws.send(tvFrame("set_auth_token", ["unauthorized_user_token"]));
      ws.send(tvFrame("set_locale", ["es", "ES"]));
      ws.send(tvFrame("quote_create_session", [session]));
      ws.send(tvFrame("quote_add_symbols", [session, symbol]));
      ws.send(tvFrame("quote_fast_symbols", [session, symbol]));
      ws.send(tvFrame("quote_add_symbols", [session, adjustedSymbol]));
      ws.send(
        tvFrame("quote_fast_symbols", [
          session,
          symbol,
          adjustedSymbol,
        ])
      );
    });

    ws.on("message", (data) => {
      for (const frame of parseFrames(data)) {
        if (frame.startsWith("~h~")) {
          if (ws.readyState === WebSocket.OPEN) {
            ws.send(`~m~${Buffer.byteLength(frame, "utf8")}~m~${frame}`);
          }
          continue;
        }

        let message;
        try {
          message = JSON.parse(frame);
        } catch {
          continue;
        }

        if (
          message?.m !== "qsd" ||
          !Array.isArray(message.p) ||
          typeof message.p[1]?.v !== "object"
        ) {
          continue;
        }

        const values = message.p[1].v;
        accumulated = { ...accumulated, ...values };

const interestingKeys = Object.keys(accumulated)
  .filter((key) => {
    const k = key.toLowerCase();
return k.endsWith("_fq_h");
  })
  .sort();

        if (!Array.isArray(accumulated.eps_estimates_fy_h)) {
          continue;
        }

        const annualEstimates = normalizeEpsEstimates(
          accumulated.eps_estimates_fy_h
        );
const nextFiscalYearForecast = numberOrNull(
  accumulated.earnings_per_share_forecast_next_fy
);

const futureEstimates = annualEstimates
  .filter((item) => item.isReported === false)
  .map((item, index) => {
    if (index === 0 && nextFiscalYearForecast !== null) {
      return {
        ...item,
        eps: nextFiscalYearForecast,
      };
    }

    return item;
  });
        
const revenueEstimates = normalizeFinancialEstimates(
  accumulated.revenue_estimates_fy_h,
  "revenue"
);

const netIncomeEstimates = normalizeFinancialEstimates(
  accumulated.net_income_estimates_fy_h,
  "netIncome"
);
const quarterlyEps = Array.isArray(
  accumulated.earnings_per_share_fq_h
)
  ? accumulated.earnings_per_share_fq_h
      .map(numberOrNull)
      .filter((value) => value !== null)
  : [];

const epsTtmNonGaap =
  quarterlyEps.length >= 4
    ? quarterlyEps
        .slice(0, 4)
        .reduce((sum, value) => sum + value, 0)
    : null;

        function buildAnnualHistory(accumulated) {
  const yearsRaw = Array.isArray(accumulated.fiscal_period_fy_h)
    ? accumulated.fiscal_period_fy_h
    : [];

  const years = yearsRaw
    .map((value) => Number(value))
    .filter((value) => Number.isInteger(value));

  const readSeries = (key) => {
    const values = Array.isArray(accumulated[key])
      ? accumulated[key]
      : [];

    return years
      .map((year, index) => ({
        year,
        value: numberOrNull(values[index]),
      }))
      .filter((item) => item.value !== null);
  };

  return {
    revenue: readSeries("total_revenue_fy_h"),
    cogs: readSeries("cost_of_goods_fy_h"),
    grossProfit: readSeries("gross_profit_fy_h"),
    netIncome: readSeries("net_income_fy_h"),
    epsDiluted: readSeries("earnings_per_share_diluted_fy_h"),
    sharesDiluted: readSeries("diluted_shares_outstanding_fy_h"),

    totalAssets: readSeries("total_assets_fy_h"),
    totalLiabilities: readSeries("total_liabilities_fy_h"),
    equity: readSeries("total_equity_fy_h"),

    cash: readSeries("cash_n_short_term_invest_fy_h"),
    longTermDebt: readSeries("long_term_debt_fy_h"),
    shortTermDebt: readSeries("short_term_debt_fy_h"),
    totalDebt: readSeries("total_debt_fy_h"),
    netDebt: readSeries("net_debt_fy_h"),

    operatingCashFlow: readSeries(
      "cash_f_operating_activities_fy_h"
    ),
    capex: readSeries("capital_expenditures_fy_h"),
    freeCashFlow: readSeries("free_cash_flow_fy_h"),

    grossMargin: readSeries("gross_margin_fy_h"),
    netMargin: readSeries("net_margin_fy_h"),
  };
}

const historical = buildAnnualHistory(accumulated);

        function buildQuarterlyHistory(accumulated) {
  const periodsRaw = Array.isArray(accumulated.fiscal_period_fq_h)
    ? accumulated.fiscal_period_fq_h
    : [];

  const endsRaw = Array.isArray(accumulated.fiscal_period_end_fq_h)
    ? accumulated.fiscal_period_end_fq_h
    : [];

  const readValues = (key) =>
    Array.isArray(accumulated[key])
      ? accumulated[key]
      : [];

  const revenueValues =
    readValues("total_revenue_fq_h").length > 0
      ? readValues("total_revenue_fq_h")
      : readValues("revenue_fq_h");

  const epsValues =
    readValues("earnings_per_share_diluted_fq_h").length > 0
      ? readValues("earnings_per_share_diluted_fq_h")
      : readValues("earnings_per_share_fq_h");
          const netIncomeValues =
  readValues("net_income_fq_h");

  function parsePeriod(value, endValue, index) {
    const text = String(value ?? "").trim().toUpperCase();

    // Formatos posibles: 2026Q2, Q2 2026, Q2
    let match = text.match(/(\d{4}).*Q([1-4])/);

    if (match) {
      return {
        year: Number(match[1]),
        quarter: Number(match[2]),
      };
    }

    match = text.match(/Q([1-4]).*(\d{4})/);

    if (match) {
      return {
        year: Number(match[2]),
        quarter: Number(match[1]),
      };
    }

    // Si solo viene Q1/Q2/Q3/Q4, usamos la fecha de cierre.
    const quarterMatch = text.match(/Q([1-4])/);
    const endDate =
      typeof endValue === "number"
        ? new Date(endValue * 1000)
        : new Date(String(endValue ?? ""));

    if (
      quarterMatch &&
      !Number.isNaN(endDate.getTime())
    ) {
      return {
        year: endDate.getUTCFullYear(),
        quarter: Number(quarterMatch[1]),
      };
    }

    // Último respaldo: deducir trimestre por la fecha de cierre.
    if (!Number.isNaN(endDate.getTime())) {
      return {
        year: endDate.getUTCFullYear(),
        quarter: Math.floor(endDate.getUTCMonth() / 3) + 1,
      };
    }

    return null;
  }

  function buildSeries(values) {
    return periodsRaw
      .map((period, index) => {
        const parsed = parsePeriod(
          period,
          endsRaw[index],
          index,
        );

        const value = numberOrNull(values[index]);

        if (
          parsed === null ||
          value === null
        ) {
          return null;
        }

        return {
          year: parsed.year,
          quarter: parsed.quarter,
          value,
        };
      })
      .filter(Boolean)
      .sort((a, b) => {
        if (a.year != b.year) {
          return b.year - a.year;
        }

        return b.quarter - a.quarter;
      });
  }

 return {
  revenue: buildSeries(revenueValues),
  epsDiluted: buildSeries(epsValues),
  netIncome: buildSeries(netIncomeValues),
};
}

const quarterlyHistorical =
  buildQuarterlyHistory(accumulated);
        
       finishResolve({
  debugFields: interestingKeys,
         historical,
          quarterlyHistorical,
          symbol,
          ticker:
            accumulated.short_name ||
            accumulated.name ||
            symbol.split(":")[1],
          company:
            accumulated.description ||
            accumulated.local_description ||
            accumulated.short_description ||
            null,
          currency:
            accumulated.currency_code ||
            accumulated.fundamental_currency_code ||
            null,
          price: numberOrNull(
            accumulated.lp ??
              accumulated.regular_close ??
              accumulated.close
          ),
        eps: {
quarterlyActuals: quarterlyEps,
epsTtmNonGaap: epsTtmNonGaap,

  dilutedTtm: numberOrNull(
      accumulated.earnings_per_share_diluted_ttm
  ),

  lastQuarterActual: numberOrNull(
      accumulated.earnings_per_share_fq
  ),

  lastAnnualActual: numberOrNull(
      accumulated.last_annual_eps
  ),

  nextQuarterForecast: numberOrNull(
      accumulated.earnings_per_share_forecast_next_fq
  ),

  nextFiscalYearForecast: numberOrNull(
      accumulated.earnings_per_share_forecast_next_fy
  ),

  annualEstimates,

  futureEstimates,
},

financials: {
  revenueEstimates,
  futureRevenueEstimates: revenueEstimates.filter(
    (item) => item.isReported === false
  ),
  netIncomeEstimates,
  futureNetIncomeEstimates: netIncomeEstimates.filter(
    (item) => item.isReported === false
  ),
},

source: "TradingView WebSocket",
fetchedAt: new Date().toISOString(),
        });
      }
    });

    ws.on("error", (error) => finishReject(error));

    ws.on("unexpected-response", (_request, response) => {
      finishReject(
        new Error(`TradingView rechazó el WebSocket: HTTP ${response.statusCode}`)
      );
    });

    ws.on("close", (code, reason) => {
      if (!finished) {
        finishReject(
          new Error(
            `TradingView cerró la conexión: ${code} ${reason.toString()}`
          )
        );
      }
    });
  });
}

async function getTradingViewFromTicker(input) {
  const candidates = candidateSymbols(input);
  const errors = [];

  for (const symbol of candidates) {
    try {
      const result = await getTradingViewEps(symbol, 8000);

      const hasHistorical =
        Array.isArray(result?.historical?.revenue) &&
        result.historical.revenue.length > 0;

      if (hasHistorical) {
        return {
          ...result,
          requestedSymbol: input,
          resolvedSymbol: symbol,
        };
      }
    } catch (error) {
      errors.push(
        `${symbol}: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  throw new Error(
    `No se encontró ${input}. Intentos: ${errors.join(" | ")}`
  );
}
// ============================================================
// SEC EARNINGS DETECTOR
// ============================================================

const SEC_HEADERS = {
  "User-Agent": "MiFinanzasApp admin@example.com",
  "Accept-Encoding": "gzip, deflate",
  "Host": "data.sec.gov",
};
const SEC_ARCHIVE_HEADERS = {
  "User-Agent": "MiFinanzasApp admin@example.com",
  "Accept-Encoding": "gzip, deflate",
};

// Ticker -> CIK
// Primero probamos con las empresas que ya hemos verificado.
const SEC_COMPANIES = {
  AMD: 2488,
  AMZN: 1018724,
  HIMS: 1773751,
  DLO: 1846832,
  TSM: 1046179,
};

function padCik(cik) {
  return String(cik).padStart(10, "0");
}

async function getSecSubmissions(ticker) {
  const upperTicker = ticker.toUpperCase();
  const cik = SEC_COMPANIES[upperTicker];

  if (!cik) {
    throw new Error(`Ticker ${upperTicker} todavía no configurado`);
  }

  const url =
    `https://data.sec.gov/submissions/CIK${padCik(cik)}.json`;

  const response = await fetch(url, {
    headers: SEC_HEADERS,
  });

  if (!response.ok) {
    throw new Error(
      `SEC respondió HTTP ${response.status}`
    );
  }

  return response.json();
}

async function getLatestEarningsFiling(ticker, submissions) {
  const recent = submissions?.filings?.recent;

  if (!recent) {
    throw new Error("SEC no devolvió filings recientes");
  }

  const upperTicker = ticker.toUpperCase();

  const forms = recent.form || [];
  const accessionNumbers = recent.accessionNumber || [];
  const filingDates = recent.filingDate || [];
  const primaryDocuments = recent.primaryDocument || [];
  const items = recent.items || [];

  const foreignIssuer = ["DLO", "TSM"].includes(upperTicker);

  for (let i = 0; i < forms.length; i++) {
    const form = forms[i];
    const filingItems = String(items[i] || "");

    const accession = accessionNumbers[i];
    const primaryDocument = primaryDocuments[i];

    if (!accession || !primaryDocument) {
      continue;
    }

    const accessionNoDashes = accession.replaceAll("-", "");
    const cik = SEC_COMPANIES[upperTicker];

    const secUrl =
      `https://www.sec.gov/Archives/edgar/data/` +
      `${cik}/${accessionNoDashes}/${primaryDocument}`;

    // EMPRESAS USA
    if (
      !foreignIssuer &&
      form === "8-K" &&
      filingItems
        .split(",")
        .map((item) => item.trim())
        .includes("2.02")
    ) {
      return {
        ticker: upperTicker,
        filing: form,
        items: filingItems,
        filedAt: filingDates[i],
        accessionNumber: accession,
        primaryDocument,
        secUrl,
        earningsDetected: true,
        detection: "8-K Item 2.02",
      };
    }

     // FOREIGN PRIVATE ISSUERS
    if (foreignIssuer && form === "6-K") {
      try {
        const filingIndexUrl =
          `https://www.sec.gov/Archives/edgar/data/` +
          `${cik}/${accessionNoDashes}/${accession}-index.html`;

        const indexResponse = await fetch(filingIndexUrl, {
          headers: SEC_ARCHIVE_HEADERS,
        });

        if (!indexResponse.ok) {
          continue;
        }

        const indexHtml = await indexResponse.text();

        const documentRegex =
          /href="([^"]+\.(?:htm|html))"/gi;

        const documents = [];
        let match;

        while ((match = documentRegex.exec(indexHtml)) !== null) {
          const documentName = match[1].split("/").pop();

          if (
            documentName &&
            !documents.includes(documentName)
          ) {
            documents.push(documentName);
          }
        }

        for (const documentName of documents) {
          const documentUrl =
            `https://www.sec.gov/Archives/edgar/data/` +
            `${cik}/${accessionNoDashes}/${documentName}`;

          const documentResponse = await fetch(documentUrl, {
            headers: SEC_ARCHIVE_HEADERS,
          });

          if (!documentResponse.ok) {
            continue;
          }

          const html = await documentResponse.text();

          const text = html
            .replace(/<script[\s\S]*?<\/script>/gi, " ")
            .replace(/<style[\s\S]*?<\/style>/gi, " ")
            .replace(/<[^>]+>/g, " ")
            .replace(/&nbsp;/gi, " ")
            .replace(/&amp;/gi, "&")
            .replace(/\s+/g, " ")
            .toLowerCase();

          const earningsPatterns = [
            /quarterly financial results/,
            /quarter financial results/,
            /reports first quarter/,
            /reports second quarter/,
            /reports third quarter/,
            /reports fourth quarter/,
            /first quarter.*financial results/,
            /second quarter.*financial results/,
            /third quarter.*financial results/,
            /fourth quarter.*financial results/,
            /earnings release/,
            /results of operations.*quarter/,
          ];

          const matchesEarnings = earningsPatterns.some(
            (pattern) => pattern.test(text)
          );

          if (!matchesEarnings) {
            continue;
          }

          return {
            ticker: upperTicker,
            filing: form,
            items: filingItems,
            filedAt: filingDates[i],
            accessionNumber: accession,
            primaryDocument,
            earningsDocument: documentName,
            secUrl: documentUrl,
            earningsDetected: true,
            detection: "6-K earnings exhibit",
          };
        }
      } catch (error) {
        console.error(
         `Error revisando 6-K ${upperTicker} ${accession}:`,
          error
        );

              continue;
      }
    }
  }  // <-- AÑADE ESTA

  return {
    ticker: upperTicker,
    earningsDetected: false,
  };
}
async function getLatestSecEarnings(ticker) {
  const submissions = await getSecSubmissions(ticker);

  return await getLatestEarningsFiling(
    ticker,
    submissions
  );
}

function requireApiKey(req, res, next) {
  if (!API_KEY) return next();

  const supplied =
    req.get("x-api-key") ||
    (req.get("authorization") || "").replace(/^Bearer\s+/i, "");

  if (supplied !== API_KEY) {
    return res.status(401).json({ error: "No autorizado" });
  }

  next();
}

app.get("/", (_req, res) => {
  res.json({
    status: "ok",
    service: "TradingView EPS Service",
    example: "/eps?symbol=NASDAQ:NVDA",
  });
});

app.get("/health", (_req, res) => {
  res.json({ status: "ok" });
});

app.get("/eps", requireApiKey, async (req, res) => {
  const symbol = String(req.query.symbol || "").trim().toUpperCase();

if (!/^[A-Z0-9._:-]{1,40}$/.test(symbol)) {
    return res.status(400).json({
      error: "Símbolo ausente o inválido",
      examples: [
        "/eps?symbol=NASDAQ:NVDA",
        "/eps?symbol=NASDAQ:META",
        "/eps?symbol=NYSE:V",
        "/eps?symbol=BME:SAN",
      ],
    });
  }

  try {
    const result = await getTradingViewFromTicker(symbol);
    res.set("Cache-Control", "public, max-age=1800");
    return res.json(result);
  } catch (error) {
    return res.status(502).json({
      error: "No se pudieron obtener las estimaciones de TradingView",
      symbol,
      detail: error instanceof Error ? error.message : String(error),
    });
  }
});
app.get("/earnings", requireApiKey, async (req, res) => {
  const ticker = String(req.query.symbol || "")
    .trim()
    .toUpperCase();

  if (!/^[A-Z0-9.-]{1,20}$/.test(ticker)) {
    return res.status(400).json({
      error: "Ticker ausente o inválido",
      example: "/earnings?symbol=AMD",
    });
  }

  try {
    const result = await getLatestSecEarnings(ticker);

    res.set("Cache-Control", "no-store");

    return res.json(result);
  } catch (error) {
    return res.status(502).json({
      error: "No se pudo consultar SEC",
      ticker,
      detail:
        error instanceof Error
          ? error.message
          : String(error),
    });
  }
});
app.get("/sync-earnings", requireApiKey, async (req, res) => {
  const ticker = String(req.query.symbol || "")
    .trim()
    .toUpperCase();

  if (!/^[A-Z0-9.-]{1,20}$/.test(ticker)) {
    return res.status(400).json({
      error: "Ticker ausente o inválido",
      example: "/sync-earnings?symbol=AMD",
    });
  }

  if (!db) {
    return res.status(500).json({
      error: "Firebase Admin no inicializado",
    });
  }

  try {
    const latest = await getLatestSecEarnings(ticker);

    if (!latest?.earningsDetected) {
      return res.json({
        ticker,
        earningsDetected: false,
        updated: false,
      });
    }

    const docRef = db
      .collection("earnings_filings")
      .doc(ticker);

    const snapshot = await docRef.get();

    const previous = snapshot.exists
      ? snapshot.data()
      : null;

    const previousAccession =
      previous?.accessionNumber || null;

    const currentAccession =
      latest.accessionNumber || null;

    const isNew =
      previousAccession !== currentAccession;

    if (!isNew) {
      return res.json({
        ticker,
        earningsDetected: true,
        updated: false,
        newEarnings: false,
        accessionNumber: currentAccession,
      });
    }

    await docRef.set({
      accessionNumber: currentAccession,
      filedAt: latest.filedAt || null,
      filing: String(latest.filing || "").trim(),
      secUrl: latest.secUrl || null,
      earningsDocument: latest.earningsDocument || null,
      primaryDocument: latest.primaryDocument || null,
      detection: latest.detection || null,
      updatedAt: new Date().toISOString(),
    });

    await db
      .collection("earnings_alerts")
      .add({
        ticker,
        accessionNumber: currentAccession,
        filedAt: latest.filedAt || null,
        filing: String(latest.filing || "").trim(),
        secUrl: latest.secUrl || null,
        createdAt: new Date().toISOString(),
        sent: false,
      });

    return res.json({
      ticker,
      earningsDetected: true,
      updated: true,
      newEarnings: true,
      previousAccession,
      currentAccession,
      secUrl: latest.secUrl || null,
    });
  } catch (error) {
    return res.status(500).json({
      error: "No se pudo sincronizar earnings",
      ticker,
      detail:
        error instanceof Error
          ? error.message
          : String(error),
    });
  }
});
app.get("/sync-all-earnings", requireApiKey, async (_req, res) => {
  if (!db) {
    return res.status(500).json({
      error: "Firebase Admin no inicializado",
    });
  }

  const tickers = Object.keys(SEC_COMPANIES);
  const results = [];

  for (const ticker of tickers) {
    try {
      const latest = await getLatestSecEarnings(ticker);

      if (!latest?.earningsDetected) {
        results.push({
          ticker,
          earningsDetected: false,
          updated: false,
        });

        continue;
      }

      const docRef = db
        .collection("earnings_filings")
        .doc(ticker);

      const snapshot = await docRef.get();

      const previous = snapshot.exists
        ? snapshot.data()
        : null;

      const previousAccession =
        previous?.accessionNumber || null;

      const currentAccession =
        latest.accessionNumber || null;

      if (previousAccession === currentAccession) {
        results.push({
          ticker,
          earningsDetected: true,
          updated: false,
          newEarnings: false,
          accessionNumber: currentAccession,
        });

        continue;
      }

      await docRef.set({
        accessionNumber: currentAccession,
        filedAt: latest.filedAt || null,
        filing: String(latest.filing || "").trim(),
        secUrl: latest.secUrl || null,
        earningsDocument: latest.earningsDocument || null,
        primaryDocument: latest.primaryDocument || null,
        detection: latest.detection || null,
        updatedAt: new Date().toISOString(),
      });

      await db
        .collection("earnings_alerts")
        .add({
          ticker,
          accessionNumber: currentAccession,
          filedAt: latest.filedAt || null,
          filing: String(latest.filing || "").trim(),
          secUrl: latest.secUrl || null,
          createdAt: new Date().toISOString(),
          sent: false,
        });

      results.push({
        ticker,
        earningsDetected: true,
        updated: true,
        newEarnings: true,
        previousAccession,
        currentAccession,
      });
    } catch (error) {
      results.push({
        ticker,
        error:
          error instanceof Error
            ? error.message
            : String(error),
      });
    }
  }

  return res.json({
    ok: true,
    checked: tickers.length,
    newEarnings: results.filter(
      (item) => item.newEarnings === true
    ).length,
    results,
  });
});

app.listen(PORT, "0.0.0.0", () => {
  console.log(`TradingView EPS service escuchando en puerto ${PORT}`);
});
