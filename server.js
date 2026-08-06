import express from "express";
import WebSocket from "ws";
const app = express();
const PORT = Number(process.env.PORT || 10000);
const API_KEY = process.env.API_KEY || "";

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
        
       finishResolve({
  debugFields: interestingKeys,
         historical,
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

app.listen(PORT, "0.0.0.0", () => {
  console.log(`TradingView EPS service escuchando en puerto ${PORT}`);
});
