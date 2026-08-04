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

    return (
      k.includes("eps") ||
      k.includes("earnings_per_share")
    );
  })
  .sort();

console.log("CAMPOS EPS TRADINGVIEW:");
console.log(interestingKeys);

        if (!Array.isArray(accumulated.eps_estimates_fy_h)) {
          continue;
        }

        const annualEstimates = normalizeEpsEstimates(
          accumulated.eps_estimates_fy_h
        );
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
        
       finishResolve({
  debugFields: interestingKeys,
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

  futureEstimates: annualEstimates.filter(
      (item) => item.isReported === false
  ),
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

  if (!validSymbol(symbol)) {
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
    const result = await getTradingViewEps(symbol);
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
