const express  = require('express');
const http     = require('http');
const path     = require('path');
const { Server } = require('socket.io');
const WebSocket  = require('ws');
const crypto     = require('crypto');
const Database   = require('better-sqlite3');
const { connectMongo, restoreToSQLite, syncFromSQLite, syncDoc, deleteDoc } = require('./mongo');

const app    = express();
const server = http.createServer(app);
const io     = new Server(server);

const PORT              = process.env.PORT || 3000;
const BYBIT_WS_URL      = 'wss://stream.bybit.com/v5/public/linear';
const PING_INTERVAL_MS  = 20000;
const RECONNECT_INITIAL = 1000;
const RECONNECT_MAX     = 30000;

// ─── Bybit Credentials ────────────────────────────────────
// Set BYBIT_API_KEY and BYBIT_API_SECRET as environment variables
// (Northflank dashboard → Service → Environment)
const API_KEY     = process.env.BYBIT_API_KEY;
const API_SECRET  = process.env.BYBIT_API_SECRET;
const RECV_WINDOW = 5000;

if (!API_KEY || !API_SECRET) {
    console.error('[Config] BYBIT_API_KEY and BYBIT_API_SECRET env vars are required.');
    process.exit(1);
}

// ─── Strategy Constants ───────────────────────────────────
const HEDGE_MARGIN_USDT = 5;        // 5 USDT margin per side
const HEDGE_LEVERAGE    = 5;        // 5× leverage → 25 USDT notional per side
const TRAIL_INITIAL_PCT = 0.005;    // 0.5% initial trailing distance
const TRAIL_TIGHT_PCT   = 0.0035;   // 0.35% trail distance on runners
const CIRCUIT_BREAKER   = 0.03;     // close both if combined loss >= 3% of notional
const SIGNAL_THRESHOLD  = 5;        // signals needed out of 7 to fire pump/dump (strict momentum filter)

app.use(express.static(path.join(__dirname, 'public')));

// ─── SQLite DB ────────────────────────────────────────────
// DB_PATH env var lets Northflank point this at the persistent volume mount.
// Falls back to a local file when running locally.
const DB_PATH = process.env.DB_PATH || path.join(__dirname, 'grid.db');
const db = new Database(DB_PATH);
console.log(`[DB] Using database at: ${DB_PATH}`);

// CREATE TABLE IF NOT EXISTS — safe to run on every start, never drops data.
db.exec(`
    CREATE TABLE IF NOT EXISTS active_trades (
        symbol     TEXT PRIMARY KEY,
        status     TEXT,
        created_at INTEGER
    );
    CREATE TABLE IF NOT EXISTS manual_legs (
        symbol        TEXT PRIMARY KEY,
        leg_direction TEXT,
        open_price    REAL,
        size          REAL,
        hedged        INTEGER,
        tp_order_id   TEXT
    );
    CREATE TABLE IF NOT EXISTS pump_dump_trades (
        symbol          TEXT PRIMARY KEY,
        long_entry      REAL,
        short_entry     REAL,
        qty             REAL,
        trade_qty       REAL DEFAULT 0,
        long_sl         REAL,
        short_sl        REAL,
        status          TEXT,
        trail_direction TEXT,
        trail_distance  REAL,
        trail_peak      REAL,
        trail_sl        REAL,
        cut_pnl         REAL DEFAULT 0,
        trail_pnl       REAL DEFAULT 0,
        total_pnl       REAL DEFAULT 0,
        close_price     REAL,
        created_at      INTEGER
    );
`);

// ─── DB Helpers ───────────────────────────────────────────
function dbAddActiveTrade(symbol) {
    db.transaction(() => {
        db.prepare('DELETE FROM active_trades WHERE symbol = ?').run(symbol);
        db.prepare('DELETE FROM manual_legs WHERE symbol = ?').run(symbol);
        db.prepare('INSERT INTO active_trades (symbol, status, created_at) VALUES (?, ?, ?)')
            .run(symbol, 'active', Date.now());
    })();
    // Persist to MongoDB
    deleteDoc('manual_legs', symbol);
    syncFromSQLite(db, 'active_trades', symbol);
}

function emitLegUpdate(symbol) {
    const active = db.prepare('SELECT * FROM active_trades WHERE symbol = ?').get(symbol);
    const leg    = db.prepare('SELECT * FROM manual_legs WHERE symbol = ?').get(symbol);
    io.emit('legUpdate', { symbol, active: !!active, leg });
}

function emitPdTradeUpdate(symbol) {
    const trade = db.prepare('SELECT * FROM pump_dump_trades WHERE symbol = ?').get(symbol);
    io.emit('pdTradeUpdate', { symbol, trade: trade || null });
}

// ─── Time Sync ────────────────────────────────────────────
let serverTimeOffset = 0;
async function syncServerTime() {
    try {
        const res  = await fetch('https://api.bybit.com/v5/market/time');
        const data = await res.json();
        serverTimeOffset = Math.floor(parseInt(data.result.timeNano) / 1_000_000 - Date.now());
        console.log(`[Time] Synced. Offset: ${serverTimeOffset}ms`);
    } catch (err) {
        console.error('[Time] Sync failed:', err.message);
    }
}

// ─── Signed REST Request ──────────────────────────────────
function generateSignature(timestamp, payload) {
    return crypto.createHmac('sha256', API_SECRET)
        .update(timestamp + API_KEY + RECV_WINDOW + payload)
        .digest('hex');
}

async function apiRequest(endpointPath, params = {}, method = 'GET') {
    const timestamp = (Date.now() + serverTimeOffset).toString();
    let url = `https://api.bybit.com${endpointPath}`;
    let body = '';
    const headers = {
        'X-BAPI-API-KEY':     API_KEY,
        'X-BAPI-TIMESTAMP':   timestamp,
        'X-BAPI-RECV-WINDOW': RECV_WINDOW.toString(),
    };

    if (method === 'GET') {
        const qs = Object.entries(params).sort(([a], [b]) => a.localeCompare(b))
            .map(([k, v]) => `${k}=${v}`).join('&');
        if (qs) url += `?${qs}`;
        headers['X-BAPI-SIGN'] = generateSignature(timestamp, qs);
    } else {
        body = JSON.stringify(params);
        headers['X-BAPI-SIGN']  = generateSignature(timestamp, body);
        headers['Content-Type'] = 'application/json';
    }

    const res = await fetch(url, { method, headers, ...(method !== 'GET' && { body }) });
    return res.json();
}

// ─── Instrument Info Cache ────────────────────────────────
let cachedInstrumentInfo = {};
async function fetchAllInstrumentInfo() {
    try {
        let cursor = '', all = [];
        do {
            const url  = `https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000${cursor ? '&cursor=' + cursor : ''}`;
            const data = await (await fetch(url)).json();
            if (data.retCode === 0 && data.result?.list) {
                all    = all.concat(data.result.list);
                cursor = data.result.nextPageCursor;
            } else { break; }
        } while (cursor);

        all.forEach(item => {
            if (item.symbol.endsWith('USDT')) cachedInstrumentInfo[item.symbol] = item;
        });
        console.log(`[Init] Cached instrument info for ${Object.keys(cachedInstrumentInfo).length} pairs.`);
    } catch (err) {
        console.error('[Init] Error:', err.message);
    }
}

// ─── Calculation Helpers ──────────────────────────────────
function getDecimals(stepStr) {
    return (stepStr.split('.')[1] || '').length;
}

function getDecimalPlaces(number) {
    const s = number.toString();
    if (s.includes('e')) {
        const exp = parseInt(s.split('e')[1]);
        const dec = (s.split('e')[0].split('.')[1]?.length || 0) - exp;
        return dec > 0 ? dec : 0;
    }
    return s.split('.')[1]?.length || 0;
}

function validatePrice(raw) {
    const n = parseFloat(raw);
    return (isFinite(n) && !isNaN(n) && n > 0) ? n : null;
}

/** qty for HEDGE_MARGIN_USDT at HEDGE_LEVERAGE — e.g. 5 USDT × 5× = 25 USDT notional */
function getPositionQtyForMargin(price, info) {
    if (!info || price <= 0) return 0;
    const notional = HEDGE_MARGIN_USDT * HEDGE_LEVERAGE;   // 25 USDT
    const minQty   = parseFloat(info.lotSizeFilter.minOrderQty);
    const qtyStep  = parseFloat(info.lotSizeFilter.qtyStep);
    const decimals = getDecimals(info.lotSizeFilter.qtyStep);
    let qty = notional / price;
    qty = Math.ceil(qty / qtyStep) * qtyStep;
    if (qty < minQty) qty = minQty;
    return parseFloat(qty.toFixed(decimals));
}

function buildPayload(symbol, price) {
    const info = cachedInstrumentInfo[symbol];
    return {
        symbol,
        price: price.toString(),
        info: info ? {
            status:      info.status,
            maxLeverage: info.leverageFilter.maxLeverage,
            tickSize:    info.priceFilter.tickSize,
            minOrderQty: info.lotSizeFilter.minOrderQty,
            baseCoin:    info.baseCoin
        } : null
    };
}

// ─── Positions Helper ─────────────────────────────────────
async function getPositions(symbol) {
    try {
        const data = await apiRequest('/v5/position/list', { category: 'linear', symbol });
        if (data.retCode === 0 && data.result?.list) {
            return data.result.list
                .filter(p => parseFloat(p.size) > 0)
                .map(p => ({
                    symbol:        p.symbol,
                    side:          p.side,
                    size:          p.size,
                    avgPrice:      p.avgPrice,
                    positionIdx:   p.positionIdx,
                    unrealisedPnl: p.unrealisedPnl
                }));
        }
    } catch (err) {
        console.error('[Positions] Error:', err.message);
    }
    return [];
}

async function closeAllPositionsMarket(symbol) {
    const positions = await getPositions(symbol);
    if (!positions.length) return;
    const orders = positions.map(pos => ({
        symbol:      pos.symbol,
        side:        pos.side === 'Buy' ? 'Sell' : 'Buy',
        orderType:   'Market',
        qty:         pos.size,
        timeInForce: 'IOC',
        positionIdx: parseInt(pos.positionIdx),
        reduceOnly:  true,
        orderLinkId: `close-mkt-${Date.now()}-${pos.symbol}-${pos.side}`
    }));
    return apiRequest('/v5/order/create-batch', { category: 'linear', request: orders }, 'POST');
}

// ─── SignalScorer ─────────────────────────────────────────
class SignalScorer {
    constructor(symbol) {
        this.symbol       = symbol;
        this.priceHistory = [];   // {time, price}
        this.tradeHistory = [];   // {time, buyVol, sellVol}
        this.oiHistory    = [];   // {time, oi}
        this.obBidTotal   = 0;
        this.obAskTotal   = 0;
        this.fundingRate  = 0;
        this.shortLiqVol  = 0;   // short positions liquidated = bullish signal
        this.longLiqVol   = 0;   // long  positions liquidated = bearish signal
        this.lastLiqClear = Date.now();
        this.klineBody    = 0;   // abs body pct of current 1m candle
        this.klineDir     = 0;   // +1 bullish, -1 bearish
        // BB breakout state — tracks whether price is persistently outside a band
        // direction: 'up' | 'down' | null
        // startTime: when the breakout was first detected (ms)
        // initDist:  how far outside the band price was at first detection
        this.bbState     = { direction: null, startTime: 0, initDist: 0 };
    }

    getVolatility() {
        if (this.priceHistory.length < 5) return 0.005; // default 0.5%
        const samples  = this.priceHistory.slice(-30).map(p => p.price);
        const n        = samples.length;
        const sma      = samples.reduce((a, v) => a + v, 0) / n;
        const variance = samples.reduce((a, v) => a + (v - sma) ** 2, 0) / n;
        const stdDev   = Math.sqrt(variance);
        return sma > 0 ? (stdDev / sma) : 0.005;
    }

    updatePrice(price) {
        const now = Date.now();
        this.priceHistory.push({ time: now, price });
        const cutoff = now - 120_000;  // keep 2 min so BB always has 20 samples
        this.priceHistory = this.priceHistory.filter(p => p.time >= cutoff);
    }

    updateTrade(buyVol, sellVol) {
        const now = Date.now();
        this.tradeHistory.push({ time: now, buyVol, sellVol });
        this.tradeHistory = this.tradeHistory.filter(t => now - t.time < 120_000);
    }

    updateOrderbook(bidTotal, askTotal) {
        this.obBidTotal = bidTotal;
        this.obAskTotal = askTotal;
    }

    updateTicker(oi, fundingRate) {
        const parsedOI = parseFloat(oi || 0);
        if (parsedOI > 0) {
            const now = Date.now();
            this.oiHistory.push({ time: now, oi: parsedOI });
            this.oiHistory = this.oiHistory.filter(o => now - o.time < 120_000);
        }
        if (fundingRate !== undefined && fundingRate !== null && fundingRate !== '') {
            this.fundingRate = parseFloat(fundingRate || 0);
        }
    }

    updateLiquidation(side, qty) {
        // 'Buy' = short position liquidated (bullish), 'Sell' = long position liquidated (bearish)
        const now = Date.now();
        if (now - this.lastLiqClear > 15_000) {
            this.shortLiqVol  = 0;
            this.longLiqVol   = 0;
            this.lastLiqClear = now;
        }
        if (side === 'Buy') this.shortLiqVol += parseFloat(qty || 0);
        else                this.longLiqVol  += parseFloat(qty || 0);
    }

    updateKline(open, close) {
        const body = (close - open) / open;
        this.klineBody = Math.abs(body);
        this.klineDir  = body >= 0 ? 1 : -1;
    }

    compute() {
        const now  = Date.now();
        let pump = 0, dump = 0;
        const details = [];

        // ── Signal 1: Price velocity (30s) ────────────────
        const old30 = [...this.priceHistory].reverse().find(p => now - p.time >= 29_000);
        const cur   = this.priceHistory[this.priceHistory.length - 1];
        if (old30 && cur) {
            const vel = (cur.price - old30.price) / old30.price * 100;
            if (vel >  0.5) { pump++; details.push({ s: 'Velocity', v: `+${vel.toFixed(2)}%`, d: 'pump' }); }
            if (vel < -0.5) { dump++; details.push({ s: 'Velocity', v: `${vel.toFixed(2)}%`,  d: 'dump' }); }
        }

        // ── Signal 2: Volume spike + direction (30s) ──────
        const recent = this.tradeHistory.filter(t => now - t.time < 30_000);
        const base   = this.tradeHistory.filter(t => now - t.time >= 30_000 && now - t.time < 120_000);
        const recentVol  = recent.reduce((s, t) => s + t.buyVol + t.sellVol, 0);
        const baseTotal  = base.reduce((s, t) => s + t.buyVol + t.sellVol, 0);
        const baseAvg30s = base.length > 0 ? baseTotal * (30_000 / 90_000) : 0; // scale to 30s

        if (baseAvg30s > 0 && recentVol > baseAvg30s * 2) {
            const bv = recent.reduce((s, t) => s + t.buyVol,  0);
            const sv = recent.reduce((s, t) => s + t.sellVol, 0);
            const tv = bv + sv;
            if (tv > 0) {
                if (bv / tv > 0.60) { pump++; details.push({ s: 'Volume', v: `${(bv/tv*100).toFixed(0)}% buy`,  d: 'pump' }); }
                if (sv / tv > 0.60) { dump++; details.push({ s: 'Volume', v: `${(sv/tv*100).toFixed(0)}% sell`, d: 'dump' }); }
            }
        }

        // ── Signal 3: Orderbook imbalance (2.0x Wall Dominance) ──
        const obTotal = this.obBidTotal + this.obAskTotal;
        if (obTotal > 0) {
            const bidRatio = this.obBidTotal / obTotal;
            if (bidRatio >= 0.67) { pump++; details.push({ s: 'OB Wall', v: `${(bidRatio*100).toFixed(0)}% bid`, d: 'pump' }); }
            if (bidRatio <= 0.33) { dump++; details.push({ s: 'OB Wall', v: `${((1-bidRatio)*100).toFixed(0)}% ask`, d: 'dump' }); }
        }

        // ── Signal 4: Open Interest trend (60s) ───────────
        const oiNow = this.oiHistory[this.oiHistory.length - 1]?.oi;
        const oi60s = [...this.oiHistory].reverse().find(o => now - o.time >= 55_000);
        if (oiNow && oi60s && oi60s.oi > 0) {
            const oiChg = (oiNow - oi60s.oi) / oi60s.oi;
            if (oiChg >  0.001) { pump++; details.push({ s: 'OI', v: `+${(oiChg*100).toFixed(2)}%`, d: 'pump' }); }
            if (oiChg < -0.001) { dump++; details.push({ s: 'OI', v: `${(oiChg*100).toFixed(2)}%`,  d: 'dump' }); }
        }

        // ── Signal 5: Liquidations (last 15s) ─────────────
        if (this.shortLiqVol > 0) { pump++; details.push({ s: 'Liq', v: `${this.shortLiqVol.toFixed(2)} short`,  d: 'pump' }); }
        if (this.longLiqVol  > 0) { dump++; details.push({ s: 'Liq', v: `${this.longLiqVol.toFixed(2)} long`,   d: 'dump' }); }

        // ── Signal 6: Kline candle momentum ───────────────
        if (this.klineBody > 0.003) {   // > 0.3% body
            if (this.klineDir > 0) { pump++; details.push({ s: 'Candle', v: `+${(this.klineBody*100).toFixed(2)}%`, d: 'pump' }); }
            else                   { dump++; details.push({ s: 'Candle', v: `-${(this.klineBody*100).toFixed(2)}%`, d: 'dump' }); }
        }

        // ── Signal 7: Bollinger Band confirmed breakout (20 samples, 2σ) ──
        //
        // Two-phase filter to avoid fakeouts:
        //  Phase 1 — Detection: price first crosses outside the band → timestamp it.
        //  Phase 2 — Confirmation: price must stay outside for ≥ BB_CONFIRM_MS (4s)
        //            AND must not have significantly retraced back toward the band
        //            (remaining distance ≥ 30% of the initial breakout distance).
        //  Reset: if price re-enters the band at any point, state clears immediately.
        const BB_CONFIRM_MS     = 4_000;   // 4 seconds outside band required
        const BB_RETRACE_FLOOR  = 0.30;    // must keep ≥ 30% of initial distance

        if (this.priceHistory.length >= 10) {
            const samples  = this.priceHistory.slice(-20).map(p => p.price);
            const n        = samples.length;
            const sma      = samples.reduce((a, v) => a + v, 0) / n;
            const variance = samples.reduce((a, v) => a + (v - sma) ** 2, 0) / n;
            const stdDev   = Math.sqrt(variance);
            const upper    = sma + 2 * stdDev;
            const lower    = sma - 2 * stdDev;
            const latest   = samples[n - 1];
            const bandPct  = stdDev > 0 ? ((2 * stdDev) / sma * 100).toFixed(3) : '0.000';

            if (latest > upper) {
                // Price is above upper band
                const dist = latest - upper;
                if (this.bbState.direction !== 'up') {
                    // Phase 1: new breakout — record the moment and initial distance
                    this.bbState = { direction: 'up', startTime: now, initDist: dist };
                } else {
                    // Phase 2: already in an 'up' breakout — check persistence & continuation
                    const elapsed   = now - this.bbState.startTime;
                    const notFaded  = dist >= this.bbState.initDist * BB_RETRACE_FLOOR;
                    if (elapsed >= BB_CONFIRM_MS && notFaded) {
                        const secs = (elapsed / 1000).toFixed(0);
                        pump++;
                        details.push({ s: 'BB✓', v: `>${bandPct}% · ${secs}s`, d: 'pump' });
                    }
                    // update initDist to follow the furthest point (ratchet outward)
                    if (dist > this.bbState.initDist) this.bbState.initDist = dist;
                }
            } else if (latest < lower) {
                // Price is below lower band
                const dist = lower - latest;
                if (this.bbState.direction !== 'down') {
                    // Phase 1: new breakout
                    this.bbState = { direction: 'down', startTime: now, initDist: dist };
                } else {
                    // Phase 2: persistence & continuation check
                    const elapsed   = now - this.bbState.startTime;
                    const notFaded  = dist >= this.bbState.initDist * BB_RETRACE_FLOOR;
                    if (elapsed >= BB_CONFIRM_MS && notFaded) {
                        const secs = (elapsed / 1000).toFixed(0);
                        dump++;
                        details.push({ s: 'BB✓', v: `<${bandPct}% · ${secs}s`, d: 'dump' });
                    }
                    if (dist > this.bbState.initDist) this.bbState.initDist = dist;
                }
            } else {
                // Price returned inside the bands — fakeout, reset state
                if (this.bbState.direction !== null) {
                    this.bbState = { direction: null, startTime: 0, initDist: 0 };
                }
            }
        }

        return { pump, dump, details, maxSignals: 7 };
    }
}

const signalScorers = {};
function getOrCreateScorer(symbol) {
    if (!signalScorers[symbol]) signalScorers[symbol] = new SignalScorer(symbol);
    return signalScorers[symbol];
}

// Score every 2s and optionally fire pump/dump action
setInterval(() => {
    if (!wsSymbol) return;
    const scorer = signalScorers[wsSymbol];
    if (!scorer) return;
    const { pump, dump, details, maxSignals } = scorer.compute();
    io.emit('signalUpdate', { symbol: wsSymbol, pump, dump, details, maxSignals });
    if (lastValidPrice !== null) {
        handleSignalScore(wsSymbol, pump, dump, lastValidPrice).catch(err =>
            console.error('[Signal] Error in handleSignalScore:', err.message)
        );
    }
}, 2000);

// ─── Orderbook State Manager ──────────────────────────────
const obState = {};   // symbol → { bids: Map<price, qty>, asks: Map<price, qty> }

function updateObState(symbol, type, bids, asks) {
    if (!obState[symbol]) obState[symbol] = { bids: new Map(), asks: new Map() };
    const ob = obState[symbol];
    if (type === 'snapshot') {
        ob.bids = new Map(bids.map(([p, q]) => [p, parseFloat(q)]));
        ob.asks = new Map(asks.map(([p, q]) => [p, parseFloat(q)]));
    } else {
        bids.forEach(([p, q]) => { const qty = parseFloat(q); qty === 0 ? ob.bids.delete(p) : ob.bids.set(p, qty); });
        asks.forEach(([p, q]) => { const qty = parseFloat(q); qty === 0 ? ob.asks.delete(p) : ob.asks.set(p, qty); });
    }
    const bidTotal = [...ob.bids.values()].reduce((s, q) => s + q, 0);
    const askTotal = [...ob.asks.values()].reduce((s, q) => s + q, 0);
    getOrCreateScorer(symbol).updateOrderbook(bidTotal, askTotal);
}

function getBestBidAsk(symbol) {
    const ob = obState[symbol];
    if (ob && ob.bids.size > 0 && ob.asks.size > 0) {
        const bidPrices = [...ob.bids.keys()].map(Number).sort((a, b) => b - a);
        const askPrices = [...ob.asks.keys()].map(Number).sort((a, b) => a - b);
        if (bidPrices.length && askPrices.length) {
            return { bestBid: bidPrices[0], bestAsk: askPrices[0] };
        }
    }
    return { bestBid: lastValidPrice, bestAsk: lastValidPrice };
}

// ─── Pump/Dump Trade Functions ─────────────────────────────

async function openHedgePositions(symbol, price, info) {
    const qty = getPositionQtyForMargin(price, info);
    console.log(`[PD] Opening ${HEDGE_MARGIN_USDT} USDT margin hedge (${HEDGE_LEVERAGE}× = ${HEDGE_MARGIN_USDT * HEDGE_LEVERAGE} USDT notional) — qty: ${qty} ${info.baseCoin} @ ~${price}`);

    await apiRequest('/v5/position/switch-mode', { category: 'linear', symbol, mode: 3 }, 'POST').catch(() => {});
    await apiRequest('/v5/position/set-leverage', {
        category: 'linear', symbol,
        buyLeverage:  HEDGE_LEVERAGE.toString(),
        sellLeverage: HEDGE_LEVERAGE.toString()
    }, 'POST').catch(() => {});

    const longRes  = await apiRequest('/v5/order/create', {
        category: 'linear', symbol, side: 'Buy', orderType: 'Market',
        qty: qty.toString(), timeInForce: 'GTC', positionIdx: 1,
        orderLinkId: `pd-long-${Date.now()}-${symbol}`
    }, 'POST');
    console.log('[PD] Long open:', JSON.stringify(longRes));

    const shortRes = await apiRequest('/v5/order/create', {
        category: 'linear', symbol, side: 'Sell', orderType: 'Market',
        qty: qty.toString(), timeInForce: 'GTC', positionIdx: 2,
        orderLinkId: `pd-short-${Date.now()}-${symbol}`
    }, 'POST');
    console.log('[PD] Short open:', JSON.stringify(shortRes));

    return qty;
}


let isEmergencyClosing = false;
async function emergencyClose(symbol) {
    if (isEmergencyClosing) return;
    isEmergencyClosing = true;
    console.log(`[Circuit Breaker] Emergency closing all positions for ${symbol}`);
    try {
        db.prepare('UPDATE pump_dump_trades SET status=? WHERE symbol=?').run('closed', symbol);
        db.prepare('DELETE FROM active_trades WHERE symbol=?').run(symbol);
        emitPdTradeUpdate(symbol);
        emitLegUpdate(symbol);
        // Persist to MongoDB
        syncFromSQLite(db, 'pump_dump_trades', symbol);
        deleteDoc('active_trades', symbol);
        await apiRequest('/v5/order/cancel-all', { category: 'linear', symbol }, 'POST').catch(() => {});
        await closeAllPositionsMarket(symbol);
        io.emit('signalEvent', { symbol, type: 'CLOSE', message: `⚡ Circuit breaker fired! Combined loss >= ${CIRCUIT_BREAKER * 100}% of notional.`, ts: Date.now() });
    } finally {
        setTimeout(() => { isEmergencyClosing = false; }, 5000);
    }
}

async function checkCircuitBreaker(symbol, positions) {
    const trade = db.prepare('SELECT * FROM pump_dump_trades WHERE symbol = ?').get(symbol);
    if (!trade || trade.status !== 'hedged') return;

    const longPos  = positions.find(p => p.side === 'Buy');
    const shortPos = positions.find(p => p.side === 'Sell');
    if (!longPos || !shortPos) return;

    const combinedPnl   = parseFloat(longPos.unrealisedPnl || 0) + parseFloat(shortPos.unrealisedPnl || 0);
    const totalNotional = trade.qty * (trade.long_entry + trade.short_entry);
    const threshold     = -(totalNotional * CIRCUIT_BREAKER);

    if (combinedPnl <= threshold) {
        console.log(`[Circuit Breaker] ${symbol}: Combined PnL ${combinedPnl.toFixed(4)} <= ${threshold.toFixed(4)} USDT`);
        await emergencyClose(symbol);
    }
}

const symbolLocks  = {}; // Per-symbol lock to prevent overlapping trade actions
const signalTimers = {}; // { [symbol]: { pumpStart: 0, dumpStart: 0 } }
const SIGNAL_PERSIST_MS = 2500; // Must hold >= 5 for 2.5 seconds to confirm genuine breakout (filters 1s wick traps)

async function handleSignalScore(symbol, pumpScore, dumpScore, currentPrice) {
    if (symbolLocks[symbol]) return;
    const trade = db.prepare('SELECT * FROM pump_dump_trades WHERE symbol = ?').get(symbol);
    if (!trade || trade.status !== 'hedged') return;

    const now = Date.now();
    if (!signalTimers[symbol]) signalTimers[symbol] = { pumpStart: 0, dumpStart: 0 };
    const st = signalTimers[symbol];

    // ── Check PUMP signal persistence ─────────────────────
    if (pumpScore >= SIGNAL_THRESHOLD) {
        if (!st.pumpStart) st.pumpStart = now;
        const elapsed = now - st.pumpStart;
        if (elapsed >= SIGNAL_PERSIST_MS) {
            st.pumpStart = 0; // reset
            st.dumpStart = 0;
            symbolLocks[symbol] = true;
            console.log(`\x1b[32m[PD] 🚀 PUMP CONFIRMED (${(elapsed/1000).toFixed(1)}s hold)! ${symbol} score: ${pumpScore}/${SIGNAL_THRESHOLD}\x1b[0m`);
            try {
                await onPumpDetected(symbol, currentPrice, trade);
            } catch (err) {
                console.error('[PD] onPumpDetected error:', err.message);
                symbolLocks[symbol] = false;
            }
        }
    } else {
        st.pumpStart = 0; // reset if dropped below threshold (fakeout filtered!)
    }

    // ── Check DUMP signal persistence ─────────────────────
    if (dumpScore >= SIGNAL_THRESHOLD) {
        if (!st.dumpStart) st.dumpStart = now;
        const elapsed = now - st.dumpStart;
        if (elapsed >= SIGNAL_PERSIST_MS) {
            st.pumpStart = 0;
            st.dumpStart = 0; // reset
            symbolLocks[symbol] = true;
            console.log(`\x1b[31m[PD] 📉 DUMP CONFIRMED (${(elapsed/1000).toFixed(1)}s hold)! ${symbol} score: ${dumpScore}/${SIGNAL_THRESHOLD}\x1b[0m`);
            try {
                await onDumpDetected(symbol, currentPrice, trade);
            } catch (err) {
                console.error('[PD] onDumpDetected error:', err.message);
                symbolLocks[symbol] = false;
            }
        }
    } else {
        st.dumpStart = 0; // reset if dropped below threshold (fakeout filtered!)
    }
}

// ─── Statistical Dynamic Parameter Calculator ───────────
function calculateDynamicParams(symbol, currentPrice, direction, trade) {
    const info = cachedInstrumentInfo[symbol];
    const scorer = signalScorers[symbol];
    const liveVol = scorer ? scorer.getVolatility() : 0.005;

    // 1. Check recent historical trades for symbol
    let avgHistoricalPeakPct = 0;
    try {
        const pastTrades = db.prepare(`
            SELECT trail_peak, long_entry, short_entry, trail_direction 
            FROM pump_dump_trades 
            WHERE symbol = ? AND status = 'closed' AND trail_peak IS NOT NULL 
            ORDER BY created_at DESC LIMIT 15
        `).all(symbol);

        if (pastTrades.length > 0) {
            const peakPcts = pastTrades.map(t => {
                if (t.trail_direction === 'long' && t.long_entry > 0) {
                    return Math.abs(t.trail_peak - t.long_entry) / t.long_entry;
                } else if (t.trail_direction === 'short' && t.short_entry > 0) {
                    return Math.abs(t.short_entry - t.trail_peak) / t.short_entry;
                }
                return 0;
            }).filter(p => p > 0);

            if (peakPcts.length > 0) {
                avgHistoricalPeakPct = peakPcts.reduce((a, b) => a + b, 0) / peakPcts.length;
            }
        }
    } catch (e) {
        console.error('[Stats] Error querying trade history:', e.message);
    }

    // 2. Compute dynamic initial trail distance
    // We want trail distance >= 1.8x live standard deviation (so noise doesn't stop us out)
    // and adapted to ~50% of historical average expansion if available.
    const volNoiseFloor = liveVol * 1.8;
    const histTarget    = avgHistoricalPeakPct > 0 ? avgHistoricalPeakPct * 0.5 : 0.006;
    let initialTrailPct = Math.max(volNoiseFloor, histTarget, TRAIL_INITIAL_PCT);
    
    // Clamp between 0.4% and 2.5%
    initialTrailPct = Math.min(Math.max(initialTrailPct, 0.004), 0.025);

    // Tightened trail is ~60% of initial trail
    const tightTrailPct = Math.max(initialTrailPct * 0.6, TRAIL_TIGHT_PCT);

    // 3. Trade Sizing:
    // Uses exchange minimum order quantity
    const minQty   = parseFloat(info.lotSizeFilter.minOrderQty);
    const qtyDec   = getDecimals(info.lotSizeFilter.qtyStep);
    const tradeQty = parseFloat(minQty.toFixed(qtyDec));

    return {
        initialTrailPct,
        tightTrailPct,
        tradeQty,
        liveVolPct: (liveVol * 100).toFixed(2),
        histAvgPct: (avgHistoricalPeakPct * 100).toFixed(2)
    };
}

async function onPumpDetected(symbol, currentPrice, trade) {
    const info     = cachedInstrumentInfo[symbol];
    const tickSize = parseFloat(info.priceFilter.tickSize);
    const dec      = getDecimalPlaces(tickSize);

    const dyn = calculateDynamicParams(symbol, currentPrice, 'long', trade);
    const tradeQty = dyn.tradeQty;
    // Cap initial stop loss leash at -0.20% max (Pure Maker floor)
    const initialTrail = 0.0020;

    // Get top of orderbook for zero-friction Maker Cut
    const { bestBid } = getBestBidAsk(symbol);
    const limitCutPrice = parseFloat((Math.floor((bestBid || currentPrice) / tickSize) * tickSize).toFixed(dec));

    // Estimate cut PnL on the slice
    const cutPnl = (trade.short_entry - limitCutPrice) * tradeQty;

    // Initial dynamic trail SL price (-0.20% from cut price)
    const trailSLPrice = parseFloat((Math.floor(limitCutPrice * (1 - initialTrail) / tickSize) * tickSize).toFixed(dec));

    // For this trailing cycle, record long_entry as limitCutPrice (the cut price baseline)
    db.prepare(`UPDATE pump_dump_trades SET status=?, trail_direction=?, trail_peak=?, trail_distance=?, trail_sl=?, cut_pnl=?, trade_qty=?, long_entry=?, created_at=? WHERE symbol=?`)
        .run('trailing_long', 'long', limitCutPrice, initialTrail, trailSLPrice, cutPnl, tradeQty, limitCutPrice, Date.now(), symbol);
    emitPdTradeUpdate(symbol);
    syncFromSQLite(db, 'pump_dump_trades', symbol);

    // 1. Close tradeQty of short via Post-Only Maker Limit (0% fee) with IOC fallback
    let closeRes = await apiRequest('/v5/order/create', {
        category: 'linear', symbol, side: 'Buy', orderType: 'Limit',
        price: limitCutPrice.toFixed(dec), qty: tradeQty.toString(),
        timeInForce: 'PostOnly', positionIdx: 2, reduceOnly: true,
        orderLinkId: `pd-cut-short-${Date.now()}-${symbol}`
    }, 'POST').catch(() => null);

    if (!closeRes || closeRes.retCode !== 0) {
        // Fallback to Market IOC if Post-Only crosses spread during rapid movement
        closeRes = await apiRequest('/v5/order/create', {
            category: 'linear', symbol, side: 'Buy', orderType: 'Market',
            qty: tradeQty.toString(), timeInForce: 'IOC', positionIdx: 2,
            reduceOnly: true, orderLinkId: `pd-cut-short-${Date.now()}-${symbol}`
        }, 'POST').catch(err => { console.error('[PD] Cut short fallback failed:', err.message); return null; });
    }

    console.log(`[PD] Maker Cut ${tradeQty} short @ ${limitCutPrice} (0% fee):`, JSON.stringify(closeRes));

    // 2. Immediately place Maker Limit Take-Profit at +0.50%
    const tpTargetPrice = limitCutPrice * 1.0050;
    placeLimitTPOrder(symbol, 'Sell', tpTargetPrice, tradeQty, 1).then(id => {
        if (id) activeLimitTPs[symbol] = id;
    });

    io.emit('signalEvent', {
        symbol, type: 'PUMP',
        message: `🚀 PUMP! Maker Cut ${tradeQty} short @ ${limitCutPrice.toFixed(4)} (0% Fee). Maker TP @ ${(tpTargetPrice).toFixed(4)} (+0.50%) | SL: ${trailSLPrice} (-0.20%)`,
        ts: Date.now()
    });
    console.log(`[PD] Trailing long (qty=${tradeQty}). Maker Cut Baseline: ${limitCutPrice}, TP: ${tpTargetPrice.toFixed(4)}, SL: ${trailSLPrice}`);
}

async function onDumpDetected(symbol, currentPrice, trade) {
    const info     = cachedInstrumentInfo[symbol];
    const tickSize = parseFloat(info.priceFilter.tickSize);
    const dec      = getDecimalPlaces(tickSize);

    const dyn = calculateDynamicParams(symbol, currentPrice, 'short', trade);
    const tradeQty = dyn.tradeQty;
    // Cap initial stop loss leash at -0.20% max (Pure Maker floor)
    const initialTrail = 0.0020;

    // Get top of orderbook for zero-friction Maker Cut
    const { bestAsk } = getBestBidAsk(symbol);
    const limitCutPrice = parseFloat((Math.ceil((bestAsk || currentPrice) / tickSize) * tickSize).toFixed(dec));

    // Estimate cut PnL on the slice
    const cutPnl = (currentPrice - trade.long_entry) * tradeQty;

    // Initial dynamic trail SL price (-0.20% ceiling from cut price)
    const trailSLPrice = parseFloat((Math.ceil(limitCutPrice * (1 + initialTrail) / tickSize) * tickSize).toFixed(dec));

    // For this trailing cycle, record short_entry as limitCutPrice (the cut price baseline)
    db.prepare(`UPDATE pump_dump_trades SET status=?, trail_direction=?, trail_peak=?, trail_distance=?, trail_sl=?, cut_pnl=?, trade_qty=?, short_entry=?, created_at=? WHERE symbol=?`)
        .run('trailing_short', 'short', limitCutPrice, initialTrail, trailSLPrice, cutPnl, tradeQty, limitCutPrice, Date.now(), symbol);
    emitPdTradeUpdate(symbol);
    syncFromSQLite(db, 'pump_dump_trades', symbol);

    // 1. Close tradeQty of long via Post-Only Maker Limit (0% fee) with IOC fallback
    let closeRes = await apiRequest('/v5/order/create', {
        category: 'linear', symbol, side: 'Sell', orderType: 'Limit',
        price: limitCutPrice.toFixed(dec), qty: tradeQty.toString(),
        timeInForce: 'PostOnly', positionIdx: 1, reduceOnly: true,
        orderLinkId: `pd-cut-long-${Date.now()}-${symbol}`
    }, 'POST').catch(() => null);

    if (!closeRes || closeRes.retCode !== 0) {
        // Fallback to Market IOC if Post-Only crosses spread during rapid movement
        closeRes = await apiRequest('/v5/order/create', {
            category: 'linear', symbol, side: 'Sell', orderType: 'Market',
            qty: tradeQty.toString(), timeInForce: 'IOC', positionIdx: 1,
            reduceOnly: true, orderLinkId: `pd-cut-long-${Date.now()}-${symbol}`
        }, 'POST').catch(err => { console.error('[PD] Cut long fallback failed:', err.message); return null; });
    }

    console.log(`[PD] Maker Cut ${tradeQty} long @ ${limitCutPrice} (0% fee):`, JSON.stringify(closeRes));

    // 2. Immediately place Maker Limit Take-Profit at +0.50%
    const tpTargetPrice = limitCutPrice * 0.9950;
    placeLimitTPOrder(symbol, 'Buy', tpTargetPrice, tradeQty, 2).then(id => {
        if (id) activeLimitTPs[symbol] = id;
    });

    io.emit('signalEvent', {
        symbol, type: 'DUMP',
        message: `📉 DUMP! Maker Cut ${tradeQty} long @ ${limitCutPrice.toFixed(4)} (0% Fee). Maker TP @ ${(tpTargetPrice).toFixed(4)} (+0.50%) | SL: ${trailSLPrice} (-0.20%)`,
        ts: Date.now()
    });
    console.log(`[PD] Trailing short (qty=${tradeQty}). Maker Cut Baseline: ${limitCutPrice}, TP: ${tpTargetPrice.toFixed(4)}, SL: ${trailSLPrice}`);
}

// Trailing stop manager — called on every price tick
// Trailing stop manager — called on every price tick
// Server-managed: detects SL breach and fires reduce-only minQty market order.
const lastTrailUpdate   = {};
const trailClosingFlags = {};  // prevents duplicate close orders per symbol
const activeLimitTPs    = {};  // symbol → orderId for Stage 2 Maker Limit TP

async function placeLimitTPOrder(symbol, side, price, qty, positionIdx) {
    try {
        const info = cachedInstrumentInfo[symbol];
        if (!info) return null;
        const tickSize = parseFloat(info.priceFilter.tickSize);
        const dec = getDecimalPlaces(tickSize);
        const roundedPrice = (Math.round(price / tickSize) * tickSize).toFixed(dec);

        const res = await apiRequest('/v5/order/create', {
            category: 'linear',
            symbol,
            side,
            orderType: 'Limit',
            price: roundedPrice,
            qty: qty.toString(),
            timeInForce: 'PostOnly',
            positionIdx,
            reduceOnly: true,
            orderLinkId: `pd-tp-${Date.now()}-${symbol}`
        }, 'POST');

        if (res.retCode === 0 && res.result?.orderId) {
            console.log(`[TP] Placed Post-Only Maker Limit TP for ${symbol} @ ${roundedPrice}: orderId=${res.result.orderId}`);
            return res.result.orderId;
        }
    } catch (err) {
        console.error(`[TP] Failed to place Limit TP for ${symbol}:`, err.message);
    }
    return null;
}

async function cancelActiveLimitTP(symbol) {
    if (activeLimitTPs[symbol]) {
        const orderId = activeLimitTPs[symbol];
        delete activeLimitTPs[symbol];
        await apiRequest('/v5/order/cancel', { category: 'linear', symbol, orderId }, 'POST').catch(() => {});
    }
}

async function updateTrailingStop(symbol, currentPrice) {
    const trade = db.prepare('SELECT * FROM pump_dump_trades WHERE symbol = ?').get(symbol);
    if (!trade || (trade.status !== 'trailing_long' && trade.status !== 'trailing_short')) return;
    if (trailClosingFlags[symbol]) return;  // already firing the trail close order

    const now      = Date.now();
    const info     = cachedInstrumentInfo[symbol];
    if (!info) return;
    const tickSize = parseFloat(info.priceFilter.tickSize);
    const dec      = getDecimalPlaces(tickSize);
    const tradeQty = trade.trade_qty || parseFloat(info.lotSizeFilter.minOrderQty);
    const elapsedSeconds = trade.created_at ? (now - trade.created_at) / 1000 : 0;

    if (trade.trail_direction === 'long') {
        // ── Long trail (benchmarked from cut baseline) ──────────────────────
        const cutPrice  = trade.long_entry > 0 ? trade.long_entry : currentPrice;
        const profitPct = cutPrice > 0 ? (currentPrice - cutPrice) / cutPrice : 0;
        const baseDist  = trade.trail_distance || 0.0035;
        
        let targetSL = 0;
        let trailDist = baseDist;

        // Stage 3: Trend Runner (> +0.80% from cut) — Trail tightly 0.25% behind peak
        if (profitPct >= 0.0080) {
            trailDist = 0.0025;
            targetSL = currentPrice * (1 - trailDist);
        }
        // Stage 2: Net Profit Lock (> +0.35% from cut) — Lock minimum +0.25% pure gain
        else if (profitPct >= 0.0035) {
            const minGuaranteed = cutPrice * 1.0025; // Cut + 0.25% pure profit
            const trailingCalc  = currentPrice * (1 - baseDist);
            targetSL = Math.max(minGuaranteed, trailingCalc);
        }
        // Stage 1: Breakeven Arming (> +0.20% from cut) — Lock SL at Cut Price + 0.05% (covers all fees)
        else if (profitPct >= 0.0020) {
            const breakevenSL   = cutPrice * 1.0005; // Cut + 0.05% buffer
            const trailingCalc  = currentPrice * (1 - baseDist);
            targetSL = Math.max(breakevenSL, trailingCalc);
        }
        // Stall Guard: If trade stalls for > 30s without hitting +0.15%, arm breakeven
        else if (elapsedSeconds > 30 && profitPct > 0) {
            targetSL = cutPrice * 1.0002;
        }
        // Initial tight floor (capped at -0.20% from cut)
        else {
            targetSL = Math.max(cutPrice * (1 - 0.0020), currentPrice * (1 - baseDist));
        }

        const newSL = parseFloat((Math.floor(targetSL / tickSize) * tickSize).toFixed(dec));

        if (currentPrice > (trade.trail_peak || 0)) {
            // New peak — record peak and advance SL
            if (!trade.trail_sl || newSL > trade.trail_sl) {
                if (!lastTrailUpdate[symbol] || now - lastTrailUpdate[symbol] >= 500) {
                    lastTrailUpdate[symbol] = now;
                    db.prepare('UPDATE pump_dump_trades SET trail_peak=?, trail_sl=?, trail_distance=? WHERE symbol=?')
                        .run(currentPrice, newSL, trailDist, symbol);
                    emitPdTradeUpdate(symbol);
                    syncFromSQLite(db, 'pump_dump_trades', symbol);
                    console.log(`\x1b[32m[Trail] Long: Peak=${currentPrice.toFixed(4)}, SL=${newSL} (Move=+${(profitPct*100).toFixed(2)}%, Trail=${(trailDist*100).toFixed(2)}%)\x1b[0m`);
                }
            }
        } else if (!trade.trail_sl || newSL > trade.trail_sl) {
            // Ratchet SL upward even between peaks if stage upgrade occurs
            if (!lastTrailUpdate[symbol] || now - lastTrailUpdate[symbol] >= 500) {
                lastTrailUpdate[symbol] = now;
                db.prepare('UPDATE pump_dump_trades SET trail_sl=?, trail_distance=? WHERE symbol=?')
                    .run(newSL, trailDist, symbol);
                emitPdTradeUpdate(symbol);
                syncFromSQLite(db, 'pump_dump_trades', symbol);
                console.log(`\x1b[32m[Trail] Long SL Ratcheted: SL=${newSL} (Move=+${(profitPct*100).toFixed(2)}%)\x1b[0m`);
            }
        } else if (trade.trail_sl && currentPrice <= trade.trail_sl) {
            // ✅ Trail SL hit — close tradeQty of long reduce-only
            trailClosingFlags[symbol] = true;
            await cancelActiveLimitTP(symbol); // Cancel any pending Limit TP
            console.log(`[PD] Long trail SL hit @ ${currentPrice.toFixed(4)} (SL was ${trade.trail_sl}). Closing ${tradeQty} long...`);
            db.prepare('UPDATE pump_dump_trades SET status=? WHERE symbol=?').run('closing', symbol);
            syncFromSQLite(db, 'pump_dump_trades', symbol);
            try {
                const res = await apiRequest('/v5/order/create', {
                    category: 'linear', symbol, side: 'Sell', orderType: 'Market',
                    qty: tradeQty.toString(), timeInForce: 'IOC', positionIdx: 1,
                    reduceOnly: true, orderLinkId: `pd-trail-long-${Date.now()}-${symbol}`
                }, 'POST');
                console.log('[PD] Trail close long:', JSON.stringify(res));
                await finalizeTradeAfterExit(symbol, trade, currentPrice);
            } catch (err) {
                console.error('[Trail] Close long failed:', err.message);
                db.prepare('UPDATE pump_dump_trades SET status=? WHERE symbol=?').run('trailing_long', symbol);
                syncFromSQLite(db, 'pump_dump_trades', symbol);
            } finally {
                delete trailClosingFlags[symbol];
            }
        }

    } else if (trade.trail_direction === 'short') {
        // ── Short trail (benchmarked from cut baseline) ─────────────────────
        const cutPrice  = trade.short_entry > 0 ? trade.short_entry : currentPrice;
        const profitPct = cutPrice > 0 ? (cutPrice - currentPrice) / cutPrice : 0;
        const baseDist  = trade.trail_distance || 0.0035;
        
        let targetSL = 0;
        let trailDist = baseDist;

        // Stage 3: Trend Runner (> +0.80% from cut) — Trail tightly 0.25% behind peak
        if (profitPct >= 0.0080) {
            trailDist = 0.0025;
            targetSL = currentPrice * (1 + trailDist);
        }
        // Stage 2: Net Profit Lock (> +0.35% from cut) — Lock minimum +0.25% pure gain
        else if (profitPct >= 0.0035) {
            const minGuaranteed = cutPrice * 0.9975; // Cut - 0.25% pure profit
            const trailingCalc  = currentPrice * (1 + baseDist);
            targetSL = Math.min(minGuaranteed, trailingCalc);
        }
        // Stage 1: Breakeven Arming (> +0.20% from cut) — Lock SL at Cut Price - 0.05% (covers all fees)
        else if (profitPct >= 0.0020) {
            const breakevenSL   = cutPrice * 0.9995; // Cut - 0.05% buffer
            const trailingCalc  = currentPrice * (1 + baseDist);
            targetSL = Math.min(breakevenSL, trailingCalc);
        }
        // Stall Guard: If trade stalls for > 30s without hitting +0.15%, arm breakeven
        else if (elapsedSeconds > 30 && profitPct > 0) {
            targetSL = cutPrice * 0.9998;
        }
        // Initial tight ceiling (capped at +0.20% from cut)
        else {
            targetSL = Math.min(cutPrice * (1 + 0.0020), currentPrice * (1 + baseDist));
        }

        const newSL = parseFloat((Math.ceil(targetSL / tickSize) * tickSize).toFixed(dec));

        if (currentPrice < (trade.trail_peak || Infinity)) {
            // New low — record peak and advance SL downward
            if (!trade.trail_sl || newSL < trade.trail_sl) {
                if (!lastTrailUpdate[symbol] || now - lastTrailUpdate[symbol] >= 500) {
                    lastTrailUpdate[symbol] = now;
                    db.prepare('UPDATE pump_dump_trades SET trail_peak=?, trail_sl=?, trail_distance=? WHERE symbol=?')
                        .run(currentPrice, newSL, trailDist, symbol);
                    emitPdTradeUpdate(symbol);
                    syncFromSQLite(db, 'pump_dump_trades', symbol);
                    console.log(`\x1b[31m[Trail] Short: Peak=${currentPrice.toFixed(4)}, SL=${newSL} (Move=+${(profitPct*100).toFixed(2)}%, Trail=${(trailDist*100).toFixed(2)}%)\x1b[0m`);
                }
            }
        } else if (!trade.trail_sl || newSL < trade.trail_sl) {
            // Ratchet SL downward even between peaks if stage upgrade occurs
            if (!lastTrailUpdate[symbol] || now - lastTrailUpdate[symbol] >= 500) {
                lastTrailUpdate[symbol] = now;
                db.prepare('UPDATE pump_dump_trades SET trail_sl=?, trail_distance=? WHERE symbol=?')
                    .run(newSL, trailDist, symbol);
                emitPdTradeUpdate(symbol);
                syncFromSQLite(db, 'pump_dump_trades', symbol);
                console.log(`\x1b[31m[Trail] Short SL Ratcheted: SL=${newSL} (Move=+${(profitPct*100).toFixed(2)}%)\x1b[0m`);
            }
        } else if (trade.trail_sl && currentPrice >= trade.trail_sl) {
            // ✅ Trail SL hit — close tradeQty of short reduce-only
            trailClosingFlags[symbol] = true;
            await cancelActiveLimitTP(symbol); // Cancel any pending Limit TP
            console.log(`[PD] Short trail SL hit @ ${currentPrice.toFixed(4)} (SL was ${trade.trail_sl}). Closing ${tradeQty} short...`);
            db.prepare('UPDATE pump_dump_trades SET status=? WHERE symbol=?').run('closing', symbol);
            syncFromSQLite(db, 'pump_dump_trades', symbol);
            try {
                const res = await apiRequest('/v5/order/create', {
                    category: 'linear', symbol, side: 'Buy', orderType: 'Market',
                    qty: tradeQty.toString(), timeInForce: 'IOC', positionIdx: 2,
                    reduceOnly: true, orderLinkId: `pd-trail-short-${Date.now()}-${symbol}`
                }, 'POST');
                console.log('[PD] Trail close short:', JSON.stringify(res));
                await finalizeTradeAfterExit(symbol, trade, currentPrice);
            } catch (err) {
                console.error('[Trail] Close short failed:', err.message);
                db.prepare('UPDATE pump_dump_trades SET status=? WHERE symbol=?').run('trailing_short', symbol);
                syncFromSQLite(db, 'pump_dump_trades', symbol);
            } finally {
                delete trailClosingFlags[symbol];
            }
        }
    }
}

// Fetches official Bybit PnL after both legs close and persists the final trade record
async function finalizeTradeAfterExit(symbol, pdTrade, exitPrice) {
    // Wait for Bybit to process the fill before querying closed-pnl
    await new Promise(r => setTimeout(r, 1200));

    let cutPnl    = pdTrade.cut_pnl || 0;
    let trailPnl  = 0;
    let totalPnl  = 0;
    let closePrice = exitPrice || pdTrade.trail_sl || lastValidPrice;
    const tradeQty = pdTrade.trade_qty || parseFloat(cachedInstrumentInfo[symbol]?.lotSizeFilter?.minOrderQty || 1);

    try {
        const closedRes = await apiRequest('/v5/position/closed-pnl', { category: 'linear', symbol, limit: 6 });
        if (closedRes.retCode === 0 && closedRes.result?.list) {
            const tradeList = closedRes.result.list.filter(item => parseInt(item.updatedTime) >= (pdTrade.created_at - 5000));
            if (tradeList.length >= 2) {
                const isLongTrail = pdTrade.trail_direction === 'long';
                const cutItem   = tradeList.find(i => isLongTrail ? i.side === 'Buy'  : i.side === 'Sell');
                const trailItem = tradeList.find(i => isLongTrail ? i.side === 'Sell' : i.side === 'Buy');
                if (cutItem && trailItem) {
                    cutPnl    = parseFloat(cutItem.closedPnl);
                    trailPnl  = parseFloat(trailItem.closedPnl);
                    totalPnl  = cutPnl + trailPnl;
                    closePrice = parseFloat(trailItem.avgExitPrice) || closePrice;
                } else {
                    totalPnl = tradeList.slice(0, 2).reduce((s, i) => s + parseFloat(i.closedPnl), 0);
                }
            } else if (tradeList.length === 1) {
                trailPnl   = parseFloat(tradeList[0].closedPnl);
                totalPnl   = cutPnl + trailPnl;
                closePrice = parseFloat(tradeList[0].avgExitPrice) || closePrice;
            }
        }
    } catch (err) {
        console.error('[PD] Bybit closed PnL fetch error:', err.message);
    }

    // Fallback to estimated PnL using trade_qty (minQty) not full hedge qty
    if (totalPnl === 0 && trailPnl === 0) {
        trailPnl = pdTrade.trail_direction === 'long'
            ? (closePrice - pdTrade.long_entry)  * tradeQty
            : (pdTrade.short_entry - closePrice) * tradeQty;
        totalPnl = cutPnl + trailPnl;
    }

    // Check remaining positions on Bybit to decide if we continue trading
    const remainingPositions = await getPositions(symbol);
    const longPos  = remainingPositions.find(p => p.side === 'Buy' && parseFloat(p.size) > 0);
    const shortPos = remainingPositions.find(p => p.side === 'Sell' && parseFloat(p.size) > 0);

    const hasBothPositions = !!(longPos && shortPos);

    if (hasBothPositions) {
        // Both sides still exist — automatically loop and continue trading
        const newLongEntry  = parseFloat(longPos.avgPrice || pdTrade.long_entry);
        const newShortEntry = parseFloat(shortPos.avgPrice || pdTrade.short_entry);
        const minPosSize    = Math.min(parseFloat(longPos.size), parseFloat(shortPos.size));

        db.prepare(`
            UPDATE pump_dump_trades 
            SET status='hedged', long_entry=?, short_entry=?, qty=?,
                trail_direction=NULL, trail_peak=NULL, trail_sl=NULL,
                cut_pnl=0, trail_pnl=?, total_pnl=?, close_price=?, created_at=?
            WHERE symbol=?
        `).run(newLongEntry, newShortEntry, minPosSize, trailPnl, totalPnl, closePrice, Date.now(), symbol);

        emitPdTradeUpdate(symbol);
        emitLegUpdate(symbol);
        syncFromSQLite(db, 'pump_dump_trades', symbol);

        io.emit('signalEvent', {
            symbol,
            type: 'CLOSE',
            message: `✓ Cycle finished! Net PnL: ${totalPnl >= 0 ? '+' : ''}${totalPnl.toFixed(4)} USDT (Cut: ${cutPnl >= 0 ? '+' : ''}${cutPnl.toFixed(4)}, Trail: ${trailPnl >= 0 ? '+' : ''}${trailPnl.toFixed(4)}). 🔄 Continuing trading with remaining Long (${longPos.size}) & Short (${shortPos.size})...`,
            ts: Date.now()
        });
        console.log(`[PD] Cycle finished for ${symbol}. Remaining positions found (Long: ${longPos.size}, Short: ${shortPos.size}). Resuming 'hedged' state.`);
    } else {
        // One or both positions closed — complete the trade session
        db.prepare('UPDATE pump_dump_trades SET status=?, trail_pnl=?, total_pnl=?, close_price=? WHERE symbol=?')
            .run('closed', trailPnl, totalPnl, closePrice, symbol);
        db.prepare('DELETE FROM active_trades WHERE symbol=?').run(symbol);
        emitPdTradeUpdate(symbol);
        emitLegUpdate(symbol);
        syncFromSQLite(db, 'pump_dump_trades', symbol);
        deleteDoc('active_trades', symbol);

        io.emit('signalEvent', {
            symbol,
            type: 'CLOSE',
            message: `🏁 Finished trading! No more dual positions remaining. Final Net PnL: ${totalPnl >= 0 ? '+' : ''}${totalPnl.toFixed(4)} USDT`,
            ts: Date.now()
        });
        console.log(`[PD] Finished trading for ${symbol}. Both positions are no longer open.`);
    }

    // Release lock for this symbol so next cycle can process safely
    delete symbolLocks[symbol];
}

// ─── Startup Recovery ─────────────────────────────────────
// Called once at boot. If a previous run crashed while status='closing'
// (exit order fired but finalizeTradeAfterExit never completed), re-run it
// so the trade record is properly saved and the UI reflects the correct state.
async function recoverStuckTrades() {
    const stuck = db.prepare("SELECT * FROM pump_dump_trades WHERE status='closing'").all();
    if (!stuck.length) return;
    console.log(`[Recovery] Found ${stuck.length} trade(s) stuck in 'closing' — re-finalizing...`);
    for (const trade of stuck) {
        console.log(`[Recovery] Re-finalizing: ${trade.symbol}`);
        try {
            await finalizeTradeAfterExit(trade.symbol, trade, trade.trail_sl || trade.trail_peak || lastValidPrice);
        } catch (err) {
            console.error(`[Recovery] Failed to finalize ${trade.symbol}:`, err.message);
            // Mark as closed so it doesn't block the UI
            db.prepare("UPDATE pump_dump_trades SET status='closed' WHERE symbol=?").run(trade.symbol);
            db.prepare('DELETE FROM active_trades WHERE symbol=?').run(trade.symbol);
            emitPdTradeUpdate(trade.symbol);
            emitLegUpdate(trade.symbol);
            syncFromSQLite(db, 'pump_dump_trades', trade.symbol);
            deleteDoc('active_trades', trade.symbol);
        }
    }
}

// ─── Balanced Hedge Opening Engine ───────────────────────
async function openHedgePositions(symbol, price, info) {
    const minOrderQty = parseFloat(info?.lotSizeFilter?.minOrderQty || 1);
    const qtyStep     = parseFloat(info?.lotSizeFilter?.qtyStep || minOrderQty);
    const dec         = getDecimalPlaces(qtyStep);

    // Calculate 100.00% exact identical size for both Long and Short
    const rawQty   = (HEDGE_MARGIN_USDT * HEDGE_LEVERAGE) / price;
    const exactQty = parseFloat((Math.floor(rawQty / qtyStep) * qtyStep).toFixed(dec));
    const finalQty = Math.max(exactQty, minOrderQty);
    const qtyStr   = finalQty.toFixed(dec);

    console.log(`[Hedge Engine] Opening 100% identical hedge for ${symbol}: ${qtyStr} Long & ${qtyStr} Short (${HEDGE_MARGIN_USDT} USDT margin @ ${HEDGE_LEVERAGE}x)...`);

    // Set leverage on Bybit
    await apiRequest('/v5/position/set-leverage', {
        category: 'linear', symbol,
        buyLeverage: HEDGE_LEVERAGE.toString(),
        sellLeverage: HEDGE_LEVERAGE.toString()
    }, 'POST').catch(() => {});

    // Open Long (Buy, positionIdx: 1) and Short (Sell, positionIdx: 2) with EXACT same size
    const [longRes, shortRes] = await Promise.all([
        apiRequest('/v5/order/create', {
            category: 'linear', symbol, side: 'Buy', orderType: 'Market',
            qty: qtyStr, timeInForce: 'GTC', positionIdx: 1,
            orderLinkId: `hedge-long-${Date.now()}-${symbol}`
        }, 'POST'),
        apiRequest('/v5/order/create', {
            category: 'linear', symbol, side: 'Sell', orderType: 'Market',
            qty: qtyStr, timeInForce: 'GTC', positionIdx: 2,
            orderLinkId: `hedge-short-${Date.now()}-${symbol}`
        }, 'POST')
    ]);

    console.log('[Hedge Engine] Long order response:', JSON.stringify(longRes));
    console.log('[Hedge Engine] Short order response:', JSON.stringify(shortRes));
    return finalQty;
}

// ─── Manual Leg Hedge Engine (existing) ───────────────────
let isHedging = false;
async function checkHedgeTriggers(symbol, currentPrice, info) {
    if (isHedging) return;
    const leg = db.prepare('SELECT * FROM manual_legs WHERE symbol = ?').get(symbol);
    if (!leg || leg.hedged !== 0 || !info) return;

    let trigger = false;
    if (leg.leg_direction === 'long'  && currentPrice <= leg.open_price * 0.99) trigger = true;
    if (leg.leg_direction === 'short' && currentPrice >= leg.open_price * 1.01) trigger = true;

    if (trigger) {
        isHedging = true;
        const side     = leg.leg_direction === 'long' ? 'Sell' : 'Buy';
        const posIdx   = leg.leg_direction === 'long' ? 2 : 1;
        try {
            db.prepare('UPDATE manual_legs SET hedged = 1 WHERE symbol = ?').run(symbol);
            emitLegUpdate(symbol);
            syncFromSQLite(db, 'manual_legs', symbol);
            if (leg.tp_order_id) {
                await apiRequest('/v5/order/cancel', { category: 'linear', symbol, orderId: leg.tp_order_id }, 'POST').catch(() => {});
            }
            await apiRequest('/v5/order/create', {
                category: 'linear', symbol, side, orderType: 'Market',
                qty: leg.size.toString(), timeInForce: 'GTC', positionIdx: posIdx,
                orderLinkId: `hedge-${Date.now()}-${symbol}`
            }, 'POST');
        } finally { isHedging = false; }
    }
}

async function placeTakeProfitOrder(symbol, dir, openPrice, qty, info) {
    try {
        const tickSize = parseFloat(info.priceFilter.tickSize);
        const tpPrice  = dir === 'long' ? openPrice * 1.0025 : openPrice * 0.9975;
        const side     = dir === 'long' ? 'Sell' : 'Buy';
        const posIdx   = dir === 'long' ? 1 : 2;
        const rounded  = (Math.floor(tpPrice / tickSize) * tickSize).toFixed(getDecimalPlaces(tickSize));
        const res = await apiRequest('/v5/order/create', {
            category: 'linear', symbol, side, orderType: 'Limit',
            price: rounded, qty: qty.toString(), timeInForce: 'GTC',
            positionIdx: posIdx, reduceOnly: true,
            orderLinkId: `tp-manual-${Date.now()}-${symbol}`
        }, 'POST');
        return res.retCode === 0 ? res.result?.orderId : null;
    } catch { return null; }
}

// ─── Public WebSocket (Multi-stream) ─────────────────────
let ws             = null;
let wsSymbol       = null;
let lastValidPrice = null;
let lastValidTime  = null;
let pingTimer      = null;
let reconnectTimer = null;
let reconnectDelay = RECONNECT_INITIAL;
let intentionalClose = false;

function sendWS(obj) { if (ws?.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj)); }
function startPing()  { clearInterval(pingTimer); pingTimer = setInterval(() => sendWS({ op: 'ping' }), PING_INTERVAL_MS); }
function stopPing()   { clearInterval(pingTimer); }

function scheduleReconnect(symbol) {
    clearTimeout(reconnectTimer);
    reconnectTimer = setTimeout(() => {
        reconnectDelay = Math.min(reconnectDelay * 2, RECONNECT_MAX);
        connectWS(symbol);
    }, reconnectDelay);
}

function connectWS(symbol) {
    intentionalClose = false;
    wsSymbol = symbol;

    const socket = new WebSocket(BYBIT_WS_URL);
    ws = socket;

    socket.on('open', () => {
        console.log(`[WS] Connected. Subscribing individually to streams for ${symbol}`);
        reconnectDelay = RECONNECT_INITIAL;
        // Subscribe individually so one unsupported topic (e.g. liquidation on low-volume pairs)
        // does NOT kill the entire batch — tickers/orderbook/trades are mandatory
        const coreTopics = [
            `tickers.${symbol}`,
            `orderbook.50.${symbol}`,
            `publicTrade.${symbol}`,
            `kline.1.${symbol}`
        ];
        coreTopics.forEach(topic => ws.send(JSON.stringify({ op: 'subscribe', args: [topic] })));
        // Liquidation is optional — use correct allLiquidation topic per Bybit v5 docs
        ws.send(JSON.stringify({ op: 'subscribe', args: [`allLiquidation.${symbol}`] }));
        startPing();
        io.emit('wsStatus', { connected: true, symbol });
    });

    socket.on('message', (raw) => {
        let msg;
        try { msg = JSON.parse(raw.toString()); } catch { return; }
        if (msg.op === 'pong' || msg.ret_msg === 'pong') return;

        if (msg.op === 'subscribe') {
            if (!msg.success) {
                const retMsg = (msg.ret_msg || '').toLowerCase();
                if (retMsg.includes('liquidation')) {
                    // Liquidation stream not available for this pair — not fatal, skip it
                    console.warn(`[WS] Liquidation unavailable for ${symbol} — skipping (non-fatal)`);
                } else {
                    // Core stream failed — reconnect
                    console.error('[WS] Core subscription FAILED:', msg);
                    disconnectWS();
                    scheduleReconnect(symbol);
                }
            }
            return;
        }

        if (!msg.topic || !msg.data) return;
        const topic = msg.topic;

        // ── Tickers (price, OI, funding) ─────────────────
        if (topic === `tickers.${symbol}`) {
            const d = msg.data;
            if (d.symbol !== wsSymbol) return;
            const price = validatePrice(d.lastPrice);
            if (price === null) return;
            if (lastValidPrice !== null && Math.abs((price - lastValidPrice) / lastValidPrice) * 100 > 50) {
                io.emit('wsWarning', { symbol, message: `Suspicious price jump, skipped` });
                return;
            }
            lastValidPrice = price;
            lastValidTime  = Date.now();
            io.emit('livePrice', buildPayload(symbol, price));
            getOrCreateScorer(symbol).updatePrice(price);
            if (d.openInterestValue !== undefined || d.openInterest !== undefined) {
                getOrCreateScorer(symbol).updateTicker(d.openInterestValue || d.openInterest, d.fundingRate);
            }
            checkHedgeTriggers(symbol, price, cachedInstrumentInfo[symbol]);
            updateTrailingStop(symbol, price).catch(() => {});
        }

        // ── Orderbook (bid/ask imbalance) ─────────────────
        else if (topic === `orderbook.50.${symbol}`) {
            const { type, b, a } = msg.data;
            updateObState(symbol, type === 'snapshot' ? 'snapshot' : 'delta', b || [], a || []);
        }

        // ── Public Trades (volume direction) ──────────────
        else if (topic === `publicTrade.${symbol}`) {
            const trades = Array.isArray(msg.data) ? msg.data : [msg.data];
            let buyVol = 0, sellVol = 0;
            trades.forEach(t => {
                const v = parseFloat(t.v || 0);
                if (t.S === 'Buy') buyVol += v; else sellVol += v;
            });
            if (buyVol + sellVol > 0) getOrCreateScorer(symbol).updateTrade(buyVol, sellVol);
        }

        // ── Kline (candle momentum) ───────────────────────
        else if (topic === `kline.1.${symbol}`) {
            const klines = Array.isArray(msg.data) ? msg.data : [msg.data];
            klines.forEach(k => getOrCreateScorer(symbol).updateKline(parseFloat(k.open), parseFloat(k.close)));
        }

        // ── Liquidations (allLiquidation topic) ──────────────
        else if (topic === `allLiquidation.${symbol}`) {
            const liqs = Array.isArray(msg.data) ? msg.data : [msg.data];
            liqs.forEach(liq => {
                // allLiquidation: side=Buy means long position was liquidated (bearish)
                //                 side=Sell means short position was liquidated (bullish)
                const isBullish = liq.S === 'Sell'; // short liq = bullish
                const sigSide   = isBullish ? 'Buy' : 'Sell'; // mapped for SignalScorer convention
                getOrCreateScorer(symbol).updateLiquidation(sigSide, liq.v);
                io.emit('signalEvent', {
                    symbol, type: 'LIQ',
                    message: `${isBullish ? '🔴 Short liq' : '🟢 Long liq'}: ${parseFloat(liq.v || 0).toFixed(3)} @ ${parseFloat(liq.p || 0).toFixed(4)}`,
                    ts: Date.now()
                });
            });
        }
    });

    socket.on('error', (err) => {
        console.error('[WS] Error:', err.message);
        io.emit('wsStatus', { connected: false, symbol, reason: err.message });
    });

    socket.on('close', (code) => {
        stopPing();
        console.warn(`[WS] Closed (code ${code})`);
        io.emit('wsStatus', { connected: false, symbol, reason: `Disconnected (code ${code})` });
        if (!intentionalClose && wsSymbol) scheduleReconnect(wsSymbol);
    });
}

function disconnectWS() {
    intentionalClose = true;
    wsSymbol = lastValidPrice = lastValidTime = null;
    stopPing();
    clearTimeout(reconnectTimer);
    if (ws) { ws.removeAllListeners(); ws.terminate(); ws = null; }
}

// ─── Private WebSocket ────────────────────────────────────
let privateWS              = null;
let privatePingTimer       = null;
let privateReconnectTimer  = null;
let privateReconnectDelay  = 1000;

function connectPrivateWS() {
    const socket = new WebSocket('wss://stream.bybit.com/v5/private');
    privateWS = socket;

    socket.on('open', () => {
        console.log('[WS Private] Connected. Authenticating...');
        privateReconnectDelay = 1000;
        const expires = Date.now() + 10000;
        const sig = crypto.createHmac('sha256', API_SECRET).update(`GET/realtime${expires}`).digest('hex');
        socket.send(JSON.stringify({ op: 'auth', args: [API_KEY, expires.toString(), sig] }));
        clearInterval(privatePingTimer);
        privatePingTimer = setInterval(() => {
            if (privateWS?.readyState === WebSocket.OPEN) privateWS.send(JSON.stringify({ op: 'ping' }));
        }, PING_INTERVAL_MS);
    });

    socket.on('message', (raw) => {
        let msg;
        try { msg = JSON.parse(raw.toString()); } catch { return; }
        if (msg.op === 'pong' || msg.ret_msg === 'pong') return;
        if (msg.op === 'auth') {
            if (msg.success) {
                console.log('[WS Private] Auth OK. Subscribing to position + order...');
                socket.send(JSON.stringify({ op: 'subscribe', args: ['position', 'order'] }));
            } else { console.error('[WS Private] Auth FAILED:', msg); }
            return;
        }
        if (msg.topic === 'position' && msg.data) handlePrivatePositionUpdate(msg.data);
        if (msg.topic === 'order'    && msg.data) handlePrivateOrderUpdate(msg.data);
    });

    socket.on('close', (code) => {
        clearInterval(privatePingTimer);
        console.warn(`[WS Private] Closed (${code}). Reconnecting...`);
        clearTimeout(privateReconnectTimer);
        privateReconnectTimer = setTimeout(() => {
            privateReconnectDelay = Math.min(privateReconnectDelay * 2, 30000);
            connectPrivateWS();
        }, privateReconnectDelay);
    });

    socket.on('error', (err) => console.error('[WS Private] Error:', err.message));
}

// Auto-close when combined PnL (any dual-position trade) >= 10 USDT
let isAutoClosing = false;
async function checkDualPositionsAutoClose(symbol, positions) {
    if (isAutoClosing) return;
    const longPos  = positions.find(p => p.side === 'Buy');
    const shortPos = positions.find(p => p.side === 'Sell');
    if (longPos && shortPos) {
        const combinedPnl = parseFloat(longPos.unrealisedPnl || 0) + parseFloat(shortPos.unrealisedPnl || 0);
        if (combinedPnl >= 10.0) {
            isAutoClosing = true;
            console.log(`[Auto Close] ${symbol}: Combined PnL ${combinedPnl.toFixed(4)} USDT >= 10. Closing.`);
            try {
                db.prepare('DELETE FROM manual_legs WHERE symbol = ?').run(symbol);
                emitLegUpdate(symbol);
                deleteDoc('manual_legs', symbol);
                await closeAllPositionsMarket(symbol);
                io.emit('signalEvent', { symbol, type: 'CLOSE', message: `Auto-close: combined PnL reached +${combinedPnl.toFixed(4)} USDT`, ts: Date.now() });
            } finally { isAutoClosing = false; }
        }
    }
}

function handlePrivatePositionUpdate(list) {
    // Broadcast live PnL for currently watched symbol
    const cur = list.filter(p => p.symbol === wsSymbol);
    if (cur.length > 0) io.emit('positions', { symbol: wsSymbol, list: cur });

    list.forEach(async (p) => {
        const symbol = p.symbol;

        // Trail close is now fully server-managed via updateTrailingStop().
        // No size===0 detection needed here — the trail fires a reduce-only
        // minQty order directly and calls finalizeTradeAfterExit().
        // ── Existing manual-leg engine ────────────────────
        const activeTrade = db.prepare('SELECT * FROM active_trades WHERE symbol = ?').get(symbol);
        if (!activeTrade) return;

        try {
            const positions = await getPositions(symbol);
            const longSize  = parseFloat(positions.find(pos => pos.side === 'Buy')?.size  || 0);
            const shortSize = parseFloat(positions.find(pos => pos.side === 'Sell')?.size || 0);

            if ((longSize > 0 && shortSize > 0) || (longSize === 0 && shortSize === 0)) {
                const existing = db.prepare('SELECT * FROM manual_legs WHERE symbol = ?').get(symbol);
                if (existing) {
                    if (existing.tp_order_id) {
                        await apiRequest('/v5/order/cancel', { category: 'linear', symbol, orderId: existing.tp_order_id }, 'POST').catch(() => {});
                    }
                    db.prepare('DELETE FROM manual_legs WHERE symbol = ?').run(symbol);
                    emitLegUpdate(symbol);
                    deleteDoc('manual_legs', symbol);
                }
            }

            await checkDualPositionsAutoClose(symbol, positions);
            await checkCircuitBreaker(symbol, positions);
        } catch (err) {
            console.error('[Hedge Engine] Position update error:', err.message);
        }
    });
}

function handlePrivateOrderUpdate(list) {
    list.forEach(async (o) => {
        const symbol = o.symbol;
        if (symbol !== wsSymbol) return;
        const activeTrade = db.prepare('SELECT * FROM active_trades WHERE symbol = ?').get(symbol);
        if (!activeTrade) return;

        const isFilled   = o.orderStatus === 'Filled';
        const isBotOrder = o.orderLinkId?.startsWith('hedge-') || o.orderLinkId?.startsWith('close-') ||
                           o.orderLinkId?.startsWith('tp-')    || o.orderLinkId?.startsWith('pd-');
        const isOpening  = o.reduceOnly === false || o.reduceOnly === 'false';

        // ── Handle Maker Limit Take-Profit fills ──────────────
        if (isFilled && o.orderLinkId?.startsWith('pd-tp-')) {
            const trade = db.prepare('SELECT * FROM pump_dump_trades WHERE symbol = ?').get(symbol);
            if (trade && (trade.status === 'trailing_long' || trade.status === 'trailing_short')) {
                delete activeLimitTPs[symbol];
                const fillPrice = parseFloat(o.avgPrice || o.price || 0);
                console.log(`[TP] ✓ Maker Limit TP FILLED for ${symbol} @ ${fillPrice}! Finalizing trade cycle...`);
                db.prepare('UPDATE pump_dump_trades SET status=? WHERE symbol=?').run('closing', symbol);
                syncFromSQLite(db, 'pump_dump_trades', symbol);
                await finalizeTradeAfterExit(symbol, trade, fillPrice);
            }
            return;
        }

        if (isFilled && !isBotOrder && isOpening) {
            try {
                const openPrice = parseFloat(o.avgPrice || o.price || lastValidPrice || 0);
                if (openPrice <= 0) return;

                const positions = await getPositions(symbol);
                const longSize  = parseFloat(positions.find(pos => pos.side === 'Buy')?.size  || 0);
                const shortSize = parseFloat(positions.find(pos => pos.side === 'Sell')?.size || 0);

                if ((longSize > 0 && shortSize > 0) || (longSize === 0 && shortSize === 0)) {
                    const existing = db.prepare('SELECT * FROM manual_legs WHERE symbol = ?').get(symbol);
                    if (existing) {
                        if (existing.tp_order_id) await apiRequest('/v5/order/cancel', { category: 'linear', symbol, orderId: existing.tp_order_id }, 'POST').catch(() => {});
                        db.prepare('DELETE FROM manual_legs WHERE symbol = ?').run(symbol);
                        emitLegUpdate(symbol);
                        deleteDoc('manual_legs', symbol);
                    }
                } else {
                    const diffSize = Math.abs(longSize - shortSize);
                    const dir = longSize > shortSize ? 'long' : 'short';
                    const existingLeg = db.prepare('SELECT * FROM manual_legs WHERE symbol = ?').get(symbol);
                    if (existingLeg?.tp_order_id) {
                        await apiRequest('/v5/order/cancel', { category: 'linear', symbol, orderId: existingLeg.tp_order_id }, 'POST').catch(() => {});
                    }
                    const info = cachedInstrumentInfo[symbol];
                    const tpOrderId = await placeTakeProfitOrder(symbol, dir, openPrice, diffSize, info);
                    db.prepare('INSERT OR REPLACE INTO manual_legs (symbol, leg_direction, open_price, size, hedged, tp_order_id) VALUES (?, ?, ?, ?, 0, ?)')
                        .run(symbol, dir, openPrice, diffSize, tpOrderId);
                    emitLegUpdate(symbol);
                    syncFromSQLite(db, 'manual_legs', symbol);
                }
            } catch (err) {
                console.error('[Order Stream] Error:', err.message);
            }
        }
    });
}

// ─── Socket.io ────────────────────────────────────────────
io.on('connection', async (socket) => {
    console.log('[IO] Client connected:', socket.id);
    socket.emit('symbolList', Object.keys(cachedInstrumentInfo).sort());

    // If there is an active running trade, push its state immediately so the UI auto-loads
    const active = db.prepare('SELECT * FROM active_trades LIMIT 1').get();
    const activeSym = active?.symbol || wsSymbol;
    if (activeSym) {
        socket.emit('activeSymbol', { symbol: activeSym });
        if (lastValidPrice !== null) {
            socket.emit('livePrice', buildPayload(activeSym, lastValidPrice));
        }
        socket.emit('positions', { symbol: activeSym, list: await getPositions(activeSym) });
        emitLegUpdate(activeSym);
        emitPdTradeUpdate(activeSym);
    }

    socket.on('watchSymbol', async (symbol) => {
        if (!cachedInstrumentInfo[symbol]) {
            socket.emit('wsWarning', { symbol, message: `Unknown symbol: ${symbol}` });
            return;
        }
        if (wsSymbol === symbol && lastValidPrice !== null) {
            socket.emit('livePrice', buildPayload(symbol, lastValidPrice));
            socket.emit('positions', { symbol, list: await getPositions(symbol) });
            emitLegUpdate(symbol);
            emitPdTradeUpdate(symbol);
            return;
        }
        disconnectWS();
        reconnectDelay = RECONNECT_INITIAL;
        connectWS(symbol);
        socket.emit('positions', { symbol, list: await getPositions(symbol) });
        emitLegUpdate(symbol);
        emitPdTradeUpdate(symbol);
    });

    // ── START: open 5 USDT long + short hedge (or adopt existing) ──
    socket.on('startTrading', async ({ symbol }) => {
        if (!symbol) return;
        const info = cachedInstrumentInfo[symbol];
        if (!info) { socket.emit('wsWarning', { symbol, message: 'Instrument info not ready.' }); return; }

        // Resolve price: prefer live WS price; fall back to REST for slow/low-volume pairs
        let price = lastValidPrice;
        if (!price) {
            socket.emit('wsWarning', { symbol, message: 'Fetching live market price...' });
            try {
                const tickerRes = await apiRequest('/v5/market/tickers', { category: 'linear', symbol });
                if (tickerRes.retCode === 0 && tickerRes.result?.list?.[0]) {
                    price = validatePrice(tickerRes.result.list[0].lastPrice);
                    if (price) lastValidPrice = price;
                }
            } catch (e) {
                console.error('[Start] REST price fetch failed:', e.message);
            }
        }

        if (!price) {
            socket.emit('wsWarning', { symbol, message: 'Cannot get price. Ensure WS is connected, then try again.' });
            return;
        }

        try {
            // Check live positions on Bybit first
            const existingPositions = await getPositions(symbol);
            const hasLong  = existingPositions.some(p => p.side === 'Buy'  && parseFloat(p.size) > 0);
            const hasShort = existingPositions.some(p => p.side === 'Sell' && parseFloat(p.size) > 0);

            let qty = 0;
            let longEntry = price;
            let shortEntry = price;

            if (hasLong && hasShort) {
                // Adopt existing open dual positions on Bybit
                const longPos  = existingPositions.find(p => p.side === 'Buy');
                const shortPos = existingPositions.find(p => p.side === 'Sell');
                longEntry  = parseFloat(longPos.avgPrice || price);
                shortEntry = parseFloat(shortPos.avgPrice || price);
                qty        = Math.min(parseFloat(longPos.size), parseFloat(shortPos.size));

                io.emit('wsWarning', { symbol, message: `Adopting existing ${symbol} hedge (Long: ${longPos.size}, Short: ${shortPos.size}). Starting cycle...` });
            } else {
                // Open new hedge positions
                dbAddActiveTrade(symbol);
                io.emit('wsWarning', { symbol, message: `Opening ${HEDGE_MARGIN_USDT} USDT long + short hedge at ${HEDGE_LEVERAGE}× leverage...` });

                qty = await openHedgePositions(symbol, price, info);

                // Wait for market order fills
                await new Promise(r => setTimeout(r, 2500));

                const positions  = await getPositions(symbol);
                longEntry  = parseFloat(positions.find(p => p.side === 'Buy')?.avgPrice  || price);
                shortEntry = parseFloat(positions.find(p => p.side === 'Sell')?.avgPrice || price);
            }

            dbAddActiveTrade(symbol);
            db.prepare(`
                INSERT OR REPLACE INTO pump_dump_trades
                (symbol, long_entry, short_entry, qty, long_sl, short_sl, status, trail_direction, trail_distance, trail_peak, trail_sl, created_at)
                VALUES (?, ?, ?, ?, NULL, NULL, 'hedged', NULL, ?, NULL, NULL, ?)
            `).run(symbol, longEntry, shortEntry, qty, TRAIL_INITIAL_PCT, Date.now());

            emitLegUpdate(symbol);
            emitPdTradeUpdate(symbol);
            // Persist new trade to MongoDB
            syncFromSQLite(db, 'active_trades', symbol);
            syncFromSQLite(db, 'pump_dump_trades', symbol);
            
            const currentPositions = await getPositions(symbol);
            io.emit('positions', { symbol, list: currentPositions });
            io.emit('signalEvent', {
                symbol, type: 'START',
                message: `✓ Hedge active: Long @ ${longEntry.toFixed(4)} / Short @ ${shortEntry.toFixed(4)} | Qty: ${qty} — Scanning momentum...`,
                ts: Date.now()
            });
            console.log(`[IO] Hedge active for ${symbol}. Long: ${longEntry} | Short: ${shortEntry} | Qty: ${qty}`);
        } catch (err) {
            console.error('[Start] Error:', err.message);
            socket.emit('wsWarning', { symbol, message: `Start failed: ${err.message}` });
        }
    });

    // ── STOP: cancel monitoring + orders (positions stay open) ──
    socket.on('stopTrading', async ({ symbol }) => {
        if (!symbol) return;
        delete symbolLocks[symbol];
        await cancelActiveLimitTP(symbol);
        console.log(`[IO] Stop requested for ${symbol}`);
        try {
            db.transaction(() => {
                db.prepare('DELETE FROM active_trades  WHERE symbol = ?').run(symbol);
                db.prepare('DELETE FROM manual_legs    WHERE symbol = ?').run(symbol);
                db.prepare('DELETE FROM pump_dump_trades WHERE symbol = ?').run(symbol);
            })();
            emitLegUpdate(symbol);
            emitPdTradeUpdate(symbol);
            // Remove from MongoDB
            deleteDoc('active_trades', symbol);
            deleteDoc('manual_legs', symbol);
            deleteDoc('pump_dump_trades', symbol);
            await apiRequest('/v5/order/cancel-all', { category: 'linear', symbol }, 'POST').catch(() => {});
            io.emit('signalEvent', { symbol, type: 'CLOSE', message: `Monitoring stopped. Positions left open.`, ts: Date.now() });
        } catch (err) {
            socket.emit('wsWarning', { symbol, message: `Stop failed: ${err.message}` });
        }
    });

    // ── CLOSE: close all positions + cancel everything ─────
    socket.on('closePositions', async ({ symbol }) => {
        if (!symbol) return;
        delete symbolLocks[symbol];
        await cancelActiveLimitTP(symbol);
        console.log(`[IO] Close positions requested for ${symbol}`);
        try {
            db.transaction(() => {
                db.prepare('DELETE FROM active_trades    WHERE symbol = ?').run(symbol);
                db.prepare('DELETE FROM manual_legs      WHERE symbol = ?').run(symbol);
                db.prepare('DELETE FROM pump_dump_trades WHERE symbol = ?').run(symbol);
            })();
            emitLegUpdate(symbol);
            emitPdTradeUpdate(symbol);
            // Remove from MongoDB
            deleteDoc('active_trades', symbol);
            deleteDoc('manual_legs', symbol);
            deleteDoc('pump_dump_trades', symbol);
            await apiRequest('/v5/order/cancel-all', { category: 'linear', symbol }, 'POST').catch(() => {});
            await closeAllPositionsMarket(symbol);
            io.emit('signalEvent', { symbol, type: 'CLOSE', message: `All positions closed manually.`, ts: Date.now() });
            setTimeout(async () => {
                io.emit('positions', { symbol, list: await getPositions(symbol) });
            }, 1500);
        } catch (err) {
            socket.emit('wsWarning', { symbol, message: `Close failed: ${err.message}` });
        }
    });

    socket.on('disconnect', () => console.log('[IO] Client disconnected:', socket.id));
});

// ─── Periodic Tasks ───────────────────────────────────────
setInterval(async () => {
    if (!wsSymbol) return;
    const pos = await getPositions(wsSymbol);
    io.emit('positions', { symbol: wsSymbol, list: pos });
    emitLegUpdate(wsSymbol);
    emitPdTradeUpdate(wsSymbol);
    await checkDualPositionsAutoClose(wsSymbol, pos);
}, 3000);

async function fetchSymbolListFromREST() {
    try {
        const res  = await fetch('https://api.bybit.com/v5/market/tickers?category=linear');
        const data = await res.json();
        if (data.retCode === 0 && data.result?.list) {
            const symbols = data.result.list.filter(i => i.symbol.endsWith('USDT')).map(i => i.symbol).sort();
            io.emit('symbolList', symbols);
        }
    } catch (err) {
        console.error('[REST] Symbol list error:', err.message);
    }
}

// ─── Startup ──────────────────────────────────────────────
syncServerTime().then(() => {
    fetchAllInstrumentInfo().then(async () => {
        // Connect to MongoDB and restore persisted state into SQLite
        await connectMongo(process.env.MONGODB_URI);
        await restoreToSQLite(db);

        connectPrivateWS();
        setInterval(fetchSymbolListFromREST, 60000);

        // Recover any trades that were mid-close when the server last stopped
        await recoverStuckTrades();

        // Resume WebSocket for the active symbol (if any)
        const active = db.prepare('SELECT * FROM active_trades LIMIT 1').get();
        if (active) {
            console.log(`[Resume] Resuming WS for: ${active.symbol}`);
            connectWS(active.symbol);
        }
    });
});

server.listen(PORT, () => console.log(`[Server] Running on http://localhost:${PORT}`));
