/**
 * mongo.js — MongoDB persistence layer for BuySellGrid
 *
 * Strategy:
 *   - SQLite remains the fast, synchronous in-process store.
 *   - MongoDB is the durable backup for the 3 critical tables.
 *   - On every write: SQLite is updated first (sync), then MongoDB is
 *     upserted fire-and-forget (async, never blocks the trading loop).
 *   - On startup: MongoDB state is loaded back into SQLite before
 *     any trading resumes, so restarts pick up exactly where they left off.
 */

'use strict';

const { MongoClient } = require('mongodb');

let mongoDB = null;

// ─── Connect ──────────────────────────────────────────────
async function connectMongo(uri) {
    if (!uri) {
        console.warn('[Mongo] MONGODB_URI not set — persistence disabled. State will be lost on restart.');
        return;
    }
    try {
        const client = new MongoClient(uri, { serverSelectionTimeoutMS: 5000 });
        await client.connect();
        mongoDB = client.db();  // uses the database name from the connection string URI

        // Ensure symbol is always indexed for fast upserts
        await mongoDB.collection('active_trades').createIndex({ symbol: 1 }, { unique: true });
        await mongoDB.collection('manual_legs').createIndex({ symbol: 1 }, { unique: true });
        await mongoDB.collection('pump_dump_trades').createIndex({ symbol: 1 }, { unique: true });

        console.log('[Mongo] Connected. Persistence enabled.');
    } catch (err) {
        console.error('[Mongo] Connection failed — persistence disabled:', err.message);
        mongoDB = null;
    }
}

// ─── Restore ──────────────────────────────────────────────
// Reads all documents from MongoDB and writes them into the local SQLite DB.
// Called once at startup before any trading logic runs.
async function restoreToSQLite(db) {
    if (!mongoDB) return;

    try {
        // active_trades
        const activeTrades = await mongoDB.collection('active_trades').find({}).toArray();
        for (const r of activeTrades) {
            db.prepare('INSERT OR REPLACE INTO active_trades (symbol, status, created_at) VALUES (?, ?, ?)')
                .run(r.symbol, r.status, r.created_at);
        }
        if (activeTrades.length) console.log(`[Mongo] Restored ${activeTrades.length} active_trade(s).`);

        // manual_legs
        const manualLegs = await mongoDB.collection('manual_legs').find({}).toArray();
        for (const r of manualLegs) {
            db.prepare('INSERT OR REPLACE INTO manual_legs (symbol, leg_direction, open_price, size, hedged, tp_order_id) VALUES (?, ?, ?, ?, ?, ?)')
                .run(r.symbol, r.leg_direction, r.open_price, r.size, r.hedged, r.tp_order_id);
        }
        if (manualLegs.length) console.log(`[Mongo] Restored ${manualLegs.length} manual_leg(s).`);

        // pump_dump_trades
        const pdTrades = await mongoDB.collection('pump_dump_trades').find({}).toArray();
        for (const r of pdTrades) {
            db.prepare(`
                INSERT OR REPLACE INTO pump_dump_trades
                (symbol, long_entry, short_entry, qty, trade_qty, long_sl, short_sl,
                 status, trail_direction, trail_distance, trail_peak, trail_sl,
                 cut_pnl, trail_pnl, total_pnl, close_price, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            `).run(
                r.symbol, r.long_entry, r.short_entry, r.qty, r.trade_qty,
                r.long_sl, r.short_sl, r.status, r.trail_direction, r.trail_distance,
                r.trail_peak, r.trail_sl, r.cut_pnl, r.trail_pnl, r.total_pnl,
                r.close_price, r.created_at
            );
        }
        if (pdTrades.length) console.log(`[Mongo] Restored ${pdTrades.length} pump_dump_trade(s).`);

    } catch (err) {
        console.error('[Mongo] Restore failed:', err.message);
    }
}

// ─── Sync helpers (fire-and-forget) ──────────────────────
// These are called after every SQLite write. They never throw or block.

/** Upsert a full row into a MongoDB collection, keyed on symbol. */
function syncDoc(collection, doc) {
    if (!mongoDB || !doc?.symbol) return;
    mongoDB.collection(collection)
        .replaceOne({ symbol: doc.symbol }, { ...doc }, { upsert: true })
        .catch(err => console.error(`[Mongo] Sync '${collection}' error:`, err.message));
}

/** Delete a document from a MongoDB collection by symbol. */
function deleteDoc(collection, symbol) {
    if (!mongoDB || !symbol) return;
    mongoDB.collection(collection)
        .deleteOne({ symbol })
        .catch(err => console.error(`[Mongo] Delete '${collection}' error:`, err.message));
}

/**
 * Convenience: re-read the latest row from SQLite and sync it to MongoDB.
 * Use this after any UPDATE so we always push the full current state.
 */
function syncFromSQLite(db, collection, symbol) {
    if (!mongoDB || !symbol) return;
    let row;
    try {
        row = db.prepare(`SELECT * FROM ${collection} WHERE symbol = ?`).get(symbol);
    } catch { return; }
    if (row) syncDoc(collection, row);
    else     deleteDoc(collection, symbol);
}

module.exports = { connectMongo, restoreToSQLite, syncDoc, deleteDoc, syncFromSQLite };
