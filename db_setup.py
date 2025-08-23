# db_setup.py
from sqlalchemy import create_engine, text

engine = create_engine("sqlite:///trading.db", echo=False)
with engine.begin() as conn:
    # Create folders table if you want later; for now just core tables:
    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS option_chain (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        as_of TEXT,                -- timestamp
        underlying TEXT,           -- NIFTY or BANKNIFTY
        expiry TEXT,               -- "YYYY-MM-DD"
        strike REAL,
        option_type TEXT,          -- CE/PE
        ltp REAL,
        iv REAL,
        volume REAL,
        oi REAL,
        oi_change REAL,
        bid_qty REAL, bid_price REAL, ask_price REAL, ask_qty REAL
    );
    """))

    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS pcr (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        as_of TEXT,
        underlying TEXT,
        expiry TEXT,
        pcr_oi REAL,
        pcr_volume REAL
    );
    """))

    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS india_vix (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        as_of TEXT,
        open REAL, high REAL, low REAL, close REAL, volume REAL
    );
    """))

    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS greeks_snapshot (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        as_of TEXT,
        underlying TEXT,
        expiry TEXT,
        strike REAL,
        option_type TEXT,
        ltp REAL,
        delta REAL, gamma REAL, theta REAL, vega REAL
    );
    """))

print("DB ready: data/trading.db")
