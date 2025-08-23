# fetch_and_store.py

from datetime import datetime
from dateutil import tz
import pandas as pd
from sqlalchemy import create_engine
from Derivatives.NSE import NSE as DerivNSE
from Derivatives.Sensibull import Sensibull
from Fundamentals import MoneyControl
# Technical.NSE import no longer needed for VIX
# from Technical.NSE import NSE as TechNSE  

# --- setup
engine = create_engine("sqlite:///data/trading.db", echo=False)
IST = tz.gettz("Asia/Kolkata")


def now_ist_str():
    return datetime.now(tz=IST).strftime("%Y-%m-%d %H:%M:%S")


def fetch_option_chain_and_pcr(underlying: str, is_index=True):
    """
    Fetch option chain and PCR for the given index.
    """
    d = DerivNSE()

    # Get next expiry
    expiry_list = d.get_options_expiry(underlying, is_index=is_index)
    expiry_dt = expiry_list[0] if isinstance(expiry_list, list) else expiry_list
    expiry_str = expiry_dt.strftime("%Y-%m-%d")

    # Option chain DataFrame
    oc_df = d.get_option_chain(underlying, is_index=is_index, expiry=expiry_dt)

    # Keep only these columns
    keep_cols = [
        'CE_strikePrice', 'CE_lastPrice', 'CE_impliedVolatility', 'CE_totalTradedVolume',
        'CE_openInterest', 'CE_changeinOpenInterest',
        'PE_strikePrice', 'PE_lastPrice', 'PE_impliedVolatility', 'PE_totalTradedVolume',
        'PE_openInterest', 'PE_changeinOpenInterest'
    ]
    oc_df = oc_df[[c for c in keep_cols if c in oc_df.columns]].copy()

    # Prepare CE
    ce = oc_df[[
        'CE_strikePrice', 'CE_lastPrice', 'CE_impliedVolatility',
        'CE_totalTradedVolume', 'CE_openInterest', 'CE_changeinOpenInterest'
    ]].copy()
    ce.columns = ['strike', 'ltp', 'iv', 'volume', 'oi', 'oi_change']
    ce['option_type'] = 'CE'

    # Prepare PE
    pe = oc_df[[
        'PE_strikePrice', 'PE_lastPrice', 'PE_impliedVolatility',
        'PE_totalTradedVolume', 'PE_openInterest', 'PE_changeinOpenInterest'
    ]].copy()
    pe.columns = ['strike', 'ltp', 'iv', 'volume', 'oi', 'oi_change']
    pe['option_type'] = 'PE'

    # Combine and add metadata
    out = pd.concat([ce, pe], ignore_index=True)
    out['underlying'] = underlying
    out['expiry'] = expiry_str
    out['as_of'] = now_ist_str()

    out.to_sql("option_chain", engine, if_exists="append", index=False)

    # PCR (OI and Volume)
    pcr_oi = d.get_pcr(underlying, is_index=is_index, on_field='OI', expiry=expiry_dt)
    pcr_vol = d.get_pcr(underlying, is_index=is_index, on_field='VOLUME', expiry=expiry_dt)
    pcr_df = pd.DataFrame([{
        'as_of': now_ist_str(),
        'underlying': underlying,
        'expiry': expiry_str,
        'pcr_oi': pcr_oi,
        'pcr_volume': pcr_vol
    }])
    pcr_df.to_sql("pcr", engine, if_exists="append", index=False)


def fetch_india_vix():
    """
    Fetch India VIX from MoneyControl fundamentals.
    """
    try:
        mc = MoneyControl()
        vix_df = mc.get_india_vix(interval='1')  # '1' for daily
        # Rename columns and convert timestamp
        vix_df = vix_df.rename(columns={
            't': 'timestamp_unix',
            'o': 'open',
            'h': 'high',
            'l': 'low',
            'c': 'close',
            'v': 'volume'
        })
        # Convert Unix timestamp to IST datetime string
        vix_df['timestamp'] = pd.to_datetime(
            vix_df['timestamp_unix'], unit='s', utc=True
        ).dt.tz_convert('Asia/Kolkata').dt.strftime('%Y-%m-%d %H:%M:%S')
        vix_df['as_of'] = now_ist_str()
        # Store into SQLite
        vix_df[['timestamp', 'open', 'high', 'low', 'close', 'volume', 'as_of']].to_sql(
            "india_vix", engine, if_exists="append", index=False
        )
    except Exception as e:
        print("VIX fetch failed:", e)


def fetch_greeks_near_atm(underlying: str):
    """
    Uses Sensibull helper to get a band of strikes around ATM.
    """
    try:
        d = DerivNSE()
        s = Sensibull()

        expiry_list = d.get_options_expiry(underlying, is_index=True)
        expiry_dt = expiry_list[0] if isinstance(expiry_list, list) else expiry_list

        token_info = s.search_token(underlying)
        greeks_df, _ = s.get_options_data_with_greeks(
            ticker_data=token_info,
            num_look_ups_from_atm=10,
            expiry_date=expiry_dt
        )

        cols = ['tradingsymbol', 'strike', 'option_type', 'ltp', 'delta', 'gamma', 'theta', 'vega']
        greeks_df = greeks_df[[c for c in cols if c in greeks_df.columns]].copy()
        greeks_df['underlying'] = underlying
        greeks_df['expiry'] = expiry_dt.strftime("%Y-%m-%d")
        greeks_df['as_of'] = now_ist_str()

        greeks_df.to_sql("greeks_snapshot", engine, if_exists="append", index=False)
    except Exception as e:
        print("Greeks fetch skipped:", e)


if __name__ == "__main__":
    import os
    os.makedirs("data", exist_ok=True)

    for sym in ["NIFTY", "BANKNIFTY"]:
        fetch_option_chain_and_pcr(sym, is_index=True)
        fetch_greeks_near_atm(sym)
        fetch_india_vix()
    print("Done.")
