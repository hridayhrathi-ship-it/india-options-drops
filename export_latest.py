# export_latest.py

import pandas as pd
from sqlalchemy import create_engine, text
import os

engine = create_engine("sqlite:///data/trading.db", echo=False)

def export_latest():
    # Create exports folder if it doesn't exist
    os.makedirs("exports", exist_ok=True)
    
    with engine.begin() as conn:
        # Get latest timestamps for each table
        latest_oc = conn.execute(text("SELECT MAX(as_of) FROM option_chain")).scalar()
        latest_pcr = conn.execute(text("SELECT MAX(as_of) FROM pcr")).scalar()
        latest_vix = conn.execute(text("SELECT MAX(as_of) FROM india_vix")).scalar()
        latest_greeks = conn.execute(text("SELECT MAX(as_of) FROM greeks_snapshot")).scalar()

        # Export latest data to CSVs
        if latest_oc:
            oc = pd.read_sql(f"SELECT * FROM option_chain WHERE as_of='{latest_oc}'", engine)
            oc.to_csv("exports/option_chain_latest.csv", index=False)
            print(f"Exported {len(oc)} option chain records")

        if latest_pcr:
            pcr = pd.read_sql(f"SELECT * FROM pcr WHERE as_of='{latest_pcr}'", engine)
            pcr.to_csv("exports/pcr_latest.csv", index=False)
            print(f"Exported {len(pcr)} PCR records")

        if latest_vix:
            vix = pd.read_sql(f"SELECT * FROM india_vix WHERE as_of='{latest_vix}'", engine)
            vix.to_csv("exports/india_vix_latest.csv", index=False)
            print(f"Exported {len(vix)} VIX records")

        # Greeks are optional
        if latest_greeks:
            try:
                greeks = pd.read_sql(f"SELECT * FROM greeks_snapshot WHERE as_of='{latest_greeks}'", engine)
                if not greeks.empty:
                    greeks.to_csv("exports/greeks_latest.csv", index=False)
                    print(f"Exported {len(greeks)} Greeks records")
            except Exception as e:
                print(f"Greeks export skipped: {e}")

    print("All exports completed in /exports folder.")

if __name__ == "__main__":
    export_latest()
