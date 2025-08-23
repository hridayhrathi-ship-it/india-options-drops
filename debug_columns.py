# debug_columns.py

from datetime import datetime
from dateutil import tz
from Derivatives.NSE import NSE as DerivNSE

# Initialize
d = DerivNSE()
expiry_list = d.get_options_expiry("NIFTY", is_index=True)
expiry_dt = expiry_list[0] if isinstance(expiry_list, list) else expiry_list

# Fetch option chain DataFrame
oc_df = d.get_option_chain("NIFTY", is_index=True, expiry=expiry_dt)

# Print actual columns
print(oc_df.columns.tolist())
