# debug_vix.py

from Fundamentals import MoneyControl
from datetime import datetime
from dateutil import tz

IST = tz.gettz("Asia/Kolkata")
def now_ist_str():
    return datetime.now(tz=IST).strftime("%Y-%m-%d %H:%M:%S")

mc = MoneyControl()
vix_df = mc.get_india_vix(interval='1')  # daily
print("Columns from MoneyControl VIX:", vix_df.columns.tolist())
print(vix_df.head())
