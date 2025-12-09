# data_fetcher.py
import os
import time
import tushare as ts
import pandas as pd
from datetime import datetime, timedelta
from db_manager import save_data, engine

TS_TOKEN = os.getenv("TS_TOKEN")
if TS_TOKEN:
    ts.set_token(TS_TOKEN)
    pro = ts.pro_api()
else:
    print("⚠️ [Data Fetcher] TS_TOKEN not found.")
    pro = None

def fetch_daily_data(trade_date_str: str):
    """
    下载单日数据 (带重试机制)
    """
    if not pro: return
    
    # === 重试机制 ===
    max_retries = 3
    for attempt in range(max_retries):
        try:
            print(f"⬇️ [Tushare] Fetching {trade_date_str} (Attempt {attempt+1}/{max_retries})...", flush=True)
            
            df = pro.daily(trade_date=trade_date_str)
            
            if df.empty:
                print(f"   ⚠️ No data for {trade_date_str} (Weekend/Holiday?)")
                return # 空数据通常是因为休市，不用重试

            df['trade_date'] = pd.to_datetime(df['trade_date'])
            save_data(df[['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'vol']])
            
            # 成功了就退出循环
            return 
            
        except Exception as e:
            print(f"   ❌ Error on attempt {attempt+1}: {e}")
            if attempt < max_retries - 1:
                print("   ⏳ Waiting 5 seconds to retry...")
                time.sleep(5)
            else:
                print(f"   ❌ Failed to fetch {trade_date_str} after {max_retries} attempts.")

def backfill_data(lookback_days: int = 200):
    """
    智能补漏：只下载数据库里缺少的日期
    """
    print(f"🔄 [Data Fetcher] Checking data completeness for last {lookback_days} days...")
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=lookback_days)
    
    target_dates = pd.date_range(start=start_date, end=end_date).strftime('%Y-%m-%d').tolist()
    
    try:
        query = f"SELECT DISTINCT trade_date FROM stock_daily WHERE trade_date >= '{start_date.strftime('%Y-%m-%d')}'"
        existing_df = pd.read_sql(query, engine)
        if not existing_df.empty:
            existing_dates = existing_df['trade_date'].astype(str).tolist()
        else:
            existing_dates = []
    except Exception as e:
        print(f"⚠️ DB Read Error: {e}, assuming empty.")
        existing_dates = []

    existing_set = set(existing_dates)
    missing_dates = [d for d in target_dates if d not in existing_set]
    missing_dates.sort()
    
    print(f"📊 Analysis: Missing {len(missing_dates)} days out of {lookback_days}.")
    
    if not missing_dates:
        print("✅ Data is complete! No download needed.")
        return

    print(f"⬇️ Starting download for {len(missing_dates)} missing days...")
    
    for date_str in missing_dates:
        ts_date = date_str.replace("-", "")
        fetch_daily_data(ts_date)
        time.sleep(0.5) # 稍微慢一点，保护接口

    print("✅ [Data Fetcher] Backfill complete.")
