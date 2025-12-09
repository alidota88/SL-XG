# data_fetcher.py
import os
import time
import tushare as ts
import pandas as pd
from datetime import datetime, timedelta
from db_manager import save_data, engine

# 初始化 Tushare
TS_TOKEN = os.getenv("TS_TOKEN")
if TS_TOKEN:
    ts.set_token(TS_TOKEN)
    pro = ts.pro_api()
else:
    print("⚠️ [Data Fetcher] TS_TOKEN not found.")
    pro = None

def fetch_daily_data(trade_date_str: str):
    """
    下载单日数据
    """
    if not pro: return
    
    print(f"⬇️ [Tushare] Fetching {trade_date_str}...", flush=True)
    try:
        # 获取日线
        df = pro.daily(trade_date=trade_date_str)
        if df.empty:
            print(f"   ⚠️ No data for {trade_date_str} (Weekend/Holiday?)")
            return

        # 稍微清洗一下
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        
        # 存入数据库 (db_manager 会自动处理重复，所以这里放心存)
        save_data(df[['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'vol']])
        
    except Exception as e:
        print(f"❌ [Tushare] Error {trade_date_str}: {e}")
        time.sleep(1)

def backfill_data(lookback_days: int = 200):
    """
    【智能回补】
    不依赖数据库的最新日期，而是强制扫描过去 N 天，
    缺哪天就补哪天。
    """
    print(f"🔄 [Data Fetcher] Checking data completeness for last {lookback_days} days...")
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=lookback_days)
    
    # 1. 生成目标日期范围（我们要这期间的所有数据）
    target_dates = pd.date_range(start=start_date, end=end_date).strftime('%Y-%m-%d').tolist()
    
    # 2. 查询数据库里已经有哪些日期了
    try:
        query = f"SELECT DISTINCT trade_date FROM stock_daily WHERE trade_date >= '{start_date.strftime('%Y-%m-%d')}'"
        existing_df = pd.read_sql(query, engine)
        if not existing_df.empty:
            # 转成字符串列表方便比对
            existing_dates = existing_df['trade_date'].astype(str).tolist()
        else:
            existing_dates = []
    except Exception as e:
        print(f"⚠️ DB Read Error: {e}, assuming empty.")
        existing_dates = []

    existing_set = set(exis
