# data_fetcher.py
import os
import time
import tushare as ts
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import text
from db_manager import save_data, engine

# 初始化 Tushare Pro
TUSHARE_TOKEN = os.getenv("TUSHARE_TOKEN")
pro = ts.pro_api(TUSHARE_TOKEN)

def fetch_daily_data(trade_date: str):
    """
    获取指定日期的全市场行情并存入数据库
    :param trade_date: 格式 'YYYYMMDD'
    """
    print(f"📥 [Tushare] Fetching data for {trade_date}...")
    
    try:
        # 1. 获取日线行情 (2000积分用户可以直接拉取全市场)
        # 字段说明: ts_code(代码), trade_date(日期), open, high, low, close, vol(成交量)
        df = pro.daily(trade_date=trade_date)
        
        if df.empty:
            print(f"⚠️ [Tushare] No trading data for {trade_date} (Holiday?).")
            return 0

        # 2. 简单的清洗
        # 我们的数据库字段叫 'vol'，Tushare 返回的也是 'vol'，无需重命名
        # Tushare 的 vol 单位是 "手"，如果要转为 "股" 可以 * 100，这里保持原样即可
        
        # 3. 存入数据库
        save_data(df)
        return len(df)

    except Exception as e:
        print(f"❌ [Tushare] Error fetching {trade_date}: {e}")
        return 0

def backfill_history(start_date: str, end_date: str):
    """
    【初始化专用】补全历史数据
    :param start_date: 'YYYYMMDD'
    :param end_date: 'YYYYMMDD'
    """
    print(f"🔄 [Data Fetcher] Starting backfill from {start_date} to {end_date}...")
    
    # 获取交易日历，只在开盘日抓取
    try:
        cal_df = pro.trade_cal(exchange='', start_date=start_date, end_date
