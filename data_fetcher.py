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
    print("⚠️ [Data Fetcher] TS_TOKEN not found. Data fetching will fail.")
    pro = None

def fetch_daily_data(trade_date: str):
    """
    获取指定日期的全市场行情
    :param trade_date: 格式 'YYYYMMDD'
    """
    if not pro:
        return
    
    print(f"⬇️ [Tushare] Fetching data for {trade_date}...")
    try:
        # 获取日线行情
        df = pro.daily(trade_date=trade_date)
        
        if df.empty:
            print(f"⚠️ [Tushare] No data for {trade_date} (Holiday?).")
            return

        # 数据清洗：重命名列以匹配我们的数据库模型
        # Tushare 返回: ts_code, trade_date, open, high, low, close, pre_close, change, pct_chg, vol, amount
        # 我们的数据库: ts_code, trade_date, open, high, low, close, vol
        
        # 转换日期格式 YYYYMMDD -> YYYY-MM-DD
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        
        # 保存入库
        save_data(df[['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'vol']])
        
    except Exception as e:
        print(f"❌ [Tushare] Error fetching {trade_date}: {e}")
        # 遇到错误休息一下，防止被封 IP
        time.sleep(1)

def backfill_data(lookback_days: int = 100):
    """
    数据回补：检查并下载过去 N 天的数据
    """
    print(f"🔄 [Data Fetcher] Starting backfill for last {lookback_days} days...")
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=lookback_days)
    
    # 生成日期序列
    date_range = pd.date_range(start=start_date, end=end_date)
    
    # 获取数据库里已有的最新日期，避免重复下载
    try:
        query = "SELECT MAX(trade_date) FROM stock_daily"
        last_db_date = pd.read_sql(query, engine).iloc[0, 0]
        if last_db_date:
             # 如果是 date 类型，转为 datetime
            last_db_date = pd.to_datetime(last_db_date)
            print(f"ℹ️ [Data Fetcher] Database updated until: {last_db_date.date()}")
        else:
            print("ℹ️ [Data Fetcher] Database is empty.")
            last_db_date = pd.to_datetime("2000-01-01") # 极早的时间
    except Exception:
        last_db_date = pd.to_datetime("2000-01-01")

    count = 0
    for date in date_range:
        # 如果该日期比数据库最新日期还早，跳过
        if date <= last_db_date:
            continue
            
        date_str = date.strftime('%Y%m%d')
        fetch_daily_data(date_str)
        count += 1
        
        # Tushare 限制每分钟访问次数，这里稍微 sleep 一下比较安全
        # 2000积分通常每分钟允许 500-800 次，非常充裕，但加上 sleep 0.3 更稳健
        time.sleep(0.3) 

    print(f"✅ [Data Fetcher] Backfill complete. Downloaded {count} days.")
