# main.py
import os
import time
import schedule
import requests
import pandas as pd
from datetime import datetime
from database import init_db
from db_manager import get_data, check_data_count
from strategy import run_strategy
from data_fetcher import backfill_data

TG_TOKEN = os.getenv("TG_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")

def send_telegram(message):
    if not TG_TOKEN or not TG_CHAT_ID: return
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TG_CHAT_ID, "text": message, "parse_mode": "Markdown"}, timeout=10)
    except: pass

def execute_logic(is_test=False):
    print("------------------------------------------------")
    print(f"🔥 [Execution] Starting Logic... (Test Mode: {is_test})")
    
    # 1. 检查数据量
    row_count = check_data_count()
    print(f"📊 Current DB Rows: {row_count}")
    
    # 【核心修正】：阈值上调到 50万行 (约等于100天数据)
    # 如果少于50万行，说明历史数据不够计算 MA60，必须强制补下载
    if row_count < 500000:
        print(f"⚠️ Data insufficient ({row_count} < 500,000). Forcing 200-day backfill...")
        send_telegram(f"🔄 检测到历史数据不足 (当前仅{row_count}行)，正在下载近200天行情，耗时较长请耐心等待...")
        
        # 强制回补 200 天
        backfill_data(lookback_days=200)
    else:
        print("✅ Data seems sufficient. Running daily update...")
        # 日常只需补 5 天
        backfill_data(lookback_days=5)

    # 2. 读取数据 (计算60日线必须足够长)
    print("📉 Loading data for strategy...")
    df = get_data(n_days=250)
    
    if df.empty:
        print("❌ Error: DB is empty.")
        send_telegram("❌ 错误：数据库为空。")
        return

    # 3. 运行策略
    print("🧠 Calculating Strategy...")
    results = run_strategy(df)
    
    # 4. 发送结果
    date_str = datetime.now().strftime("%Y-%m-%d")
    
    if not results.empty:
        # 只取前 20 只
        top = results.head(20)
        msg = [f"🤖 **量化选股结果** ({date_str})", f"✅ 策略执行成功，共选出 {len(results)} 只", "---"]
        for _, row in top.iterrows():
            line = f"`{row['ts_code']}` 💰{row['close']:.2f}\nℹ️ {row['reason']}"
            msg.append(line)
        
        send_telegram("\n".join(msg))
        print(f"✅ Result sent. Selected {len(results)} stocks.")
    else:
        # 调试信息：如果没有选出股票，打印一下是因为什么
        print("ℹ️ No stocks selected. Debugging...")
        if 'ma_60' in df.columns:
            valid_ma = df['ma_60'].notnull().sum()
            print(f"   Stocks with valid MA60: {valid_ma} / {len(df)}")
            if valid_ma == 0:
                print("   ❌ CRITICAL: All MA60 are NaN. History data is still too short!")
        
        msg = f"🤖 **量化选股结果** ({date_str})\n\n策略运行正常，但今日无标的满足条件。"
        send_telegram(msg)
        print("✅ Strategy finished. No stocks selected.")
    print("------------------------------------------------")

def main():
    print("🚀 System Starting...")
    init_db()
    
    try:
        execute_logic(is_test=True)
    except Exception as e:
        print(f"❌ Critical Error: {e}")
        send_telegram(f"❌ 程序运行报错: {e}")

    schedule.every().day.at("08:30").do(execute_logic)
    
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()
