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
    print(f"🔥 [Execution] Starting Weighted Strategy... (Test Mode: {is_test})")
    
    # 1. 尝试回补数据（柔性模式）
    print("🛡️ Verifying data integrity...")
    try:
        # 尝试补全 200 天，如果网络不好，报错了也没关系，继续往下走
        backfill_data(lookback_days=200)
    except Exception as e:
        print(f"⚠️ Backfill failed/interrupted: {e}")
        print("⚠️ Proceeding with existing data...")

    # 2. 读取数据
    row_count = check_data_count()
    print(f"📉 Loading data... (Total Rows: {row_count})")
    
    if row_count < 10000:
        send_telegram("❌ 错误：数据库数据量过少，无法运行策略。请检查 Tushare 连接。")
        return

    df = get_data(n_days=250)
    
    # 3. 运行加权评分策略
    print("🧠 Calculating Weighted Scores...")
    results = run_strategy(df)
    
    # 4. 发送前 10 名
    date_str = datetime.now().strftime("%Y-%m-%d")
    
    if not results.empty:
        top = results.head(10)
        
        msg = [f"🏆 **量化选股 TOP 10** ({date_str})", "---"]
        msg.append(f"📊 策略：加权评分 (Flexible版)")
        msg.append(f"✅ 入选库：{len(results)} 只\n")
        
        for i, (_, row) in enumerate(top.iterrows()):
            rank = i + 1
            icon = "🥇" if i==0 else "🥈" if i==1 else "🥉" if i==2 else f"{rank}."
            line = (
                f"{icon} `{row['ts_code']}` 💰{row['close']:.2f}\n"
                f"   **总分: {row['总分']:.0f}** | {row['reason']}\n"
                f"   偏离MA60: {row['distance_ma60']:.1f}%"
            )
            msg.append(line)
        
        send_telegram("\n".join(msg))
        print(f"✅ Result sent. Top 10 stocks selected.")
    else:
        msg = f"🏆 **量化选股结果** ({date_str})\n\n今日无股票达到 60 分。\n市场可能处于非缩量期。"
        send_telegram(msg)
        print("✅ Strategy finished. No stocks qualified.")
    print("------------------------------------------------")

def main():
    print("🚀 System Starting...")
    init_db()
    
    # 立即运行一次
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
