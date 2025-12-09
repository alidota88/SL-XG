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
    
    row_count = check_data_count()
    print(f"📊 Current DB Rows: {row_count}")
    
    # === 关键修改：为了修复缺失的几天，强制每次启动都检查过去 200 天 ===
    # 之前是行数够了就不检查，现在改为：只要是测试启动，必须检查完整性
    print("🛡️ Verifying data integrity for the last 200 days...")
    backfill_data(lookback_days=200)
    
    # 2. 读取数据
    print("📉 Loading data for strategy...")
    df = get_data(n_days=250)
    
    if df.empty:
        send_telegram("❌ 错误：数据库为空。")
        return

    # 3. 运行策略
    print("🧠 Calculating Strategy...")
    results = run_strategy(df)
    
    # 4. 发送结果
    date_str = datetime.now().strftime("%Y-%m-%d")
    
    if not results.empty:
        # 选出 555 只太多了，说明大盘在底部，策略过滤太松
        # 我们只发前 20 只分数最高的
        top = results.head(20)
        msg = [f"🤖 **量化选股结果** ({date_str})", f"✅ 策略执行成功，共选出 {len(results)} 只", "---"]
        for _, row in top.iterrows():
            line = f"`{row['ts_code']}` 💰{row['close']:.2f}\nℹ️ {row['reason']}"
            msg.append(line)
        
        # 如果选出太多，提示一下
        if len(results) > 20:
            msg.append(f"\n...以及其他 {len(results)-20} 只")
            
        send_telegram("\n".join(msg))
        print(f"✅ Result sent. Selected {len(results)} stocks.")
    else:
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
