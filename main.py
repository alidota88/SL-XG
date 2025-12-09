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
    """
    统一的执行逻辑
    """
    print("------------------------------------------------")
    print(f"🔥 [Execution] Starting Logic... (Test Mode: {is_test})")
    
    # 1. 检查数据量，决定是否需要回补
    row_count = check_data_count()
    print(f"📊 Current DB Rows: {row_count}")
    
    # 如果数据量少于 10万行（A股一天约5000只，200天约100万行），说明数据严重缺失
    # 即使是测试，没有数据策略也跑不通
    if row_count < 10000:
        print("⚠️ Data insufficient. Forcing 200-day backfill...")
        send_telegram("🔄 数据不足，开始强制回补 200 天历史行情，请耐心等待...")
        backfill_data(lookback_days=200)
    else:
        # 正常日常更新，只补 5 天
        print("✅ Data seems sufficient. Running daily update...")
        backfill_data(lookback_days=5)

    # 2. 读取数据 (计算60日线必须足够长)
    print("📉 Loading data for strategy...")
    df = get_data(n_days=250)
    
    if df.empty:
        print("❌ Error: DB is still empty after backfill.")
        send_telegram("❌ 错误：数据回补失败，数据库为空。")
        return

    # 3. 运行策略
    print("🧠 Calculating Strategy...")
    results = run_strategy(df)
    
    # 4. 发送结果
    date_str = datetime.now().strftime("%Y-%m-%d")
    
    if not results.empty:
        top = results.head(20)
        msg = [f"🤖 **量化选股结果** ({date_str})", f"✅ 策略执行成功，共选出 {len(results)} 只", "---"]
        for _, row in top.iterrows():
            line = f"`{row['ts_code']}` 💰{row['close']:.2f}\nℹ️ {row['reason']}"
            msg.append(line)
        
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
    
    # === 启动时立即运行一次，确保拿到结果 ===
    try:
        execute_logic(is_test=True)
    except Exception as e:
        print(f"❌ Critical Error during startup execution: {e}")
        send_telegram(f"❌ 程序启动运行报错: {e}")

    # === 定时任务 ===
    schedule.every().day.at("08:30").do(execute_logic)
    
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()
