# main.py
import os
import time
import schedule
import requests
import pandas as pd
from datetime import datetime
from database import init_db
from db_manager import get_data
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

def job_daily_selection():
    print(f"⏰ [Job] Starting Task at {datetime.now()}...")
    
    # 1. 回补数据 (平时补5天即可)
    backfill_data(lookback_days=5)
    
    # 2. 读取 200 天数据 (计算60日线需要)
    print("📉 Reading data...")
    df = get_data(n_days=200)
    
    if df.empty:
        send_telegram("⚠️ 数据库为空，无法运行策略")
        return

    # 3. 运行 v2.0 策略
    print("🧠 Running Strategy v2.0...")
    results = run_strategy(df)
    
    # 4. 推送
    date_str = datetime.now().strftime("%Y-%m-%d")
    if not results.empty:
        top = results.head(15)
        msg = [f"🤖 **选股日报 v2.0** ({date_str})", "---"]
        for _, row in top.iterrows():
            stars = "⭐" * int(row['score'])
            msg.append(f"`{row['ts_code']}` 💰{row['close']}\n{stars} {row['reason']}")
        msg.append("\n⚠️ *仅供参考*")
        send_telegram("\n".join(msg))
    else:
        send_telegram(f"🤖 **选股日报 v2.0** ({date_str})\n\n今日无标的入选。")
    print("✅ Task Finished.")

def main():
    print("🚀 System Starting...")
    init_db()
    
    # === 关键：首次运行下载 200 天数据 ===
    # 只要有了 db_manager.py 的防崩溃补丁，这里重复下载也不会报错
    print("⬇️ Initializing Data (200 days)...")
    backfill_data(lookback_days=200)
    
    send_telegram("🚀 **机器人已重启**\nv2.0策略已加载，数据初始化完成。")

    # === 立即运行一次测试 ===
    print("🔥 Running immediate test...")
    try:
        job_daily_selection()
    except Exception as e:
        print(f"Test run error: {e}")

    # === 定时任务 (北京 16:30 / UTC 08:30) ===
    schedule.every().day.at("08:30").do(job_daily_selection)
    
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()
