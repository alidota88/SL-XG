# main.py
import os
import time
import schedule
import requests
import pandas as pd
from datetime import datetime
from database import init_db
from db_manager import get_data, save_data
from strategy import run_strategy

# 配置信息
TG_TOKEN = os.getenv("TG_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")

def send_telegram(message: str):
    """推送消息到 Telegram"""
    if not TG_TOKEN or not TG_CHAT_ID:
        print("⚠️ [Telegram] Token missing.")
        return
    
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"❌ [Telegram] Error: {e}")

def job_daily_selection():
    """
    【核心任务】每日执行的选股流程
    """
    print(f"⏰ [Job] Starting daily task at {datetime.now()}...")
    
    # 1. (重要) 这里必须接入真实数据源
    # 比如：tushare_fetcher.fetch_today_data()
    # 目前只是演示，如果没有数据，后面什么都做不了
    
    # 2. 从数据库读取数据
    df = get_data(n_days=150)
    
    if df.empty:
        print("⚠️ [Job] No data found.")
        send_telegram("⚠️ **机器人警报**\n\n数据库为空，无法执行选股。\n请检查数据获取模块。")
        return

    # 3. 运行策略
    results = run_strategy(df)
    
    # 4. 推送结果
    current_date = datetime.now().strftime("%Y-%m-%d")
    if not results.empty:
        msg = [f"🤖 **选股日报 ({current_date})**", "---"]
        for _, row in results.iterrows():
            msg.append(f"✅ `{row['ts_code']}` | 收盘: {row['close']}")
        msg.append("\n⚠️ *入市需谨慎*")
        send_telegram("\n".join(msg))
    else:
        print("ℹ️ [Job] No stocks selected.")
        send_telegram(f"🤖 **选股日报 ({current_date})**\n\n今日无标的入选。")
    
    print("✅ [Job] Task finished.")

def main():
    print("🚀 [System] Stock Quant Bot is running in Daemon Mode...")
    
    # 初始化数据库
    init_db()
    
    # 发送一条启动通知，确认服务重启成功
    send_telegram("🚀 **机器人已上线**\n正在等待预定时间执行任务...")

    # === 设定定时任务 ===
    # Railway 服务器通常是 UTC 时间。
    # 北京时间 15:30 = UTC 07:30
    # 北京时间 18:00 = UTC 10:00
    
    # 设定每天 UTC 07:30 (北京 15:30) 执行
    schedule.every().day.at("07:30").do(job_daily_selection)
    
    # 如果你想测试，可以把下面这行注释取消，每 2 分钟跑一次（测试完记得注释掉！）
    # schedule.every(2).minutes.do(job_daily_selection)

    print("🕒 [System] Scheduler is active. Waiting for next run...")

    # === 死循环：保持程序一直活着 ===
    while True:
        schedule.run_pending()
        time.sleep(60) # 每分钟检查一次，节省 CPU

if __name__ == "__main__":
    main()
