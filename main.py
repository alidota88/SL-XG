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
# 引入我们刚才写好的数据获取模块
from data_fetcher import backfill_data

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
    【核心任务】每日数据更新 + 选股
    """
    print(f"⏰ [Job] Starting execution task at {datetime.now()}...")
    
    # 1. 确保数据是最新的
    # 只是为了测试，我们回补最近 20 天即可，速度快
    print("🔄 [Job] Fetching latest data...")
    backfill_data(lookback_days=20) 
    
    # 2. 从数据库读取数据 (150天用于计算均线)
    print("📉 [Job] Loading data from DB...")
    df = get_data(n_days=150)
    
    if df.empty:
        send_telegram("⚠️ **测试失败**\n\n数据库为空，无法运行策略。\n请检查 Tushare Token 是否配置正确。")
        return

    # 3. 运行策略
    results = run_strategy(df)
    
    # 4. 推送结果
    current_date = datetime.now().strftime("%Y-%m-%d")
    if not results.empty:
        msg = [f"🧪 **测试运行报告 ({current_date})**", "---"]
        msg.append(f"📊 策略：60日线趋势 + 缩量")
        msg.append(f"🎯 选中 {len(results)} 只标的：\n")
        
        # 取前 15 个展示
        top_results = results.head(15) 
        
        for _, row in top_results.iterrows():
            code = row['ts_code']
            price = row['close']
            vol = row['vol']
            msg.append(f"`{code}` | 💰 {price}")
            
        msg.append("\n✅ **系统自检通过！**")
        send_telegram("\n".join(msg))
    else:
        print("ℹ️ [Job] No stocks selected.")
        send_telegram(f"🧪 **测试运行报告 ({current_date})**\n\n系统运行正常。\n今日无符合策略的标的。\n✅ **流程自检通过！**")
    
    print("✅ [Job] Task finished.")

def main():
    print("🚀 [System] Stock Quant Bot is starting...")
    
    # 1. 初始化数据库
    init_db()
    
    # 2. 发送启动通知
    send_telegram("🚀 **机器人正在启动**\n正在进行立即测试，请稍候...")

    # ==========================================
    # 🧪 【关键】启动后立即运行一次，验证全流程
    # ==========================================
    print("🔥 [System] Triggering IMMEDIATE TEST run...")
    try:
        job_daily_selection()
    except Exception as e:
        print(f"❌ [System] Test run failed: {e}")
        send_telegram(f"❌ **测试运行报错**\n\n错误信息：{e}")

    # ==========================================
    # 🕒 设定后续的定时任务 (北京时间 16:30 = UTC 08:30)
    # ==========================================
    schedule.every().day.at("08:30").do(job_daily_selection)
    
    print("🕒 [System] Test complete. Scheduler is active (Daily at 08:30 UTC).")

    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()
