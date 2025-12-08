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
# === 新增引入 ===
from data_fetcher import backfill_data, fetch_daily_data

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
    print(f"⏰ [Job] Starting daily task at {datetime.now()}...")
    
    # 1. 获取今日（或最新交易日）数据
    # 为了保险，我们直接运行一次回补逻辑，它会自动补齐最近缺漏的几天
    # 这样即使昨天机器人挂了，今天也能补回来
    backfill_data(lookback_days=5) 
    
    # 2. 从数据库读取数据 (150天用于计算均线)
    df = get_data(n_days=150)
    
    if df.empty:
        send_telegram("⚠️ **机器人警报**\n\n数据库为空，选股失败。")
        return

    # 3. 运行策略
    results = run_strategy(df)
    
    # 4. 推送结果
    current_date = datetime.now().strftime("%Y-%m-%d")
    if not results.empty:
        msg = [f"🤖 **选股日报 ({current_date})**", "---"]
        msg.append(f"📊 策略：缩量回调 + 60日线支撑")
        msg.append(f"🎯 选中 {len(results)} 只标的：\n")
        
        # 限制消息长度，防止超过 Telegram 限制
        top_results = results.head(20) 
        
        for _, row in top_results.iterrows():
            code = row['ts_code']
            price = row['close']
            vol = row['vol']
            # 这里可以加个链接跳转到财经网站
            link = f"http://quote.eastmoney.com/{'sh' if code.endswith('.SH') else 'sz'}{code[:6]}.html"
            msg.append(f"[{code}]({link}) | 💰 {price}")
            
        if len(results) > 20:
            msg.append(f"\n... 以及其他 {len(results)-20} 只")
            
        msg.append("\n⚠️ *入市需谨慎，仅供参考*")
        send_telegram("\n".join(msg))
    else:
        print("ℹ️ [Job] No stocks selected.")
        send_telegram(f"🤖 **选股日报 ({current_date})**\n\n今日无符合策略的标的。")
    
    print("✅ [Job] Task finished.")

def main():
    print("🚀 [System] Stock Quant Bot is running...")
    
    # 1. 初始化数据库表结构
    init_db()
    
    # 2. 启动时自检：如果是新环境，先下载历史数据
    # 检查过去 100 天的数据，如果缺失会自动补全
    # 第一次运行这步会花几分钟（下载约50万行数据）
    print("🔄 [System] Checking data integrity...")
    backfill_data(lookback_days=100) 
    
    send_telegram("🚀 **机器人已上线**\n历史数据自检完成，等待每日任务...")

    # === 设定定时任务 ===
    # Tushare 数据通常在收盘后 16:00 左右更新稳定
    # 北京时间 16:30 = UTC 08:30
    schedule.every().day.at("08:30").do(job_daily_selection)
    
    print("🕒 [System] Scheduler is active (Daily at 08:30 UTC).")

    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()
