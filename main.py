# main.py
import os
import time
import schedule
import requests
import pandas as pd
from datetime import datetime
from database import init_db
from db_manager import get_data
# 引入新策略
from strategy import run_strategy
# 引入数据获取
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
    【核心任务】每日数据更新 + 选股 (v2.0)
    """
    start_time = datetime.now()
    print(f"⏰ [Job] v2.0 Strategy Task started at {start_time}...")
    
    # 1. 确保数据是最新的
    # 生产环境建议回补 5-7 天，防止假期或遗漏
    print("🔄 [Job] Fetching latest data...")
    backfill_data(lookback_days=5) 
    
    # 2. 从数据库读取数据 
    # v2.0 策略计算 60日线和斜率，至少需要 70-100 天数据
    print("📉 [Job] Loading data from DB...")
    df = get_data(n_days=150)
    
    if df.empty:
        send_telegram("⚠️ **运行失败**\n\n数据库为空，无法运行策略。")
        return

    # 3. 运行 v2.0 策略
    results = run_strategy(df)
    
    # 4. 推送结果 (优化版排版)
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    if not results.empty:
        # 选取前 15 名
        top_results = results.head(15)
        
        msg = [f"🤖 **量化选股日报 v2.0**", f"📅 {current_date}", "---"]
        msg.append(f"🔍 策略：60日线潜伏 + 智能评分")
        msg.append(f"💎 共入选 {len(results)} 只 (按评分排序)：\n")
        
        for _, row in top_results.iterrows():
            code = row['ts_code']
            # 将代码格式化，去掉后缀以便阅读 (可选)
            simple_code = code.split('.')[0]
            price = row['close']
            score = row['score'] # 0-5分
            reason = row['reason']
            
            # 使用 Emoji 代表分数
            stars = "⭐" * score if score > 0 else "⚪"
            
            line = (
                f"`{code}` | 💰{price}\n"
                f"{stars} {reason}\n"
            )
            msg.append(line)
            
        msg.append("\n⚠️ *投资有风险，决策需自主*")
        send_telegram("\n".join(msg))
    else:
        print("ℹ️ [Job] No stocks selected.")
        send_telegram(f"🤖 **量化选股日报 v2.0**\n📅 {current_date}\n\n今日无符合严选条件的标的。\n(空仓也是一种策略 🧘‍♂️)")
    
    duration = datetime.now() - start_time
    print(f"✅ [Job] Task finished in {duration}.")

def main():
    print("🚀 [System] Stock Quant Bot v2.0 is starting...")
    
    # 1. 初始化数据库
    init_db()
    
    # 2. 启动时自检
    print("🔄 [System] Performing startup checks...")
    # 生产环境为了快速启动，这里只回补少量天数，或注释掉
    backfill_data(lookback_days=7) 
    
    send_telegram("🚀 **机器人 v2.0 已上线**\n策略引擎已升级，等待每日任务...")

    # ==========================================
    # 🧪 测试模式：取消下面注释可立即运行一次
    # ==========================================
    # print("🔥 [Test] Running immediate strategy check...")
    # job_daily_selection()

    # === 设定定时任务 ===
    # 北京时间 16:30 (收盘后) = UTC 08:30
    schedule.every().day.at("08:30").do(job_daily_selection)
    
    print("🕒 [System] Scheduler is active (Daily at 08:30 UTC).")

    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()
