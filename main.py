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
    
    # 1. 数据完整性检查 (每次启动都强制检查200天，确保无死角)
    print("🛡️ Verifying data integrity for the last 200 days...")
    backfill_data(lookback_days=200)
    
    # 2. 读取数据
    print("📉 Loading data...")
    df = get_data(n_days=250)
    
    if df.empty:
        send_telegram("❌ 错误：数据库为空。")
        return

    # 3. 运行加权评分策略
    print("🧠 Calculating Weighted Scores...")
    results = run_strategy(df)
    
    # 4. 发送结果
    date_str = datetime.now().strftime("%Y-%m-%d")
    
    if not results.empty:
        # 【关键】只取前 10 名
        top = results.head(10)
        
        msg = [f"🏆 **量化选股 TOP 10** ({date_str})", "---"]
        msg.append(f"📊 策略：加权评分 (Flexible版)")
        msg.append(f"✅ 总入选：{len(results)} 只 (分数线 65+)\n")
        
        for i, (_, row) in enumerate(top.iterrows()):
            # Emoji 排名
            rank_icon = "🥇" if i==0 else "🥈" if i==1 else "🥉" if i==2 else f"{i+1}."
            
            line = (
                f"{rank_icon} `{row['ts_code']}` 💰{row['close']:.2f}\n"
                f"   **总分: {row['总分']:.0f}** (核心: {row['核心条件分']})\n"
                f"   📉 量比min: {row['vol_to_min']:.2f} | 偏离: {abs(row['distance_ma60']):.1f}%"
            )
            msg.append(line)
        
        send_telegram("\n".join(msg))
        print(f"✅ Result sent. Top 10 stocks selected.")
    else:
        msg = f"🏆 **量化选股结果** ({date_str})\n\n今日无股票达到及格线 (65分)。\n市场可能处于非缩量期或反弹期。"
        send_telegram(msg)
        print("✅ Strategy finished. No stocks qualified.")
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
