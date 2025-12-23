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

# 环境变量读取
TG_TOKEN = os.getenv("TG_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")

def send_telegram(message):
    """发送消息到 Telegram"""
    if not TG_TOKEN or not TG_CHAT_ID: return
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        # 使用 Markdown 格式发送，注意字符转义
        requests.post(url, json={"chat_id": TG_CHAT_ID, "text": message, "parse_mode": "Markdown"}, timeout=10)
    except Exception as e:
        print(f"⚠️ Telegram send failed: {e}")

def execute_logic(is_test=False):
    print("------------------------------------------------")
    print(f"🔥 [Execution] Starting Shrink Breakout Strategy... (Test Mode: {is_test})")
    
    # 1. 尝试回补数据
    # 策略需要回溯20天计算缩量，且用到60日均线，所以200天数据回补是安全的
    print("🛡️ Verifying data integrity...")
    try:
        backfill_data(lookback_days=200)
    except Exception as e:
        print(f"⚠️ Backfill failed/interrupted: {e}")
        print("⚠️ Proceeding with existing data...")

    # 2. 读取数据
    row_count = check_data_count()
    print(f"📉 Loading data... (Total Rows: {row_count})")
    
    if row_count < 10000:
        err_msg = "❌ 错误：数据库数据量过少，无法运行策略。请检查 Tushare 连接。"
        print(err_msg)
        send_telegram(err_msg)
        return

    # 获取足够长的数据以计算 MA60 和 历史缩量信号
    df = get_data(n_days=250)
    
    # 3. 运行【极致缩量起爆】策略
    print("🧠 Calculating Breakout Scores...")
    results = run_strategy(df)
    
    # 4. 发送结果
    date_str = datetime.now().strftime("%Y-%m-%d")
    
    if not results.empty:
        # 只取前 10 名
        top = results.head(10)
        
        # 构造消息头部
        msg = [f"🚀 **缩量后起爆精选** ({date_str})", "---"]
        msg.append(f"📊 策略：历史极致缩量(90%) + 今日放量大阳")
        msg.append(f"✅ 入选库：{len(results)} 只\n")
        
        for i, (_, row) in enumerate(top.iterrows()):
            rank = i + 1
            # 排名图标
            icon = "🥇" if i==0 else "🥈" if i==1 else "🥉" if i==2 else f"{rank}."
            
            # 构造单行信息
            # 格式：排名 代码 价格 (涨幅)
            #      理由...
            line = (
                f"{icon} `{row['ts_code']}` 💰{row['close']:.2f} (**+{row['pct_change']:.2f}%**)\n"
                f"   📝 {row['reason']}\n"
            )
            msg.append(line)
        
        full_msg = "\n".join(msg)
        send_telegram(full_msg)
        print(f"✅ Result sent. Top {len(top)} stocks selected.")
    else:
        # 如果没有股票入选
        msg = (
            f"🚀 **缩量后起爆选股** ({date_str})\n\n"
            "今日无股票入选。\n"
            "可能原因：\n"
            "1. 市场整体低迷，无大阳线启动。\n"
            "2. 前期无满足条件的极致缩量信号。"
        )
        send_telegram(msg)
        print("✅ Strategy finished. No stocks qualified.")
    print("------------------------------------------------")

def main():
    print("🚀 System Starting...")
    init_db()
    
    # 程序启动时立即运行一次，方便调试
    try:
        execute_logic(is_test=True)
    except Exception as e:
        print(f"❌ Critical Error: {e}")
        send_telegram(f"❌ 程序运行报错: {e}")

    # 定时任务：每个交易日收盘后 16:30 运行
    schedule.every().day.at("16:30").do(execute_logic)
    
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()
