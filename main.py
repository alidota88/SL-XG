# main.py
import os
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
    """
    推送消息到 Telegram
    """
    if not TG_TOKEN or not TG_CHAT_ID:
        print("⚠️ [Telegram] Token or Chat ID missing. Skipping notification.")
        return

    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload = {
        "chat_id": TG_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    
    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code == 200:
            print("✅ [Telegram] Message sent successfully.")
        else:
            print(f"❌ [Telegram] Failed to send: {response.text}")
    except Exception as e:
        print(f"❌ [Telegram] Connection error: {e}")

def mock_data_loader():
    """
    【架构师注】
    实际部署时，你需要在这里接入 Tushare/Baostock/Yahoo财经 等 API。
    为了演示，这里留空，假设数据库里已经有数据了。
    如果数据库为空，你需要先编写一个脚本把历史数据存入数据库。
    """
    print("ℹ️ [System] Assuming data exists in DB. Skipping external fetch.")
    # 示例：
    # df = tushare_api.get_daily(...)
    # save_data(df)

def main():
    print("🚀 [System] Stock Quant Bot starting...")
    
    # 1. 初始化数据库
    init_db()

    # 2. (可选) 获取最新行情并存库
    # 这一步通常通过定时任务(Cron)调用外部API完成
    mock_data_loader()

    # 3. 从数据库读取最近 150 天数据 (预留 Buffer 计算 60日均线)
    df = get_data(n_days=150)

    if df.empty:
        print("⚠️ [System] No data found in database. Exiting.")
        send_telegram("⚠️ 机器人运行警告：数据库中没有数据，请检查数据源。")
        return

    # 4. 运行策略
    results = run_strategy(df)

    # 5. 生成报告并推送
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    if not results.empty:
        msg_lines = [f"🤖 **选股日报 ({current_date})**", "---"]
        msg_lines.append(f"策略：60日线下+极致缩量+静默期")
        msg_lines.append(f"共选出 {len(results)} 只标的：\n")
        
        for _, row in results.iterrows():
            line = (
                f"Checking `{row['ts_code']}`:\n"
                f"💰 收盘: {row['close']:.2f}\n"
                f"📉 波动: {row['amp_mean15']:.2%}\n"
            )
            msg_lines.append(line)
        
        msg_lines.append("\n⚠️ *投资有风险，入市需谨慎*")
        send_telegram("\n".join(msg_lines))
    else:
        print("ℹ️ [System] No stocks selected today.")
        # 可选：如果没选出股票也通知一下，确认机器人还活着
        send_telegram(f"🤖 **选股日报 ({current_date})**\n\n今日无符合策略条件的标的。")

if __name__ == "__main__":
    main()
