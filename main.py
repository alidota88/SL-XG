import os
import time
import schedule
import requests
import pandas as pd
import argparse  # 用于解析命令行参数
from datetime import datetime
from database import init_db
from db_manager import get_data, check_data_count
from strategy import run_strategy
from data_fetcher import backfill_data

# === 配置部分 ===
TG_TOKEN = os.getenv("TG_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")
RESULTS_DIR = "results"  # 结果保存目录

def send_telegram(message):
    """发送消息到 Telegram"""
    if not TG_TOKEN or not TG_CHAT_ID: return
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TG_CHAT_ID, "text": message, "parse_mode": "Markdown"}, timeout=10)
    except Exception as e:
        print(f"⚠️ Telegram send failed: {e}")

def save_results_to_csv(df, date_str):
    """
    自动保留：将选股结果保存到本地 CSV 文件
    """
    if df.empty: return
    
    if not os.path.exists(RESULTS_DIR):
        os.makedirs(RESULTS_DIR)
        
    filename = f"{RESULTS_DIR}/selection_{date_str}.csv"
    try:
        # 保存关键列，防止文件太乱
        cols = ['ts_code', 'trade_date', 'close', 'pct_change', '总分', 'reason']
        # 如果有其他想看的列也可以加进去
        save_df = df[cols].copy() if set(cols).issubset(df.columns) else df.copy()
        
        save_df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"💾 [Auto Save] 选股结果已自动保留至: {filename}")
    except Exception as e:
        print(f"⚠️ 保存 CSV 失败: {e}")

def execute_logic(manual=False):
    """
    执行核心逻辑
    :param manual: 是否为手动触发（仅影响日志显示）
    """
    mode_str = "手动模式" if manual else "自动调度"
    print("------------------------------------------------")
    print(f"🔥 [Execution] 正在运行策略 ({mode_str})...")
    
    # 1. 数据回补 (确保最近200天数据完整，用于计算缩量)
    print("🛡️ 正在检查数据完整性...")
    try:
        backfill_data(lookback_days=200)
    except Exception as e:
        print(f"⚠️ 数据回补中断: {e}")

    # 2. 读取数据
    row_count = check_data_count()
    print(f"📉 加载数据中... (当前库内共 {row_count} 条)")
    
    if row_count < 10000:
        err_msg = "❌ 错误：数据库数据量过少，无法运行策略。请先运行数据抓取。"
        print(err_msg)
        if not manual: send_telegram(err_msg)
        return

    df = get_data(n_days=250)
    
    # 3. 运行【极致缩量起爆】策略
    print("🧠 正在计算策略得分...")
    results = run_strategy(df)
    
    date_str = datetime.now().strftime("%Y-%m-%d")
    
    # 4. 处理结果
    if not results.empty:
        # === A. 自动保留 (保存到本地) ===
        save_results_to_csv(results, date_str)
        
        # === B. 发送通知 (取前10名) ===
        top = results.head(10)
        
        msg = [f"🚀 **缩量后起爆精选** ({date_str})", "---"]
        msg.append(f"📊 模式：{mode_str}")
        msg.append(f"✅ 入选库：{len(results)} 只 (已自动归档)\n")
        
        for i, (_, row) in enumerate(top.iterrows()):
            rank = i + 1
            icon = "🥇" if i==0 else "🥈" if i==1 else "🥉" if i==2 else f"{rank}."
            line = (
                f"{icon} `{row['ts_code']}` 💰{row['close']:.2f} (**+{row['pct_change']:.2f}%**)\n"
                f"   📝 {row['reason']}\n"
            )
            msg.append(line)
        
        send_telegram("\n".join(msg))
        print(f"✅ 选股完成。已推送 Top {len(top)}，完整列表已保存。")
    else:
        print("✅ 策略运行完成。今日无符合条件的股票。")
        if manual:
            print("💡 提示：手动运行时若无结果，通常是因为今日数据尚未更新或市场无信号。")
            
    print("------------------------------------------------")

def run_schedule():
    print("⏰ 定时任务已启动：将在每天 16:30 自动运行...")
    
    # 设定每天下午 4:30 运行
    schedule.every().day.at("16:30").do(execute_logic, manual=False)
    
    while True:
        schedule.run_pending()
        time.sleep(60)

def main():
    print("🚀 系统启动...")
    init_db()
    
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="量化选股系统控制台")
    parser.add_argument('--run-now', action='store_true', help="启动时立即手动运行一次策略")
    args = parser.parse_args()

    # 如果带有 --run-now 参数，或者直接默认启动时，你想让它先跑一次看结果
    # 这里设置为：默认启动时，先手动跑一次，确保程序没问题，然后进入定时
    # 如果你只想纯定时，可以把下面这行注释掉
    try:
        print("⚡ 正在执行启动自检 (手动选股模式)...")
        execute_logic(manual=True)
    except Exception as e:
        print(f"❌ 启动运行报错: {e}")
        send_telegram(f"❌ 程序启动报错: {e}")

    # 进入死循环等待定时任务
    run_schedule()

if __name__ == "__main__":
    main()
