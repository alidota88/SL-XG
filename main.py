import os
import logging
import pandas as pd
import pytz
import asyncio
from datetime import time, datetime
from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler

# 引入你的原有逻辑
from database import init_db
from db_manager import get_data, check_data_count
from strategy import run_strategy
from data_fetcher import backfill_data

# === 配置 ===
TG_TOKEN = os.getenv("TG_TOKEN")
# 你的 Chat ID，用于鉴权，防止陌生人操作你的机器人
# 如果不知道，可以先设为 None，运行并在TG发 /start，控制台会打印你的 Chat ID
ALLOWED_CHAT_ID = os.getenv("TG_CHAT_ID") 
RESULTS_DIR = "results"

# 设置日志
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

async def check_auth(update: Update):
    """权限检查"""
    if not ALLOWED_CHAT_ID:
        return True # 如果没设 ID，默认允许（建议设置）
    
    user_id = str(update.effective_chat.id)
    if user_id != str(ALLOWED_CHAT_ID):
        await update.message.reply_text(f"⛔ 无权访问。你的 ID: {user_id}")
        return False
    return True

def save_results_to_csv(df):
    """保存 CSV 并返回文件名"""
    if df.empty: return None
    if not os.path.exists(RESULTS_DIR):
        os.makedirs(RESULTS_DIR)
        
    date_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"{RESULTS_DIR}/selection_{date_str}.csv"
    
    # 保存关键列
    cols = ['ts_code', 'trade_date', 'close', 'pct_change', '总分', 'reason']
    save_df = df[cols].copy() if set(cols).issubset(df.columns) else df.copy()
    save_df.to_csv(filename, index=False, encoding='utf-8-sig')
    return filename

async def core_logic(context: ContextTypes.DEFAULT_TYPE, chat_id: str, manual: bool = False):
    """核心选股逻辑 (复用)"""
    mode_text = "手动指令" if manual else "定时任务"
    await context.bot.send_message(chat_id=chat_id, text=f"🔥 [{mode_text}] 正在启动极致缩量策略...")
    
    # 1. 运行数据回补（放在线程池中以免阻塞 Bot）
    try:
        await context.bot.send_message(chat_id=chat_id, text="🛡️ 正在检查数据完整性...")
        loop = asyncio.get_running_loop()
        # 将耗时的同步操作放到 executor 中运行
        await loop.run_in_executor(None, backfill_data, 200)
    except Exception as e:
        await context.bot.send_message(chat_id=chat_id, text=f"⚠️ 数据回补出现警告: {e}")

    # 2. 获取数据
    row_count = check_data_count()
    if row_count < 10000:
        await context.bot.send_message(chat_id=chat_id, text="❌ 错误：数据库数据太少，请检查 Tushare。")
        return

    # 3. 运行策略
    df = get_data(n_days=250)
    await context.bot.send_message(chat_id=chat_id, text="🧠 正在计算策略得分...")
    
    results = run_strategy(df)
    
    date_str = datetime.now().strftime("%Y-%m-%d")

    # 4. 结果处理
    if not results.empty:
        # 保存文件
        csv_path = save_results_to_csv(results)
        
        # 构造文本消息
        top = results.head(10)
        msg = [f"🚀 **缩量后起爆精选** ({date_str})", "---"]
        msg.append(f"📊 模式：{mode_text}")
        msg.append(f"✅ 入选：{len(results)} 只 (已自动存档)\n")
        
        for i, (_, row) in enumerate(top.iterrows()):
            rank = i + 1
            icon = "🥇" if i==0 else "🥈" if i==1 else "🥉" if i==2 else f"{rank}."
            line = (
                f"{icon} `{row['ts_code']}` 💰{row['close']:.2f} (**+{row['pct_change']:.2f}%**)\n"
                f"   📝 {row['reason']}\n"
            )
            msg.append(line)
            
        await context.bot.send_message(chat_id=chat_id, text="\n".join(msg), parse_mode="Markdown")
        
        # 直接发送 CSV 文件给用户
        if csv_path and os.path.exists(csv_path):
            await context.bot.send_document(chat_id=chat_id, document=open(csv_path, 'rb'), filename=os.path.basename(csv_path))
            
    else:
        await context.bot.send_message(chat_id=chat_id, text=f"✅ [{mode_text}] 运行结束，今日无符合条件的股票。")

# === Telegram Command Handlers ===

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"🤖 量化机器人已就绪！\n"
        f"你的 Chat ID: `{update.effective_chat.id}`\n\n"
        f"指令列表：\n"
        f"/run - 立即手动运行选股\n"
        f"/last - 获取最近一次的 CSV 文件\n"
        f"⏰ 自动任务：每天北京时间 16:30",
        parse_mode="Markdown"
    )

async def run_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """手动运行 /run"""
    if not await check_auth(update): return
    chat_id = update.effective_chat.id
    await core_logic(context, chat_id, manual=True)

async def get_last_csv(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """获取最新文件 /last"""
    if not await check_auth(update): return
    
    if not os.path.exists(RESULTS_DIR):
        await update.message.reply_text("📂 暂无结果文件夹。")
        return
        
    files = [os.path.join(RESULTS_DIR, f) for f in os.listdir(RESULTS_DIR) if f.endswith('.csv')]
    if not files:
        await update.message.reply_text("📂 暂无历史结果文件。")
        return
        
    # 找最新的
    latest_file = max(files, key=os.path.getctime)
    await update.message.reply_document(document=open(latest_file, 'rb'), caption="📄 这是最近一次的选股结果。")

async def scheduled_task(context: ContextTypes.DEFAULT_TYPE):
    """定时任务回调"""
    # 从 context.job.chat_id 获取目标 ID，或者直接使用全局配置
    target_id = context.job.chat_id if context.job.chat_id else ALLOWED_CHAT_ID
    if target_id:
        await core_logic(context, target_id, manual=False)
    else:
        print("⚠️ 定时任务触发，但未配置 Chat ID，无法发送。")

def main():
    if not TG_TOKEN:
        print("❌ 请在环境变量设置 TG_TOKEN")
        return

    init_db()
    
    application = ApplicationBuilder().token(TG_TOKEN).build()
    
    # 注册指令
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("run", run_command))
    application.add_handler(CommandHandler("last", get_last_csv))
    
    # 设置定时任务 (每天北京时间 16:30)
    # 无论你的服务器在美国还是哪里，指定 pytz.timezone('Asia/Shanghai') 都能准确定位到北京时间
    if ALLOWED_CHAT_ID:
        beijing_tz = pytz.timezone('Asia/Shanghai')
        # 注意：run_daily 的 time 参数需要是 datetime.time
        run_time = time(hour=16, minute=30, tzinfo=beijing_tz)
        
        application.job_queue.run_daily(
            scheduled_task,
            time=run_time,
            chat_id=ALLOWED_CHAT_ID,
            name="daily_strategy"
        )
        print(f"⏰ 定时任务已设定：每天北京时间 16:30 (发送至 {ALLOWED_CHAT_ID})")
    else:
        print("⚠️ 未设置 TG_CHAT_ID，定时任务无法自动发送消息。请先运行 /start 获取 ID。")
    
    print("🚀 Telegram Bot 启动中... (按 Ctrl+C 停止)")
    application.run_polling()

if __name__ == "__main__":
    main()
