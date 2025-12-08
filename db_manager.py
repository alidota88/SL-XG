# db_manager.py
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from database import engine

def save_data(df: pd.DataFrame):
    """
    将 DataFrame 保存到数据库
    :param df: 包含行情数据的 DataFrame，列名需与数据库字段对应
    """
    if df.empty:
        print("⚠️ [DB Manager] No data to save.")
        return

    try:
        # 使用 append 模式，如果数据量大建议改用 method='multi' 加速
        # 注意：生产环境通常需要处理主键冲突（Upsert），这里简化为追加
        # 实际使用中请确保传入的数据不包含库中已有的 (code, date)
        df.to_sql('stock_daily', engine, if_exists='append', index=False, chunksize=1000)
        print(f"✅ [DB Manager] Successfully saved {len(df)} rows.")
    except Exception as e:
        print(f"❌ [DB Manager] Save failed: {e}")

def get_data(n_days: int = 100) -> pd.DataFrame:
    """
    获取最近 N 天的数据用于策略计算
    :param n_days: 回溯天数（需预留足够的计算均线的 Buffer）
    """
    # 这里使用简单的日期过滤。实际交易日可能少于自然日，建议取稍微大一点的范围
    # 比如要计算60日均线，最好取最近120个自然日的数据
    
    query = f"""
    SELECT * FROM stock_daily 
    WHERE trade_date >= current_date - INTERVAL '{n_days} days'
    ORDER BY ts_code, trade_date ASC
    """
    
    try:
        df = pd.read_sql(query, engine)
        # 确保日期格式正确
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        print(f"✅ [DB Manager] Loaded {len(df)} rows from database.")
        return df
    except Exception as e:
        print(f"❌ [DB Manager] Load failed: {e}")
        return pd.DataFrame()
