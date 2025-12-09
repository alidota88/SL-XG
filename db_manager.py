# db_manager.py
import pandas as pd
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from database import engine

def upsert_method(table, conn, keys, data_iter):
    """
    PostgreSQL 专用：批量写入时，如果主键冲突则忽略（保留已有数据），
    确保程序不会因为某一天数据已存在而崩溃停止。
    """
    data = [dict(zip(keys, row)) for row in data_iter]
    if not data:
        return

    stmt = insert(table.table).values(data)
    # 核心：遇到 (ts_code, trade_date) 冲突，什么都不做，继续下一条
    stmt = stmt.on_conflict_do_nothing(index_elements=['ts_code', 'trade_date'])
    conn.execute(stmt)

def save_data(df: pd.DataFrame):
    """
    保存数据，带防崩溃机制
    """
    if df.empty:
        return

    try:
        df.to_sql(
            'stock_daily', 
            engine, 
            if_exists='append', 
            index=False, 
            chunksize=2000, 
            method=upsert_method # 使用上面的防崩溃方法
        )
        # 不打印啰嗦日志，只报错时说话
    except Exception as e:
        print(f"❌ [DB Error] Save failed: {e}")

def get_data(n_days: int = 250) -> pd.DataFrame:
    """
    读取足够长的数据以计算均线
    """
    # 强制取最近 250 天，保证 MA60, MA120 都能算出来
    query = f"""
    SELECT * FROM stock_daily 
    WHERE trade_date >= current_date - INTERVAL '{n_days} days'
    ORDER BY ts_code, trade_date ASC
    """
    try:
        df = pd.read_sql(query, engine)
        if not df.empty:
            df['trade_date'] = pd.to_datetime(df['trade_date'])
        return df
    except Exception as e:
        print(f"❌ [DB Error] Load failed: {e}")
        return pd.DataFrame()

def check_data_count():
    """查看数据库现在到底有多少行，心中有数"""
    try:
        with engine.connect() as conn:
            cnt = conn.execute(text("SELECT count(*) FROM stock_daily")).scalar()
            return cnt
    except:
        return 0
