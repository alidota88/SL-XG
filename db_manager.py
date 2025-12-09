# db_manager.py
import pandas as pd
from sqlalchemy.dialects.postgresql import insert
from database import engine

def insert_on_conflict_nothing(table, conn, keys, data_iter):
    """
    PostgreSQL 专用插入方法：遇到主键冲突（重复数据）时自动忽略，不报错
    """
    data = [dict(zip(keys, row)) for row in data_iter]
    if not data:
        return

    # 构造插入语句
    stmt = insert(table.table).values(data)
    
    # 冲突处理：DO NOTHING
    stmt = stmt.on_conflict_do_nothing(index_elements=['ts_code', 'trade_date'])
    
    # 执行
    conn.execute(stmt)

def save_data(df: pd.DataFrame):
    """
    保存数据到数据库（防崩溃版）
    """
    if df.empty:
        return

    try:
        # 使用自定义的 method 防止重复报错
        df.to_sql(
            'stock_daily', 
            engine, 
            if_exists='append', 
            index=False, 
            chunksize=1000, 
            method=insert_on_conflict_nothing 
        )
        print(f"✅ [DB Manager] Processed {len(df)} rows (duplicates ignored).")
    except Exception as e:
        print(f"❌ [DB Manager] Save failed: {e}")

def get_data(n_days: int = 100) -> pd.DataFrame:
    """读取数据"""
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
        print(f"❌ [DB Manager] Load failed: {e}")
        return pd.DataFrame()
