# db_manager.py
import pandas as pd
from sqlalchemy.dialects.postgresql import insert
from database import engine

def insert_on_conflict_nothing(table, conn, keys, data_iter):
    """
    自定义的 Pandas to_sql 插入方法
    使用 PostgreSQL 的 ON CONFLICT DO NOTHING 语法
    """
    # 将迭代器转换为字典列表
    data = [dict(zip(keys, row)) for row in data_iter]
    
    if not data:
        return

    # 构建 INSERT 语句
    stmt = insert(table.table).values(data)
    
    # 定义冲突时的动作：什么都不做 (DO NOTHING)
    # 注意：这里假设你的主键是 ts_code 和 trade_date
    stmt = stmt.on_conflict_do_nothing(index_elements=['ts_code', 'trade_date'])
    
    # 执行语句
    conn.execute(stmt)

def save_data(df: pd.DataFrame):
    """
    将 DataFrame 保存到数据库 (自动去重)
    :param df: 包含行情数据的 DataFrame
    """
    if df.empty:
        return

    try:
        # 使用自定义的 method 参数处理冲突
        # chunksize 设置为 1000 防止 SQL 语句过长
        df.to_sql(
            'stock_daily', 
            engine, 
            if_exists='append', 
            index=False, 
            chunksize=1000, 
            method=insert_on_conflict_nothing
        )
        print(f"✅ [DB Manager] Successfully processed {len(df)} rows (duplicates ignored).")
    except Exception as e:
        print(f"❌ [DB Manager] Save failed: {e}")

def get_data(n_days: int = 100) -> pd.DataFrame:
    """
    获取最近 N 天的所有股票数据
    """
    # 增加 LIMIT 防止内存溢出，但对于 100 天数据通常不需要
    query = f"""
    SELECT * FROM stock_daily 
    WHERE trade_date >= current_date - INTERVAL '{n_days} days'
    ORDER BY ts_code, trade_date ASC
    """
    
    try:
        df = pd.read_sql(query, engine)
        if not df.empty:
            df['trade_date'] = pd.to_datetime(df['trade_date'])
        print(f"✅ [DB Manager] Loaded {len(df)} rows from database.")
        return df
    except Exception as e:
        print(f"❌ [DB Manager] Load failed: {e}")
        return pd.DataFrame()
