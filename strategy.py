# strategy.py
import pandas as pd
import numpy as np

def run_strategy(df: pd.DataFrame) -> pd.DataFrame:
    """
    执行选股策略：60日线下 + 极致缩量 + 静默期
    """
    if df.empty:
        return pd.DataFrame()

    print("🔄 [Strategy] Running strategy analysis...")

    # 1. 确保数据按代码和日期排序
    df = df.sort_values(by=['ts_code', 'trade_date']).reset_index(drop=True)

    # === 特征计算 (使用 GroupBy 向量化计算) ===
    
    # 计算 60日均线
    df['ma60'] = df.groupby('ts_code')['close'].transform(lambda x: x.rolling(window=60).mean())
    
    # 计算 20日内最低成交量
    df['vol_min20'] = df.groupby('ts_code')['vol'].transform(lambda x: x.rolling(window=20).min())
    
    # 计算 振幅 = (High - Low) / Low (注：也常用 PreClose 计算，这里按需求用 Low)
    df['amp'] = (df['high'] - df['low']) / df['low']
    
    # 计算 15日平均振幅
    df['amp_mean15'] = df.groupby('ts_code')['amp'].transform(lambda x: x.rolling(window=15).mean())

    # 计算 当日涨跌幅 (用于判断是否大跌)
    # 假设跌幅 < 3% 指的是 pct_change > -0.03
    df['pct_chg'] = df.groupby('ts_code')['close'].pct_change()

    # === 筛选逻辑 ===
    # 我们只关心“最新一个交易日”符合条件的股票
    # 获取数据中每个股票的最后一行
    last_date = df['trade_date'].max()
    current_df = df[df['trade_date'] == last_date].copy()

    # 条件 1: 收盘价 < 60日均线
    cond_trend = current_df['close'] < current_df['ma60']

    # 条件 2: 形态 - 收阴线 (Close < Open) 且 跌幅 < 3% (即 pct_chg > -0.03)
    # 注意：如果“跌幅”是指绝对值小于3%，则是 abs(change) < 0.03。这里按常规“没跌太惨”理解。
    cond_shape = (current_df['close'] < current_df['open']) & (current_df['pct_chg'] > -0.03)

    # 条件 3: 量能 - 当日成交量 = 过去20天最低值 (极致缩量)
    # 浮点数比较建议用 np.isclose 或设置极小容差，但整数Vol通常直接比
    cond_vol = current_df['vol'] <= current_df['vol_min20']

    # 条件 4: 波动 - 过去15天平均振幅 < 3.5% (0.035)
    cond_wave = current_df['amp_mean15'] < 0.035

    # 条件 5: 过滤 - 股价 > 3 元
    cond_filter = current_df['close'] > 3.0

    # === 综合筛选 ===
    result = current_df[cond_trend & cond_shape & cond_vol & cond_wave & cond_filter]

    print(f"✅ [Strategy] Found {len(result)} stocks matching criteria.")
    return result[['ts_code', 'trade_date', 'close', 'vol', 'amp_mean15']]
