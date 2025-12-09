# strategy.py
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

def run_strategy(df: pd.DataFrame) -> pd.DataFrame:
    """
    执行 v2.0 选股策略
    """
    if df.empty:
        print("⚠️ [Strategy] Input data is empty.")
        return pd.DataFrame()

    # 1. 字段标准化 (解决 vol 和 volume 的命名混乱)
    if 'vol' in df.columns and 'volume' not in df.columns:
        df['volume'] = df['vol']
    
    required_cols = ['ts_code', 'trade_date', 'close', 'ma_60', 'volume']
    
    # 2. 计算指标
    # 必须先按代码和日期排序，否则滚动计算全是错的
    df = df.sort_values(['ts_code', 'trade_date'])
    
    # 计算 MA60
    df['ma_60'] = df.groupby('ts_code')['close'].transform(lambda x: x.rolling(60).mean())
    
    # 计算 均线斜率 (5日变化)
    df['ma_60_shift'] = df.groupby('ts_code')['ma_60'].shift(5)
    df['ma_60_slope'] = (df['ma_60'] - df['ma_60_shift']) / df['ma_60_shift']
    
    # 计算 20日均量
    df['vol_ma20'] = df.groupby('ts_code')['volume'].transform(lambda x: x.rolling(20).mean())
    
    # 计算 RSI (14日)
    def calc_rsi(series, period=14):
        delta = series.diff()
        gain = (delta.where(delta > 0, 0)).rolling(period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(period).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))
    
    df['rsi'] = df.groupby('ts_code')['close'].transform(lambda x: calc_rsi(x))

    # 3. 截取最新一天的数据进行选股
    # 注意：这里不能只取最后一行，因为不同股票最新日期可能不同（停牌等）
    # 我们取数据集中最大的日期
    last_date = df['trade_date'].max()
    current_df = df[df['trade_date'] == last_date].copy()
    
    if current_df.empty:
        return pd.DataFrame()

    # 4. 筛选逻辑
    # 条件A: 收盘价 < 60日线 (且 MA60 不是空值)
    c_trend = (current_df['close'] < current_df['ma_60']) & (current_df['ma_60'].notnull())
    
    # 条件B: 缩量 (量 < 20日均量 * 0.6)
    c_vol = current_df['volume'] < (current_df['vol_ma20'] * 0.6)
    
    # 条件C: 价格过滤 > 3元
    c_price = current_df['close'] > 3.0
    
    # 条件D: RSI 不高 (小于 50)
    c_rsi = current_df['rsi'] < 50

    # 综合筛选
    final_df = current_df[c_trend & c_vol & c_price & c_rsi].copy()
    
    # 计算评分用于排序 (距离均线越远分越高，RSI越低分越高)
    final_df['score'] = (
        (final_df['ma_60'] - final_df['close']) / final_df['ma_60'] * 100 + 
        (50 - final_df['rsi'])
    )
    
    # 格式化输出
    final_df = final_df.sort_values('score', ascending=False)
    
    # 生成 reason 字段供通知使用
    final_df['reason'] = final_df.apply(
        lambda x: f"离MA60偏离{((x['ma_60']-x['close'])/x['ma_60']*100):.1f}% RSI:{x['rsi']:.1f}", 
        axis=1
    )
    
    return final_df
