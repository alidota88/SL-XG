# strategy.py
import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

class ImprovedStrategy:
    """
    v2.0 改进版策略 (适配版)
    """
    def __init__(self, ma_period=60, min_price=3.0):
        self.ma_period = ma_period
        self.min_price = min_price
        
    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        # 兼容 vol 字段名
        if 'vol' in df.columns and 'volume' not in df.columns:
            df['volume'] = df['vol']

        # 均线
        df['ma_60'] = df['close'].rolling(window=60).mean()
        
        # 均线斜率 (判断趋势是否走平)
        df['ma_60_slope'] = (df['ma_60'] - df['ma_60'].shift(5)) / df['ma_60'].shift(5) * 100
        
        # 量能均线
        df['volume_ma20'] = df['volume'].rolling(window=20).mean()
        df['volume_ma5'] = df['volume'].rolling(window=5).mean()
        df['volume_ratio'] = df['volume'] / df['volume_ma5']
        
        # 振幅 (15日均值)
        df['amplitude'] = (df['high'] - df['low']) / df['low'] * 100
        df['amplitude_ma15'] = df['amplitude'].rolling(window=15).mean()
        
        # RSI (14日)
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))
        
        # 乖离率
        df['distance_from_ma60'] = (df['close'] - df['ma_60']) / df['ma_60'] * 100
        
        return df
    
    def analyze_ticker(self, df: pd.DataFrame):
        """分析单只股票"""
        if len(df) < 70: return None # 数据太少不算
        
        df = self.calculate_indicators(df)
        curr = df.iloc[-1]
        
        # === 核心门槛 ===
        # 1. 股价在60日线下
        if not (curr['close'] < curr['ma_60']): return None
        # 2. 极致缩量 (小于20日均量的60%)
        if not (curr['volume'] < curr['volume_ma20'] * 0.6): return None
        # 3. 价格过滤
        if curr['close'] < self.min_price: return None
        
        # === 加分项 ===
        score = 0
        reasons = []
        
        # 乖离率适中 (-15% ~ 0%)
        if -15 <= curr['distance_from_ma60'] <= 0:
            score += 1
            reasons.append("贴近60日线")
            
        # 极度静默 (波动小)
        if curr['amplitude_ma15'] < 3.0:
            score += 1
            reasons.append("低波动")
            
        # RSI超卖
        if curr['rsi'] < 45:
            score += 1
            reasons.append("RSI低位")
            
        # 至少要有一个加分项才推荐
        if score >= 1:
            return {
                'ts_code': curr['ts_code'],
                'close': curr['close'],
                'score': score,
                'reason': " ".join(reasons)
            }
        return None

    def run(self, full_df: pd.DataFrame):
        results = []
        # 按股票分组计算
        for code, data in full_df.groupby('ts_code'):
            try:
                data = data.sort_values('trade_date')
                res = self.analyze_ticker(data)
                if res:
                    results.append(res)
            except:
                continue
        
        return pd.DataFrame(results).sort_values('score', ascending=False) if results else pd.DataFrame()

def run_strategy(df):
    return ImprovedStrategy().run(df)
