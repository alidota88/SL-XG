# strategy.py
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

class WeightedScoringStrategy:
    """加权评分策略：核心条件50分，其他条件分值降低"""
    
    def __init__(self, version='flexible'):
        self.version = version
        self.params = {'min_score': 60} # 稍微降低分数线，确保能选出股票
    
    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        if 'vol' in df.columns and 'volume' not in df.columns:
            df['volume'] = df['vol']
        
        df = df.sort_values(['ts_code', 'trade_date'])
        
        # 基础指标
        df['ma_60'] = df.groupby('ts_code')['close'].transform(lambda x: x.rolling(60).mean())
        
        # 成交量
        df['vol_ma20'] = df.groupby('ts_code')['volume'].transform(lambda x: x.rolling(20).mean())
        df['vol_min_20'] = df.groupby('ts_code')['volume'].transform(lambda x: x.rolling(20).min())
        
        # 振幅
        df['amplitude'] = (df['high'] - df['low']) / df['low'] * 100
        df['amp_ma15'] = df.groupby('ts_code')['amplitude'].transform(lambda x: x.rolling(15).mean())
        
        # 涨跌
        df['pct_change'] = df.groupby('ts_code')['close'].transform(lambda x: x.pct_change() * 100)
        
        # 衍生
        df['distance_ma60'] = (df['close'] - df['ma_60']) / df['ma_60'] * 100
        df['vol_ratio'] = df['volume'] / df['vol_ma20']
        df['vol_to_min'] = df['volume'] / df['vol_min_20']
        
        return df
    
    def calculate_scores(self, row: pd.Series) -> dict:
        scores = {}
        
        # === 1. 核心条件 (50分) ===
        # A. 最低量 (40分)
        vol_score = 0
        if pd.notnull(row['vol_to_min']):
            r = row['vol_to_min']
            if r <= 1.0: vol_score = 40
            elif r <= 1.05: vol_score = 35
            elif r <= 1.1: vol_score = 30
            elif r <= 1.2: vol_score = 20
            elif r <= 1.5: vol_score = 10
        scores['最低量分'] = vol_score
        
        # B. 下跌 (10分)
        drop_score = 0
        if row['pct_change'] < 0:
            pct = row['pct_change']
            if -1 <= pct < 0: drop_score = 10
            elif -3 <= pct < -1: drop_score = 8
            elif pct < -3: drop_score = 5
        scores['下跌分'] = drop_score
        
        scores['核心条件分'] = vol_score + drop_score

        # === 2. 趋势条件 (15分) ===
        trend_score = 0
        if pd.notnull(row['distance_ma60']):
            # 在均线下方 (distance < 0)
            if row['distance_ma60'] < 0:
                abs_dist = abs(row['distance_ma60'])
                if abs_dist >= 10: trend_score = 15
                elif abs_dist >= 5: trend_score = 12
                elif abs_dist >= 2: trend_score = 8
                else: trend_score = 5
        scores['趋势分'] = trend_score

        # === 3. 波动条件 (10分) ===
        vol_score = 0
        if pd.notnull(row['amp_ma15']):
            if row['amp_ma15'] < 2.5: vol_score = 10
            elif row['amp_ma15'] < 3.5: vol_score = 6
            elif row['amp_ma15'] < 4.5: vol_score = 2
        scores['波动分'] = vol_score

        # === 4. 价格条件 (10分) ===
        price_score = 0
        if 3 < row['close'] <= 30: price_score = 10
        elif 30 < row['close'] <= 60: price_score = 5
        scores['价格分'] = price_score

        # === 5. 额外加分 (15分) ===
        extra_score = 0
        if row['vol_ratio'] < 0.8: extra_score += 5
        if -10 <= row['distance_ma60'] <= -2: extra_score += 5
        if -2 <= row['pct_change'] <= -0.5: extra_score += 5
        scores['额外分'] = min(15, extra_score)
        
        total = scores['核心条件分'] + trend_score + vol_score + price_score + scores['额外分']
        scores['总分'] = min(100, total)
        return scores

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty: return pd.DataFrame()
        
        df = self.calculate_indicators(df)
        last_date = df['trade_date'].max()
        current_df = df[df['trade_date'] == last_date].copy()
        
        if current_df.empty: return pd.DataFrame()
        
        results = []
        for idx, row in current_df.iterrows():
            scores = self.calculate_scores(row)
            if scores['总分'] >= self.params['min_score']:
                res = row.to_dict()
                res.update(scores)
                results.append(res)
                
        if not results: return pd.DataFrame()
        
        final_df = pd.DataFrame(results).sort_values('总分', ascending=False)
        
        # 构造推送到手机的理由字符串
        final_df['reason'] = final_df.apply(
            lambda x: f"核心{x['核心条件分']}分 | 量比min:{x['vol_to_min']:.2f}", 
            axis=1
        )
        return final_df

def run_strategy(df):
    return WeightedScoringStrategy().run(df)
