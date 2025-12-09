# strategy.py
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

class WeightedScoringStrategy:
    """加权评分策略：核心条件50分，其他条件分值降低"""
    
    def __init__(self, version='flexible'):
        """
        初始化策略
        version: 'pure'(纯净版), 'flexible'(灵活版), 'strict'(严格版)
        """
        self.version = version
        self.strategy_name = f"加权评分策略-{version}"
        
        if version == 'pure':
            self.params = {'min_score': 70}
        elif version == 'flexible':
            self.params = {'min_score': 65}
        elif version == 'strict':
            self.params = {'min_score': 75}
    
    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算指标"""
        df = df.copy()
        
        # 字段标准化
        if 'vol' in df.columns and 'volume' not in df.columns:
            df['volume'] = df['vol']
        
        # 必须排序
        df = df.sort_values(['ts_code', 'trade_date'])
        
        # 基础指标
        # GroupBy Transform 保持数据行数不变，便于后续过滤
        df['ma_60'] = df.groupby('ts_code')['close'].transform(lambda x: x.rolling(60).mean())
        df['ma_20'] = df.groupby('ts_code')['close'].transform(lambda x: x.rolling(20).mean())
        
        # 成交量
        df['vol_ma20'] = df.groupby('ts_code')['volume'].transform(lambda x: x.rolling(20).mean())
        df['vol_min_20'] = df.groupby('ts_code')['volume'].transform(lambda x: x.rolling(20).min())
        
        # 振幅
        df['amplitude'] = (df['high'] - df['low']) / df['low'] * 100
        df['amp_ma15'] = df.groupby('ts_code')['amplitude'].transform(lambda x: x.rolling(15).mean())
        
        # 涨跌形态
        df['is_red'] = df['close'] >= df['open']
        df['pct_change'] = df.groupby('ts_code')['close'].transform(lambda x: x.pct_change() * 100)
        df['change_pct'] = df['pct_change'] # 别名兼容
        
        # 衍生指标
        df['distance_ma60'] = (df['close'] - df['ma_60']) / df['ma_60'] * 100
        df['vol_ratio'] = df['volume'] / df['vol_ma20']
        df['vol_to_min'] = df['volume'] / df['vol_min_20']
        
        return df
    
    def calculate_scores(self, row: pd.Series) -> dict:
        """计算单行分数"""
        scores = {}
        
        # === 1. 核心条件 (50分) ===
        # A. 是否接近最低量 (40分)
        vol_score = 0
        if pd.notnull(row['vol_to_min']):
            r = row['vol_to_min']
            if r <= 1.0: vol_score = 40
            elif r <= 1.01: vol_score = 38
            elif r <= 1.02: vol_score = 35
            elif r <= 1.05: vol_score = 30
            elif r <= 1.1: vol_score = 25
            elif r <= 1.2: vol_score = 20
            elif r <= 1.3: vol_score = 15
            elif r <= 1.5: vol_score = 10
        scores['最低量分'] = vol_score
        
        # B. 是否下跌 (10分)
        drop_score = 0
        if not row['is_red']:
            # 这里的 change_pct 应该是当日涨跌幅
            pct = row['pct_change']
            if -1 <= pct < 0: drop_score = 10
            elif -2 <= pct < -1: drop_score = 8
            elif -3 <= pct < -2: drop_score = 5
            elif pct < -3: drop_score = 3
        scores['下跌分'] = drop_score
        
        scores['核心条件分'] = vol_score + drop_score

        # === 2. 趋势条件 (15分) ===
        trend_score = 0
        if pd.notnull(row['ma_60']):
            # distance 为负数表示在均线下方，我们要找的是跌破均线的
            # 原逻辑: (ma60 - close) / ma60 > 0 => close < ma60
            # 这里的 row['distance_ma60'] = (close - ma60) / ma60
            # 所以 distance_ma60 < 0 代表在下方
            dist = row['distance_ma60']
            if dist < 0:
                abs_dist = abs(dist)
                if abs_dist >= 15: trend_score = 15
                elif abs_dist >= 10: trend_score = 12
                elif abs_dist >= 5: trend_score = 9
                elif abs_dist >= 2: trend_score = 6
                else: trend_score = 3
        scores['趋势分'] = trend_score

        # === 3. 波动条件 (10分) ===
        volatility_score = 0
        if pd.notnull(row['amp_ma15']):
            amp = row['amp_ma15']
            if amp < 2.0: volatility_score = 10
            elif amp < 2.5: volatility_score = 8
            elif amp < 3.0: volatility_score = 6
            elif amp < 3.5: volatility_score = 4
            elif amp < 4.0: volatility_score = 2
        scores['波动分'] = volatility_score

        # === 4. 价格条件 (10分) ===
        price_score = 0
        if row['close'] > 3:
            p = row['close']
            if p <= 10: price_score = 10
            elif p <= 20: price_score = 8
            elif p <= 30: price_score = 6
            elif p <= 50: price_score = 4
            else: price_score = 2
        scores['价格分'] = price_score

        # === 5. 额外加分 (15分) ===
        extra_score = 0
        if self.version == 'flexible':
            # 连续缩量
            if row['vol_ratio'] < 0.8: extra_score += 5
            # 位置合理
            if -10 <= row['distance_ma60'] <= -2: extra_score += 5
            # 跌幅适中
            if -2 <= row['pct_change'] <= -0.5: extra_score += 5
        
        scores['额外分'] = min(15, extra_score)
        
        # 总分
        total = scores['核心条件分'] + trend_score + volatility_score + price_score + scores['额外分']
        scores['总分'] = min(100, total)
        
        return scores

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        """执行策略入口"""
        if df.empty: return pd.DataFrame()
        
        # 1. 计算指标
        df = self.calculate_indicators(df)
        
        # 2. 取最新一天
        last_date = df['trade_date'].max()
        current_df = df[df['trade_date'] == last_date].copy()
        
        if current_df.empty: return pd.DataFrame()
        
        # 3. 打分
        results = []
        for idx, row in current_df.iterrows():
            scores = self.calculate_scores(row)
            if scores['总分'] >= self.params['min_score']:
                res = row.to_dict()
                res.update(scores)
                results.append(res)
                
        if not results: return pd.DataFrame()
        
        # 4. 排序返回
        final_df = pd.DataFrame(results)
        final_df = final_df.sort_values('总分', ascending=False)
        
        # 生成 reason 字段用于通知
        final_df['reason'] = final_df.apply(
            lambda x: f"总分{x['总分']:.0f} (核心{x['核心条件分']}分) | 量比min:{x['vol_to_min']:.2f}", 
            axis=1
        )
        
        return final_df

def run_strategy(df):
    # 默认使用 Flexible 版本，对实战最友好
    return WeightedScoringStrategy(version='flexible').run(df)
