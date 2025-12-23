# strategy.py
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

class WeightedScoringStrategy:
    """
    加权评分策略 (极致缩量版)
    必要前提(一票否决): 
    1. 当日收盘价 < 开盘价 (假阴真阳或纯阴线)
    2. 当日成交量 < 前20天(不含当日)最低成交量的 80%
    """
    
    def __init__(self, version='strict_shrink'):
        self.version = version
        # 既然条件这么苛刻，一旦入选通常值得关注，分数线设为60即可
        self.params = {'min_score': 60} 
    
    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        if 'vol' in df.columns and 'volume' not in df.columns:
            df['volume'] = df['vol']
        
        df = df.sort_values(['ts_code', 'trade_date'])
        
        # === 基础指标 ===
        df['ma_60'] = df.groupby('ts_code')['close'].transform(lambda x: x.rolling(60).mean())
        
        # === 关键：成交量计算 ===
        df['vol_ma20'] = df.groupby('ts_code')['volume'].transform(lambda x: x.rolling(20).mean())
        
        # 计算前20天(不含今天)的最低成交量
        # shift(1) 将数据下移一位，确保 rolling(20) 取到的是昨天及之前的20天
        df['prev_vol_min_20'] = df.groupby('ts_code')['volume'].transform(
            lambda x: x.shift(1).rolling(20).min()
        )
        
        # === 辅助指标 ===
        # 振幅
        df['amplitude'] = (df['high'] - df['low']) / df['low'] * 100
        df['amp_ma15'] = df.groupby('ts_code')['amplitude'].transform(lambda x: x.rolling(15).mean())
        
        # 涨跌幅
        df['pct_change'] = df.groupby('ts_code')['close'].transform(lambda x: x.pct_change() * 100)
        
        # 均线距离
        df['distance_ma60'] = (df['close'] - df['ma_60']) / df['ma_60'] * 100
        
        # 量比 (用于后续显示，非核心判断，因为核心判断用绝对值比对)
        df['vol_ratio'] = df['volume'] / df['vol_ma20']
        
        return df
    
    def calculate_scores(self, row: pd.Series) -> dict:
        scores = {}
        
        # ==========================================
        # === 0. 必要前提条件 (一票否决) ===
        # ==========================================
        
        # 条件1: 必须是绿的 (收盘价 < 开盘价)
        if row['close'] >= row['open']:
            scores['总分'] = 0
            scores['淘汰原因'] = '非绿盘'
            return scores
            
        # 条件2: 交易量是前面至少20天最低量的80%以下
        # 如果历史数据不足导致 prev_vol_min_20 为空，也直接过滤
        if pd.isnull(row['prev_vol_min_20']):
            scores['总分'] = 0
            return scores
            
        limit_vol = row['prev_vol_min_20'] * 0.8
        if row['volume'] >= limit_vol:
            scores['总分'] = 0
            scores['淘汰原因'] = '量能未达极致缩量'
            return scores

        # ==========================================
        # === 1. 核心条件 (50分) ===
        # ==========================================
        
        # A. 最低量分 (40分)
        # 能走到这里，说明已经满足了 < 80% 前低，这是非常极致的信号，直接给高分
        # 我们根据缩得有多厉害再细分一下
        vol_score = 40
        ratio = row['volume'] / row['prev_vol_min_20'] # 必然小于 0.8
        
        if ratio <= 0.5: vol_score = 40      # 缩量一半以上，极度惜售
        elif ratio <= 0.6: vol_score = 38
        elif ratio <= 0.7: vol_score = 35
        else: vol_score = 30                 # 0.7-0.8之间
        
        scores['最低量分'] = vol_score
        
        # B. 下跌幅度分 (10分)
        # 已经是绿盘了，看跌多少。通常极致缩量伴随小跌或急跌
        drop_score = 0
        pct = row['pct_change']
        if -2 <= pct < 0: drop_score = 10    # 阴跌/小跌 最好
        elif -4 <= pct < -2: drop_score = 8  # 中跌
        elif pct < -4: drop_score = 5        # 大跌
        scores['下跌分'] = drop_score
        
        scores['核心条件分'] = vol_score + drop_score

        # ==========================================
        # === 2. 趋势条件 (15分) ===
        # ==========================================
        trend_score = 0
        if pd.notnull(row['distance_ma60']):
            # 这种极致缩量，通常发生在回调到底部时
            if row['distance_ma60'] < 0:
                abs_dist = abs(row['distance_ma60'])
                if abs_dist >= 10: trend_score = 15 # 乖离率较大，超跌
                elif abs_dist >= 5: trend_score = 12
                else: trend_score = 8
            else:
                # 均线上方缩量回踩
                trend_score = 10 
        scores['趋势分'] = trend_score

        # ==========================================
        # === 3. 波动条件 (10分) ===
        # ==========================================
        # 缩量通常意味着波动率下降
        volatility_score = 0
        if pd.notnull(row['amp_ma15']):
            if row['amp_ma15'] < 3.0: volatility_score = 10
            elif row['amp_ma15'] < 4.5: volatility_score = 6
            else: volatility_score = 3
        scores['波动分'] = volatility_score

        # ==========================================
        # === 4. 价格条件 (10分) ===
        # ==========================================
        price_score = 0
        if 3 < row['close'] <= 30: price_score = 10 # 偏好中小盘低价
        elif 30 < row['close'] <= 80: price_score = 5
        scores['价格分'] = price_score

        # ==========================================
        # === 5. 额外加分 (15分) ===
        # ==========================================
        extra_score = 0
        # 实体很小（十字星类）加分
        body_size = abs(row['close'] - row['open']) / row['open'] * 100
        if body_size < 1.0: extra_score += 5
        
        # 如果是长下影线（探底回升）加分
        lower_shadow = (min(row['open'], row['close']) - row['low']) / row['low'] * 100
        if lower_shadow > 1.5: extra_score += 5
        
        # 量比特别小
        if row['vol_ratio'] < 0.6: extra_score += 5
        
        scores['额外分'] = min(15, extra_score)
        
        total = scores['核心条件分'] + trend_score + volatility_score + price_score + scores['额外分']
        scores['总分'] = min(100, total)
        
        # 记录关键数值供显示
        scores['_ratio_val'] = ratio # 记录缩量比例 
        
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
        # 显示格式：得分 | 现量/前20低占比 | 跌幅
        final_df['reason'] = final_df.apply(
            lambda x: f"极致缩量{x['总分']}分|是前低的:{x['_ratio_val']:.2f}倍|跌:{x['pct_change']:.1f}%", 
            axis=1
        )
        return final_df

def run_strategy(df):
    return WeightedScoringStrategy().run(df)
