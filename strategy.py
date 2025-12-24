# strategy.py
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

class ShrinkBreakoutStrategy:
    """
    极致缩量后起爆策略 (20日量比排序版)
    
    逻辑：
    1. 历史回溯：过去20天内（不含今日），是否存在过【极致缩量日】。
       - 极致缩量日定义：收盘<开盘 (绿盘) 且 成交量 < 前20天(不含当日)最低成交量的 90%。
    2. 今日触发：突然放量上涨，收大阳线。
    3. 排序：按20日量比从高到低排序。
    """
    
    def __init__(self, version='breakout_v2_vol_rank'):
        self.version = version
        self.params = {'min_score': 60} 
    
    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        if 'vol' in df.columns and 'volume' not in df.columns:
            df['volume'] = df['vol']
        
        df = df.sort_values(['ts_code', 'trade_date'])
        
        # === 1. 基础指标 ===
        df['ma_5'] = df.groupby('ts_code')['close'].transform(lambda x: x.rolling(5).mean())
        df['ma_20'] = df.groupby('ts_code')['close'].transform(lambda x: x.rolling(20).mean())
        df['ma_60'] = df.groupby('ts_code')['close'].transform(lambda x: x.rolling(60).mean())
        
        # === 成交量均线 (新增10日) ===
        df['vol_ma5'] = df.groupby('ts_code')['volume'].transform(lambda x: x.rolling(5).mean())
        df['vol_ma10'] = df.groupby('ts_code')['volume'].transform(lambda x: x.rolling(10).mean())
        df['vol_ma20'] = df.groupby('ts_code')['volume'].transform(lambda x: x.rolling(20).mean())
        
        df['pct_change'] = df.groupby('ts_code')['close'].transform(lambda x: x.pct_change() * 100)
        
        # === 2. 核心：识别历史上的“极致缩量日” ===
        
        # A. 计算每一天其“前20天（不含当日）”的最低成交量
        df['prev_20_min_vol'] = df.groupby('ts_code')['volume'].transform(
            lambda x: x.shift(1).rolling(20).min()
        )
        
        # B. 标记哪一天是“极致缩量日” (True/False)
        # 条件：绿盘 (close < open) 且 量 < 前20天最低量的 90%
        condition_shrink = (df['close'] < df['open']) & (df['volume'] < 0.9 * df['prev_20_min_vol'])
        df['is_shrink_day'] = condition_shrink
        
        # C. 统计过去20天内（不含今天）是否有过“极致缩量日”
        df['has_shrink_in_past_20'] = df.groupby('ts_code')['is_shrink_day'].transform(
            lambda x: x.shift(1).rolling(20).sum()
        )
        
        return df
    
    def calculate_scores(self, row: pd.Series) -> dict:
        scores = {}
        
        # ==========================================
        # === 0. 必要前提 (一票否决) ===
        # ==========================================
        
        # 条件1: 过去20天内必须出现过极致缩量信号
        if pd.isnull(row['has_shrink_in_past_20']) or row['has_shrink_in_past_20'] < 1:
            scores['总分'] = 0
            scores['淘汰原因'] = '近期无缩量信号'
            return scores

        # 条件2: 今天必须是红盘 (收盘 > 开盘) 且是上涨的
        if row['close'] <= row['open'] or row['pct_change'] <= 0:
            scores['总分'] = 0
            scores['淘汰原因'] = '今日非阳线'
            return scores
            
        # 条件3: 今天必须放量
        # 基础门槛：量比20日均量 > 1.0 且 涨幅 > 3%
        if pd.isnull(row['vol_ma20']) or row['vol_ma20'] == 0:
             scores['总分'] = 0
             return scores

        if row['volume'] < row['vol_ma20']:
            scores['总分'] = 0
            scores['淘汰原因'] = '今日未放量(小于20日均量)'
            return scores
            
        if row['pct_change'] < 3.0: 
            scores['总分'] = 0
            scores['淘汰原因'] = '涨幅力度不够'
            return scores

        # === 计算具体的量比数值 (用于排序和显示) ===
        # 10日量比
        vol_ratio_10 = row['volume'] / row['vol_ma10'] if row['vol_ma10'] > 0 else 0
        # 20日量比
        vol_ratio_20 = row['volume'] / row['vol_ma20'] if row['vol_ma20'] > 0 else 0
        
        # 将这两个数值存入 scores，以便合并到结果 DataFrame 中
        scores['vol_ratio_10'] = vol_ratio_10
        scores['vol_ratio_20'] = vol_ratio_20

        # ==========================================
        # === 1. 爆发力度评分 (40分) ===
        # ==========================================
        breakout_score = 0
        if row['pct_change'] >= 9.0: breakout_score = 40
        elif row['pct_change'] >= 6.0: breakout_score = 35
        elif row['pct_change'] >= 4.0: breakout_score = 25
        else: breakout_score = 15
        scores['爆发力度分'] = breakout_score

        # ==========================================
        # === 2. 放量程度评分 (30分) ===
        # ==========================================
        # 修改为：主要参考 20日量比 评分，因为这是排序依据
        vol_score = 0
        
        if vol_ratio_20 >= 3.0: vol_score = 30     # 3倍量，极强
        elif vol_ratio_20 >= 2.0: vol_score = 25   # 倍量
        elif vol_ratio_20 >= 1.5: vol_score = 20
        else: vol_score = 10
        
        scores['放量分'] = vol_score

        # ==========================================
        # === 3. 趋势位置评分 (20分) ===
        # ==========================================
        trend_score = 0
        # 突破20日线
        if row['open'] < row['ma_20'] and row['close'] > row['ma_20']:
            trend_score += 10
        
        # 均线乖离判断
        dist_60 = (row['close'] - row['ma_60']) / row['ma_60'] * 100
        if -15 < dist_60 < 5: 
             trend_score += 10
        scores['趋势分'] = trend_score

        # ==========================================
        # === 4. 额外加分 (10分) ===
        # ==========================================
        extra_score = 0
        upper_shadow = (row['high'] - row['close']) / row['close'] * 100
        if upper_shadow < 0.5: extra_score += 5
        if 5 < row['close'] < 50: extra_score += 5
        scores['额外分'] = extra_score
        
        total = breakout_score + vol_score + trend_score + extra_score
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
        
        final_df = pd.DataFrame(results)
        
        # === 核心修改：排序逻辑 ===
        # 根据 20日线量比 (vol_ratio_20) 从高到低排序
        final_df = final_df.sort_values('vol_ratio_20', ascending=False)
        
        # 构造理由
        final_df['reason'] = final_df.apply(
            lambda x: f"总分{x['总分']} | 量比(20日):{x['vol_ratio_20']:.1f}倍 | 量比(10日):{x['vol_ratio_10']:.1f}倍", 
            axis=1
        )
        return final_df

def run_strategy(df):
    return ShrinkBreakoutStrategy().run(df)
