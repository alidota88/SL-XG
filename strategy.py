# strategy.py
import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

class ImprovedStrategy:
    """
    改进版选股策略 v2.0 (适配 Railway 现有数据库版)
    策略逻辑：60日线下 + 缩量调整 + 低波动静默期 + 评分机制
    """
    
    def __init__(self, 
                 ma_period: int = 60,
                 min_price: float = 3.0):
        self.ma_period = ma_period
        self.min_price = min_price
        
    def calculate_indicators(self, data: pd.DataFrame) -> pd.DataFrame:
        """计算技术指标"""
        df = data.copy()
        
        # 【关键适配】将数据库的 'vol' 映射为策略通用的 'volume'
        if 'vol' in df.columns and 'volume' not in df.columns:
            df['volume'] = df['vol']

        # 1. 均线系统
        df['ma_60'] = df['close'].rolling(window=self.ma_period).mean()
        df['ma_20'] = df['close'].rolling(window=20).mean()
        df['ma_5'] = df['close'].rolling(window=5).mean()
        
        # 均线斜率（60日线近5日变化率）
        df['ma_60_slope'] = (df['ma_60'] - df['ma_60'].shift(5)) / df['ma_60'].shift(5) * 100
        
        # 2. 量能指标
        df['volume_ma20'] = df['volume'].rolling(window=20).mean()
        df['volume_ma5'] = df['volume'].rolling(window=5).mean()
        # 量比
        df['volume_ratio'] = df['volume'] / df['volume_ma5']
        
        # 3. 价格位置和波动
        # 当日振幅
        df['amplitude'] = (df['high'] - df['low']) / df['low'] * 100
        # 平均振幅 (15天)
        df['amplitude_ma15'] = df['amplitude'].rolling(window=15).mean()
        # 涨跌幅
        df['pct_change'] = df['close'].pct_change() * 100
        # K线实体大小
        df['k_body'] = (df['close'] - df['open']).abs() / df['open'] * 100
        
        # 4. 计算 RSI (14日) - 手动实现无需 TA-Lib
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))
        
        # 5. 衍生指标：收盘价与60日线的距离百分比
        df['distance_from_ma60'] = (df['close'] - df['ma_60']) / df['ma_60'] * 100
        
        return df
    
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        """生成选股信号"""
        # 确保数据长度足够
        if len(df) < 70: 
            df['signal'] = False
            return df
        
        # 获取最新一行数据进行判断
        latest = df.iloc[-1]
        
        # === 1. 核心条件 (必须满足) ===
        # 趋势：收盘价 < 60日线
        c_trend = latest['close'] < latest['ma_60']
        # 趋势：60日线走平或向下 (斜率 < 0.1%)
        c_slope = latest['ma_60_slope'] <= 0.1
        # 量能：缩量 (小于20日均量的 60%)
        c_vol = latest['volume'] < (latest['volume_ma20'] * 0.6)
        # 价格：不做低价仙股
        c_price = latest['close'] >= self.min_price
        
        core_score = sum([c_trend, c_slope, c_vol, c_price])
        
        # === 2. 重要条件 (打分制) ===
        # 距离：乖离率适中 (-15% 到 0%)
        i_dist = -15 <= latest['distance_from_ma60'] <= 0
        # 波动：低波动 (15日平均振幅 < 3.5%)
        i_amp = latest['amplitude_ma15'] < 3.5
        # 形态：小跌或微涨 (-3% 到 1%)
        i_shape = -3 <= latest['pct_change'] <= 1
        # 量比：不极端 (0.5 - 1.5)
        i_vr = 0.5 <= latest['volume_ratio'] <= 1.5
        # RSI：超卖区间反弹潜力 (30 - 55)
        i_rsi = 30 <= latest['rsi'] <= 55
        
        score_details = [i_dist, i_amp, i_shape, i_vr, i_rsi]
        important_score = sum(score_details)
        
        # === 3. 生成最终信号 ===
        # 逻辑：核心条件必须全对 + 重要条件至少满足 3 个
        is_selected = (core_score == 4) and (important_score >= 3)
        
        # 将结果写回 DataFrame 的最后一行 (为了保持格式一致返回 df)
        df['signal'] = False
        df.iloc[-1, df.columns.get_loc('signal')] = is_selected
        
        # 记录评分和理由 (Hack: 存到最后一行)
        df['total_score'] = 0
        df.iloc[-1, df.columns.get_loc('total_score')] = important_score
        
        return df

    def analyze(self, full_df: pd.DataFrame) -> pd.DataFrame:
        """
        主入口：处理所有股票数据
        """
        results = []
        
        # 按股票代码分组处理
        grouped = full_df.groupby('ts_code')
        
        print(f"🔄 [Strategy] Analyzing {len(grouped)} stocks with v2.0 Logic...")
        
        for code, data in grouped:
            try:
                # 必须按日期排序
                data = data.sort_values('trade_date')
                
                # 计算指标
                data = self.calculate_indicators(data)
                
                # 生成信号
                data = self.generate_signals(data)
                
                # 提取结果
                latest = data.iloc[-1]
                if latest['signal']:
                    # 生成推荐理由
                    reasons = []
                    if latest['distance_from_ma60'] > -5: reasons.append("贴近60日线")
                    if latest['volume'] < latest['volume_ma20'] * 0.4: reasons.append("极致缩量")
                    if latest['amplitude_ma15'] < 2.0: reasons.append("极度静默")
                    if latest['rsi'] < 40: reasons.append("RSI超卖")
                    
                    results.append({
                        'ts_code': code,
                        'trade_date': latest['trade_date'],
                        'close': latest['close'],
                        'vol': latest['volume'],
                        'score': int(latest['total_score']),
                        'reason': " ".join(reasons) if reasons else "形态综合良好"
                    })
            except Exception as e:
                continue
                
        if not results:
            return pd.DataFrame()
            
        # 结果转 DF 并按分数排序
        res_df = pd.DataFrame(results)
        return res_df.sort_values('score', ascending=False)

# 为了兼容旧代码的调用方式，提供一个简单的包装函数
def run_strategy(df: pd.DataFrame) -> pd.DataFrame:
    strategy = ImprovedStrategy()
    return strategy.analyze(df)
