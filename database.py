# database.py
import os
from sqlalchemy import create_engine, Column, String, Float, Date, Integer
from sqlalchemy.orm import declarative_base, sessionmaker

# 获取环境变量
DATABASE_URL = os.getenv("DATABASE_URL")

# 兼容性处理：SQLAlchemy 1.4+ 需要 postgresql:// 协议头
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# 创建数据库引擎
engine = create_engine(DATABASE_URL, echo=False)

# 创建会话工厂
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# 声明基类
Base = declarative_base()

class StockDaily(Base):
    """
    股票日线行情表模型
    """
    __tablename__ = "stock_daily"

    # 使用复合主键防止重复数据 (股票代码 + 日期)
    ts_code = Column(String(20), primary_key=True, index=True, comment="股票代码")
    trade_date = Column(Date, primary_key=True, index=True, comment="交易日期")
    
    open = Column(Float, comment="开盘价")
    high = Column(Float, comment="最高价")
    low = Column(Float, comment="最低价")
    close = Column(Float, comment="收盘价")
    vol = Column(Float, comment="成交量")

def init_db():
    """
    初始化数据库：如果表不存在则创建
    """
    try:
        Base.metadata.create_all(bind=engine)
        print("✅ [Database] Schema initialized successfully.")
    except Exception as e:
        print(f"❌ [Database] Initialization failed: {e}")

def get_db():
    """依赖注入用的 Session 生成器"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
