from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker, DeclarativeBase
from coinx import config

# 创建数据库引擎
engine = create_engine(
    config.DATABASE_URI, 
    pool_recycle=3600, 
    pool_size=10, 
    max_overflow=20,
    echo=False  # 设置为True可以查看生成的SQL语句
)

# 创建线程安全的会话
db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))

# 创建基类
class Base(DeclarativeBase):
    pass

#为了方便查询，可以将 query 属性绑定到 Base
Base.query = db_session.query_property()

def init_db():
    """初始化数据库，创建所有表"""
    # 在这里导入定义模型的所有模块，以便它们在元数据上正确注册。
    # 否则，您必须在调用 init_db() 之前先导入它们。
    
    from coinx import models
    
    Base.metadata.create_all(bind=engine)

def get_session():
    """获取一个新的会话"""
    return db_session()
