import datetime
import os

from dotenv import load_dotenv
from sqlalchemy import Boolean, create_engine, Column, DateTime, Float, Integer, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class BaseTable():
    __table_args__ = {
        "mysql_charset": "utf8mb4"
    }
    Count_Log = Column(Integer, nullable=False, primary_key=True)
    status = Column(Boolean, nullable=False)
    demand = Column(Float(5, 2), nullable=False)
    frequency = Column(Float(5, 2), nullable=False)
    Uan = Column(Float(5, 2), nullable=False)
    Ubn = Column(Float(5, 2), nullable=False)
    Ucn = Column(Float(5, 2), nullable=False)
    lnAV = Column(Float(5, 2), nullable=False)
    Uab = Column(Float(5, 2), nullable=False)
    Ubc = Column(Float(5, 2), nullable=False)
    Uca = Column(Float(5, 2), nullable=False)
    Ia = Column(Float(5, 2), nullable=False)
    Ib = Column(Float(5, 2), nullable=False)
    Ic = Column(Float(5, 2), nullable=False)
    IAV = Column(Float(5, 2), nullable=False)
    kWa = Column(Float(5, 2), nullable=False)
    kWb = Column(Float(5, 2), nullable=False)
    kWc = Column(Float(5, 2), nullable=False)
    kW = Column(Float(5, 2), nullable=False)
    kWH = Column(Float(5, 2), nullable=False)
    PFAV = Column(Float(5, 2), nullable=False)
    kVarH = Column(Float(5, 2), nullable=False)
    datetime = Column(DateTime(timezone=False), nullable=False)
    

class BondingMachine1(BaseTable, Base): # 接合機1
    __tablename__ = 'BondingMachine1'


class BondingMachine2(BaseTable, Base): # 接合機2
    __tablename__ = 'BondingMachine2'


class DrillerMachine1(BaseTable, Base): # 鑽孔機1
    __tablename__ = 'DrillerMachine1'


class DrillerMachine2(BaseTable, Base): # 鑽孔機2
    __tablename__ = 'DrillerMachine2'


class RollerMachine(BaseTable, Base): # 捲圓機
    __tablename__ = 'RollerMachine'


class ChipRemovalMachine(BaseTable, Base): # 排屑機
    __tablename__ = 'ChipRemovalMachine'


class SawingMachine(BaseTable, Base): # 鋸切機
    __tablename__ = 'SawingMachine'


if __name__ == '__main__':
    current_directory = os.path.dirname(os.path.abspath(__file__))
    dotenv_path = os.path.join(current_directory, "..", ".env")
    load_dotenv(dotenv_path)

    engine = create_engine(os.environ.get("SQL_SERVER"), echo=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

