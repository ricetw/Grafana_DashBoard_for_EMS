import datetime
import os

from dotenv import load_dotenv
from sqlalchemy import Boolean, create_engine, Column, DateTime, Float, Integer, String, TIMESTAMP
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class BaseTable():
    __table_args__ = {
        "mysql_charset": "utf8mb4"
    }
    id = Column(Integer, nullable=False, primary_key=True)
    timestamp = Column(TIMESTAMP, nullable=False,
                        default=datetime.datetime.utcnow())


class BaseTableForEM():
    __table_args__ = {
        "mysql_charset": "utf8mb4"
    }
    id = Column(Integer, nullable=False, primary_key=True)
    timestamp = Column(TIMESTAMP, nullable=False,
                        default=datetime.datetime.utcnow())
    I1 = Column(String(25), nullable=False)
    I2 = Column(String(25), nullable=False)
    I3 = Column(String(25), nullable=False)
    EffectivePower = Column(String(25), nullable=False)
    ReactivePower = Column(String(25), nullable=False)


class EnergyConsumption(BaseTable, Base):
    __tablename__ = 'EnergyConsumption'
    frequency = Column(Float(5, 2), nullable=False)
    Uan = Column(Float(5, 2), nullable=False)
    Ubn = Column(Float(5, 2), nullable=False)
    Ucn = Column(Float(5, 2), nullable=False)
    lnAV = Column(Float(5, 2), nullable=False)
    Uab = Column(Float(5, 2), nullable=False)
    Ubc = Column(Float(5, 2), nullable=False)
    Uca = Column(Float(5, 2), nullable=False)
    PFAV = Column(Float(5, 2), nullable=False)
    kVarH = Column(Float(5, 2), nullable=False)
    Ia = Column(Float(5, 2), nullable=False)
    Ib = Column(Float(5, 2), nullable=False)
    Ic = Column(Float(5, 2), nullable=False)
    In = Column(Float(5, 2), nullable=False)
    IAV = Column(Float(5, 2), nullable=False)
    kWa = Column(Float(5, 2), nullable=False)
    kWb = Column(Float(5, 2), nullable=False)
    kWc = Column(Float(5, 2), nullable=False)
    kW = Column(Float(5, 2), nullable=False)
    kWH_total = Column(Float(5, 2), nullable=False)
    kWH_last1H = Column(Float(5, 2), nullable=False)
    demand = Column(Float(5, 2), nullable=False)


class EM1(BaseTableForEM, Base):
    __tablename__ = 'EM1'


class EM2(BaseTableForEM, Base):
    __tablename__ = 'EM2'


class EM3(BaseTableForEM, Base):
    __tablename__ = 'EM3'


class EM4(BaseTableForEM, Base):
    __tablename__ = 'EM4'


class EM5(BaseTableForEM, Base):
    __tablename__ = 'EM5'


class EM6(BaseTableForEM, Base):
    __tablename__ = 'EM6'


class EM7(BaseTableForEM, Base):
    __tablename__ = 'EM7'


class EM8(BaseTableForEM, Base):
    __tablename__ = 'EM8'


class EM9(BaseTableForEM, Base):
    __tablename__ = 'EM9'


class EM10(BaseTableForEM, Base):
    __tablename__ = 'EM10'


class EM11(BaseTableForEM, Base):
    __tablename__ = 'EM11'


class EM12(BaseTableForEM, Base):
    __tablename__ = 'EM12'


class EM14(BaseTable, Base):
    __tablename__ = 'EM14'
    flow_rate = Column(String(25), nullable=False)
    flow = Column(String(25), nullable=False)

class EM15(BaseTable, Base):
    __tablename__ = 'EM15'
    EM1500 = Column(String(25), nullable=False)
    EM1502 = Column(String(25), nullable=False)
    EM1504 = Column(String(25), nullable=False)
    EM1510 = Column(String(25), nullable=False)
    EM1511 = Column(String(25), nullable=False)
    EM1512 = Column(String(25), nullable=False)
    EM1513 = Column(String(25), nullable=False)
    EM1514 = Column(String(25), nullable=False)
    EM1515 = Column(String(25), nullable=False)
    EM1516 = Column(String(25), nullable=False)
    EM1517 = Column(String(25), nullable=False)
    EM1518 = Column(String(25), nullable=False)
    EM1519 = Column(String(25), nullable=False)
    EM1520 = Column(String(25), nullable=False)
    EM1521 = Column(String(25), nullable=False)
    EM1522 = Column(String(25), nullable=False)
    EM1523 = Column(String(25), nullable=False)
    EM1524 = Column(String(25), nullable=False)


if __name__ == '__main__':
    current_directory = os.path.dirname(os.path.abspath(__file__))
    dotenv_path = os.path.join(current_directory, "..", ".env")
    load_dotenv(dotenv_path)

    engine = create_engine(os.environ.get("SQL_SERVER"), echo=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
