from ast import In
from sqlalchemy import create_engine, Column, String, Integer, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import sessionmaker


engine = create_engine('sqlite:///ireland-dashboard.db', echo=False)
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()

class DailyDashboard(Base):
    __tablename__ = "DailyDashboard"

    id = Column(Integer, primary_key=True)
    date = Column('Date', Date)
    adunit = Column('Ad Unit', String)
    device_category = Column('Device Category', String)
    adunit_id = Column('Ad Unit ID', Integer)
    device_id = Column('Device ID', Integer)
    total_code_count = Column('Total Code Count', Integer)
    total_impressions = Column('Total Impressions', Integer)
    total_adrequests = Column('Total Ad Requests', Integer)
    total_fillrate = Column('Total Fill Rate', Integer)
    total_response_served = Column('Total Responses Served', Integer)
    total_unmatched_adrequests = Column('Total Unmatched Ad Requests', Integer)

class DashboardSponsorship(Base):
    __tablename__ = "DailyDashboardSponsorship"
    
    id = Column(Integer, primary_key=True)
    date = Column('Date', Date)
    adunit = Column('Ad Unit', String)
    device_category = Column('Device Category', String)
    line_item_type = Column('Line Item Type', String)
    ad_unit_id = Column('Ad Unit ID', Integer)
    device_id = Column('Device ID', Integer)
    total_impressions = Column('Total Impressions', Integer)


class AdRequests(Base):
    __tablename__ = "AdRequests"

    id = Column(Integer, primary_key=True)
    date = Column('Date', Date)
    cu_connected_tv = Column('CU Connected TV', Integer)
    cu_smartphone = Column('CU Smartphone', Integer)
    cu_tablet = Column('CU Tablet', Integer)
    cu_desktop = Column('CU Desktop', Integer)
    live_connected_tv = Column('Live Connected TV', Integer)
    live_smartphone = Column('Live Smartphone', Integer)
    live_tablet = Column('Live Tablet', Integer)
    live_desktop = Column('Live Desktop', Integer)
    tv_stb = Column('TV STB VOD', Integer)
    total = Column('Total', Integer)

class Responses(Base):
    __tablename__ = "Responses"

    id = Column(Integer, primary_key=True)
    date = Column('Date', Date)
    cu_connected_tv = Column('CU Connected TV', Integer)
    cu_smartphone = Column('CU Smartphone', Integer)
    cu_tablet = Column('CU Tablet', Integer)
    cu_desktop = Column('CU Desktop', Integer)
    live_connected_tv = Column('Live Connected TV', Integer)
    live_smartphone = Column('Live Smartphone', Integer)
    live_tablet = Column('Live Tablet', Integer)
    live_desktop = Column('Live Desktop', Integer)
    tv_stb = Column('TV STB VOD', Integer)
    total = Column('Total', Integer)

class Impressions(Base):
    __tablename__ = "Impressions"

    id = Column(Integer, primary_key=True)
    date = Column('Date', Date)
    cu_connected_tv = Column('CU Connected TV', Integer)
    cu_smartphone = Column('CU Smartphone', Integer)
    cu_tablet = Column('CU Tablet', Integer)
    cu_desktop = Column('CU Desktop', Integer)
    live_connected_tv = Column('Live Connected TV', Integer)
    live_smartphone = Column('Live Smartphone', Integer)
    live_tablet = Column('Live Tablet', Integer)
    live_desktop = Column('Live Desktop', Integer)
    tv_stb = Column('TV STB VOD', Integer)
    total = Column('Total', Integer)

class SellThroughRate(Base):
    __tablename__ = "SellThroughRate"

    id = Column(Integer, primary_key=True)
    date = Column('Date', Date)
    cu_connected_tv = Column('CU Connected TV', Integer)
    cu_smartphone = Column('CU Smartphone', Integer)
    cu_tablet = Column('CU Tablet', Integer)
    cu_desktop = Column('CU Desktop', Integer)
    live_connected_tv = Column('Live Connected TV', Integer)
    live_smartphone = Column('Live Smartphone', Integer)
    live_tablet = Column('Live Tablet', Integer)
    tv_stb = Column('TV STB VOD', Integer)
    average = Column('Average', Integer)

class RenderRate(Base):
    __tablename__ = "RenderRate"

    id = Column(Integer, primary_key=True)
    date = Column('Date', Date)
    cu_connected_tv = Column('CU Connected TV', Integer)
    cu_smartphone = Column('CU Smartphone', Integer)
    cu_tablet = Column('CU Tablet', Integer)
    cu_desktop = Column('CU Desktop', Integer)
    live_connected_tv = Column('Live Connected TV', Integer)
    live_smartphone = Column('Live Smartphone', Integer)
    live_tablet = Column('Live Tablet', Integer)
    tv_stb = Column('TV STB VOD', Integer)
    average = Column('Average', Integer)