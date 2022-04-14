#Internal Imports
from hashlib import new
from tracemalloc import start

from pkg_resources import require
from sqlalchemy import true
from models import *
from adunit_id import *
from functools import reduce


#External Imports
import argparse
import pathlib
import csv
import datetime
import zipfile

parser = argparse.ArgumentParser(description='Uploads the CSV, process it.')
parser.add_argument('--dash', type=pathlib.Path)
parser.add_argument('--spon', type=pathlib.Path)

args = parser.parse_args()

dashboard = args.dash
spon_dashboard = args.spon

def start():
    if args.dash:
        add_daily_dashboard()
        populate_adrequests()
        populate_responses()
        populate_impressions()
        populate_sell_through_rate()

    elif args.spon:
        add_daily_spon()
        populate_adrequests()
        populate_responses()
        populate_impressions()
        populate_sell_through_rate()


def add_daily_dashboard():
    '''
    Adds the Daily Dashboard CSV file to the 'DailyDashboard' DB.
    '''

    with open(dashboard) as csvfile:
        data = csv.DictReader(csvfile)
        for row in data:
            date = datetime.datetime.strptime(row['Date'], '%d/%m/%Y').date()
            adunit = row['Ad unit']
            device_category = row['Device category']
            adunit_id = row['Ad unit ID']
            device_id = row['Device category ID']
            total_code_count = row['Total code served count']
            total_impressions = row['Total impressions'].replace(',', '')
            total_adrequests = row['Total ad requests'].replace(',', '')
            total_fillrate = row['Total fill rate']
            total_response_served = row['Total responses served'].replace(',', '')
            total_unmatched_adrequests = row['Total unmatched ad requests'].replace(',', '')

            new_data = DailyDashboard(date=date, adunit=adunit, device_category=device_category, adunit_id=adunit_id, device_id=device_id, total_code_count=total_code_count,
                                    total_impressions=total_impressions, total_adrequests=total_adrequests, total_fillrate=total_fillrate, total_response_served=total_response_served,
                                    total_unmatched_adrequests=total_unmatched_adrequests)

            data_in_db = session.query(DailyDashboard).filter(
                DailyDashboard.date == new_data.date,
                DailyDashboard.adunit == new_data.adunit,
                DailyDashboard.device_category == new_data.device_category,
                DailyDashboard.adunit_id == new_data.adunit_id,
                DailyDashboard.device_id == new_data.device_id,
                DailyDashboard.total_code_count == new_data.total_code_count,
                DailyDashboard.total_impressions == new_data.total_impressions,
                DailyDashboard.total_adrequests == new_data.total_adrequests,
                DailyDashboard.total_fillrate == new_data.total_fillrate,
                DailyDashboard.total_response_served == new_data.total_response_served,
                DailyDashboard.total_unmatched_adrequests == new_data.total_unmatched_adrequests
            ).one_or_none()

            if data_in_db != None:
                pass
            else:
                session.add(new_data)
                session.commit()


def add_daily_spon():
    with open(spon_dashboard) as csvfile:
        data = csv.DictReader(csvfile)
        for row in data:
            date = datetime.datetime.strptime(row['Date'], '%d/%m/%Y').date()
            adunit = row['Ad unit']
            device_category = row['Device category']
            line_item_type = row['Line item type']
            ad_unit_id = row['Ad unit ID']
            device_id = row['Device category ID']
            total_impressions = row['Total impressions'].replace(',', '')

            new_data = DashboardSponsorship(date=date, adunit=adunit, device_category=device_category, line_item_type=line_item_type, ad_unit_id=ad_unit_id,
                device_id=device_id, total_impressions=total_impressions)

            data_in_db = session.query(DashboardSponsorship).filter(
                DashboardSponsorship.date == new_data.date,
                DashboardSponsorship.adunit == new_data.adunit,
                DashboardSponsorship.device_category == new_data.device_category,
                DashboardSponsorship.line_item_type == new_data.line_item_type,
                DashboardSponsorship.ad_unit_id == new_data.ad_unit_id,
                DashboardSponsorship.device_id == new_data.device_id,
                DashboardSponsorship.total_impressions == new_data.total_impressions
            ).one_or_none()

            if data_in_db != None:
                pass
            else:
                session.add(new_data)
                session.commit()


def populate_adrequests():
    start_date = datetime.date(2022, 4, 1)
    today = datetime.date.today()
    delta = datetime.timedelta(days=1)
    while start_date != today:
        try:
            cu_connected_tv_data = AdRequests(
                date=start_date,

                cu_connected_tv=reduce(get_adrequest, [device.total_adrequests for device in session.query(DailyDashboard).all() 
                if (device.adunit_id == VM_CATCHUP and device.device_id == CONNECTED_TV and device.date == start_date) or 
                (device.adunit_id == VM_CATCHUP_CHROMECAST and device.device_id == CONNECTED_TV and device.date == start_date) or
                (device.adunit_id == VM_CATCHUP_AMAZONFIRE and device.device_id == CONNECTED_TV and device.date == start_date) or
                (device.adunit_id == VM_CATCHUP_ANDROIDTV and device.device_id == CONNECTED_TV and device.date == start_date) or
                (device.adunit_id == VM_CATCHUP_APPLETV and device.device_id == CONNECTED_TV and device.date == start_date) or
                (device.adunit_id == VM_CATCHUP_ROKU and device.device_id == CONNECTED_TV and device.date == start_date)]),

                cu_smartphone=reduce(get_adrequest, [device.total_adrequests for device in session.query(DailyDashboard).all()
                    if (device.adunit_id == VM_CATCHUP and device.device_id == SMARTPHONE) or 
                    (device.adunit_id == VM_CATCHUP_CHROMECAST and device.device_id == SMARTPHONE and device.date == start_date) or
                    (device.adunit_id == VM_CATCHUP_AMAZONFIRE and device.device_id == SMARTPHONE and device.date == start_date) or
                    (device.adunit_id == VM_CATCHUP_ANDROIDTV and device.device_id == SMARTPHONE and device.date == start_date) or
                    (device.adunit_id == VM_CATCHUP_APPLETV and device.device_id == SMARTPHONE and device.date == start_date) or
                    (device.adunit_id == VM_CATCHUP_ROKU and device.device_id == SMARTPHONE and device.date == start_date)]),

                cu_tablet=reduce(get_adrequest, [device.total_adrequests for device in session.query(DailyDashboard).all() 
                    if (device.adunit_id == VM_CATCHUP and device.device_id == TABLET) or 
                    (device.adunit_id == VM_CATCHUP_CHROMECAST and device.device_id == TABLET and device.date == start_date) or
                    (device.adunit_id == VM_CATCHUP_AMAZONFIRE and device.device_id == TABLET and device.date == start_date) or
                    (device.adunit_id == VM_CATCHUP_ANDROIDTV and device.device_id == TABLET and device.date == start_date) or
                    (device.adunit_id == VM_CATCHUP_APPLETV and device.device_id == TABLET and device.date == start_date) or
                    (device.adunit_id == VM_CATCHUP_ROKU and device.device_id == TABLET and device.date == start_date)]),

                cu_desktop=reduce(get_adrequest, [device.total_adrequests for device in session.query(DailyDashboard).all() 
                    if (device.adunit_id == VM_CATCHUP and device.device_id == DESKTOP) or 
                    (device.adunit_id == VM_CATCHUP_CHROMECAST and device.device_id == DESKTOP and device.date == start_date) or
                    (device.adunit_id == VM_CATCHUP_AMAZONFIRE and device.device_id == DESKTOP and device.date == start_date) or
                    (device.adunit_id == VM_CATCHUP_ANDROIDTV and device.device_id == DESKTOP and device.date == start_date) or
                    (device.adunit_id == VM_CATCHUP_APPLETV and device.device_id == DESKTOP and device.date == start_date) or
                    (device.adunit_id == VM_CATCHUP_ROKU and device.device_id == DESKTOP and device.date == start_date)]),
            )
                
        except TypeError:
            cu_connected_tv_data = AdRequests(date=start_date, cu_connected_tv=0, cu_smartphone=0, cu_tablet=0, cu_desktop=0)
        
        session.add(cu_connected_tv_data)
        session.commit()
        start_date += delta


def populate_responses():
    '''
    Populates the 'AdRequest' DB.
    '''
    responses_data = Responses(
        #CATCH UP AD UNITS
        cu_connected_tv=reduce(get_adrequest, [device.total_response_served for device in session.query(DailyDashboard).all() 
            if (device.adunit_id == VM_CATCHUP and device.device_id == CONNECTED_TV) or 
            (device.adunit_id == VM_CATCHUP_CHROMECAST and device.device_id == CONNECTED_TV) or
            (device.adunit_id == VM_CATCHUP_AMAZONFIRE and device.device_id == CONNECTED_TV) or
            (device.adunit_id == VM_CATCHUP_ANDROIDTV and device.device_id == CONNECTED_TV) or
            (device.adunit_id == VM_CATCHUP_APPLETV and device.device_id == CONNECTED_TV) or
            (device.adunit_id == VM_CATCHUP_ROKU and device.device_id == CONNECTED_TV)]),

        cu_smartphone=reduce(get_adrequest, [device.total_response_served for device in session.query(DailyDashboard).all() 
            if (device.adunit_id == VM_CATCHUP and device.device_id == SMARTPHONE) or 
            (device.adunit_id == VM_CATCHUP_CHROMECAST and device.device_id == SMARTPHONE) or
            (device.adunit_id == VM_CATCHUP_AMAZONFIRE and device.device_id == SMARTPHONE) or
            (device.adunit_id == VM_CATCHUP_ANDROIDTV and device.device_id == SMARTPHONE) or
            (device.adunit_id == VM_CATCHUP_APPLETV and device.device_id == SMARTPHONE) or
            (device.adunit_id == VM_CATCHUP_ROKU and device.device_id == SMARTPHONE)]),

        cu_tablet=reduce(get_adrequest, [device.total_response_served for device in session.query(DailyDashboard).all() 
            if (device.adunit_id == VM_CATCHUP and device.device_id == TABLET) or 
            (device.adunit_id == VM_CATCHUP_CHROMECAST and device.device_id == TABLET) or
            (device.adunit_id == VM_CATCHUP_AMAZONFIRE and device.device_id == TABLET) or
            (device.adunit_id == VM_CATCHUP_ANDROIDTV and device.device_id == TABLET) or
            (device.adunit_id == VM_CATCHUP_APPLETV and device.device_id == TABLET) or
            (device.adunit_id == VM_CATCHUP_ROKU and device.device_id == TABLET)]),

        cu_desktop=reduce(get_adrequest, [device.total_response_served for device in session.query(DailyDashboard).all() 
            if (device.adunit_id == VM_CATCHUP and device.device_id == DESKTOP) or 
            (device.adunit_id == VM_CATCHUP_CHROMECAST and device.device_id == DESKTOP) or
            (device.adunit_id == VM_CATCHUP_AMAZONFIRE and device.device_id == DESKTOP) or
            (device.adunit_id == VM_CATCHUP_ANDROIDTV and device.device_id == DESKTOP) or
            (device.adunit_id == VM_CATCHUP_APPLETV and device.device_id == DESKTOP) or
            (device.adunit_id == VM_CATCHUP_ROKU and device.device_id == DESKTOP)]),

        #LIVE AD UNITS
        live_connected_tv=reduce(get_adrequest, [device.total_response_served for device in session.query(DailyDashboard).all() 
            if (device.adunit_id == LIVEONE and device.device_id == CONNECTED_TV) or 
            (device.adunit_id == LIVEONE_CHROMECAST and device.device_id == CONNECTED_TV) or
            (device.adunit_id == LIVEONE_AMAZONFIRE and device.device_id == CONNECTED_TV) or
            (device.adunit_id == LIVEONE_ANDROID and device.device_id == CONNECTED_TV) or
            (device.adunit_id == LIVEONE_APPLETV and device.device_id == CONNECTED_TV) or
            (device.adunit_id == LIVEONE_ROKU and device.device_id == CONNECTED_TV) or
            (device.adunit_id == LIVETWO and device.device_id == CONNECTED_TV) or
            (device.adunit_id == LIVETWO_CHROMECAST and device.device_id == CONNECTED_TV) or
            (device.adunit_id == LIVETWO_AMAZONFIRE and device.device_id == CONNECTED_TV) or
            (device.adunit_id == LIVETWO_ANDROID and device.device_id == CONNECTED_TV) or
            (device.adunit_id == LIVETWO_APPLETV and device.device_id == CONNECTED_TV) or
            (device.adunit_id == LIVETWO_ROKU and device.device_id == CONNECTED_TV) or
            (device.adunit_id == LIVETHREE and device.device_id == CONNECTED_TV) or
            (device.adunit_id == LIVETHREE_CHROMECAST and device.device_id == CONNECTED_TV) or
            (device.adunit_id == LIVETHREE_AMAZONFIRE and device.device_id == CONNECTED_TV) or
            (device.adunit_id == LIVETHREE_ANDROID and device.device_id == CONNECTED_TV) or
            (device.adunit_id == LIVETHREE_APPLETV and device.device_id == CONNECTED_TV) or
            (device.adunit_id == LIVETHREE_ROKU and device.device_id == CONNECTED_TV)]),

        live_smartphone=reduce(get_adrequest, [device.total_response_served for device in session.query(DailyDashboard).all() 
            if (device.adunit_id == LIVEONE and device.device_id == SMARTPHONE) or 
            (device.adunit_id == LIVEONE_CHROMECAST and device.device_id == SMARTPHONE) or
            (device.adunit_id == LIVEONE_AMAZONFIRE and device.device_id == SMARTPHONE) or
            (device.adunit_id == LIVEONE_ANDROID and device.device_id == SMARTPHONE) or
            (device.adunit_id == LIVEONE_APPLETV and device.device_id == SMARTPHONE) or
            (device.adunit_id == LIVEONE_ROKU and device.device_id == SMARTPHONE) or
            (device.adunit_id == LIVETWO and device.device_id == SMARTPHONE) or
            (device.adunit_id == LIVETWO_CHROMECAST and device.device_id == SMARTPHONE) or
            (device.adunit_id == LIVETWO_AMAZONFIRE and device.device_id == SMARTPHONE) or
            (device.adunit_id == LIVETWO_ANDROID and device.device_id == SMARTPHONE) or
            (device.adunit_id == LIVETWO_APPLETV and device.device_id == SMARTPHONE) or
            (device.adunit_id == LIVETWO_ROKU and device.device_id == SMARTPHONE) or
            (device.adunit_id == LIVETHREE and device.device_id == SMARTPHONE) or
            (device.adunit_id == LIVETHREE_CHROMECAST and device.device_id == SMARTPHONE) or
            (device.adunit_id == LIVETHREE_AMAZONFIRE and device.device_id == SMARTPHONE) or
            (device.adunit_id == LIVETHREE_ANDROID and device.device_id == SMARTPHONE) or
            (device.adunit_id == LIVETHREE_APPLETV and device.device_id == SMARTPHONE) or
            (device.adunit_id == LIVETHREE_ROKU and device.device_id == SMARTPHONE)]),

        live_tablet=reduce(get_adrequest, [device.total_response_served for device in session.query(DailyDashboard).all() 
            if (device.adunit_id == LIVEONE and device.device_id == TABLET) or 
            (device.adunit_id == LIVEONE_CHROMECAST and device.device_id == TABLET) or
            (device.adunit_id == LIVEONE_AMAZONFIRE and device.device_id == TABLET) or
            (device.adunit_id == LIVEONE_ANDROID and device.device_id == TABLET) or
            (device.adunit_id == LIVEONE_APPLETV and device.device_id == TABLET) or
            (device.adunit_id == LIVEONE_ROKU and device.device_id == TABLET) or
            (device.adunit_id == LIVETWO and device.device_id == TABLET) or
            (device.adunit_id == LIVETWO_CHROMECAST and device.device_id == TABLET) or
            (device.adunit_id == LIVETWO_AMAZONFIRE and device.device_id == TABLET) or
            (device.adunit_id == LIVETWO_ANDROID and device.device_id == TABLET) or
            (device.adunit_id == LIVETWO_APPLETV and device.device_id == TABLET) or
            (device.adunit_id == LIVETWO_ROKU and device.device_id == TABLET) or
            (device.adunit_id == LIVETHREE and device.device_id == TABLET) or
            (device.adunit_id == LIVETHREE_CHROMECAST and device.device_id == TABLET) or
            (device.adunit_id == LIVETHREE_AMAZONFIRE and device.device_id == TABLET) or
            (device.adunit_id == LIVETHREE_ANDROID and device.device_id == TABLET) or
            (device.adunit_id == LIVETHREE_APPLETV and device.device_id == TABLET) or
            (device.adunit_id == LIVETHREE_ROKU and device.device_id == TABLET)]),

        live_desktop=reduce(get_adrequest, [device.total_response_served for device in session.query(DailyDashboard).all() 
            if (device.adunit_id == LIVEONE and device.device_id == DESKTOP) or 
            (device.adunit_id == LIVEONE_CHROMECAST and device.device_id == DESKTOP) or
            (device.adunit_id == LIVEONE_AMAZONFIRE and device.device_id == DESKTOP) or
            (device.adunit_id == LIVEONE_ANDROID and device.device_id == DESKTOP) or
            (device.adunit_id == LIVEONE_APPLETV and device.device_id == DESKTOP) or
            (device.adunit_id == LIVEONE_ROKU and device.device_id == DESKTOP) or
            (device.adunit_id == LIVETWO and device.device_id == DESKTOP) or
            (device.adunit_id == LIVETWO_CHROMECAST and device.device_id == DESKTOP) or
            (device.adunit_id == LIVETWO_AMAZONFIRE and device.device_id == DESKTOP) or
            (device.adunit_id == LIVETWO_ANDROID and device.device_id == DESKTOP) or
            (device.adunit_id == LIVETWO_APPLETV and device.device_id == DESKTOP) or
            (device.adunit_id == LIVETWO_ROKU and device.device_id == DESKTOP) or
            (device.adunit_id == LIVETHREE and device.device_id == DESKTOP) or
            (device.adunit_id == LIVETHREE_CHROMECAST and device.device_id == DESKTOP) or
            (device.adunit_id == LIVETHREE_AMAZONFIRE and device.device_id == DESKTOP) or
            (device.adunit_id == LIVETHREE_ANDROID and device.device_id == DESKTOP) or
            (device.adunit_id == LIVETHREE_APPLETV and device.device_id == DESKTOP) or
            (device.adunit_id == LIVETHREE_ROKU and device.device_id == DESKTOP)])
    )

    session.add(responses_data)
    session.commit()

def populate_impressions():
    '''
    Populates the 'Impressions' DB.
    '''
    impressions_data = Impressions(
        #CATCH UP AD UNITS
        cu_connected_tv=reduce(get_adrequest, [device.total_impressions for device in session.query(DailyDashboard).all() 
            if (device.adunit_id == VM_CATCHUP and device.device_id == CONNECTED_TV) or 
            (device.adunit_id == VM_CATCHUP_CHROMECAST and device.device_id == CONNECTED_TV) or
            (device.adunit_id == VM_CATCHUP_AMAZONFIRE and device.device_id == CONNECTED_TV) or
            (device.adunit_id == VM_CATCHUP_ANDROIDTV and device.device_id == CONNECTED_TV) or
            (device.adunit_id == VM_CATCHUP_APPLETV and device.device_id == CONNECTED_TV) or
            (device.adunit_id == VM_CATCHUP_ROKU and device.device_id == CONNECTED_TV)]),

        cu_smartphone=reduce(get_adrequest, [device.total_impressions for device in session.query(DailyDashboard).all() 
            if (device.adunit_id == VM_CATCHUP and device.device_id == SMARTPHONE) or 
            (device.adunit_id == VM_CATCHUP_CHROMECAST and device.device_id == SMARTPHONE) or
            (device.adunit_id == VM_CATCHUP_AMAZONFIRE and device.device_id == SMARTPHONE) or
            (device.adunit_id == VM_CATCHUP_ANDROIDTV and device.device_id == SMARTPHONE) or
            (device.adunit_id == VM_CATCHUP_APPLETV and device.device_id == SMARTPHONE) or
            (device.adunit_id == VM_CATCHUP_ROKU and device.device_id == SMARTPHONE)]),

        cu_tablet=reduce(get_adrequest, [device.total_impressions for device in session.query(DailyDashboard).all() 
            if (device.adunit_id == VM_CATCHUP and device.device_id == TABLET) or 
            (device.adunit_id == VM_CATCHUP_CHROMECAST and device.device_id == TABLET) or
            (device.adunit_id == VM_CATCHUP_AMAZONFIRE and device.device_id == TABLET) or
            (device.adunit_id == VM_CATCHUP_ANDROIDTV and device.device_id == TABLET) or
            (device.adunit_id == VM_CATCHUP_APPLETV and device.device_id == TABLET) or
            (device.adunit_id == VM_CATCHUP_ROKU and device.device_id == TABLET)]),

        cu_desktop=reduce(get_adrequest, [device.total_impressions for device in session.query(DailyDashboard).all() 
            if (device.adunit_id == VM_CATCHUP and device.device_id == DESKTOP) or 
            (device.adunit_id == VM_CATCHUP_CHROMECAST and device.device_id == DESKTOP) or
            (device.adunit_id == VM_CATCHUP_AMAZONFIRE and device.device_id == DESKTOP) or
            (device.adunit_id == VM_CATCHUP_ANDROIDTV and device.device_id == DESKTOP) or
            (device.adunit_id == VM_CATCHUP_APPLETV and device.device_id == DESKTOP) or
            (device.adunit_id == VM_CATCHUP_ROKU and device.device_id == DESKTOP)]),

        #LIVE AD UNITS
        live_connected_tv=reduce(get_adrequest, [device.total_impressions for device in session.query(DailyDashboard).all() 
            if (device.adunit_id == LIVEONE and device.device_id == CONNECTED_TV) or 
            (device.adunit_id == LIVEONE_CHROMECAST and device.device_id == CONNECTED_TV) or
            (device.adunit_id == LIVEONE_AMAZONFIRE and device.device_id == CONNECTED_TV) or
            (device.adunit_id == LIVEONE_ANDROID and device.device_id == CONNECTED_TV) or
            (device.adunit_id == LIVEONE_APPLETV and device.device_id == CONNECTED_TV) or
            (device.adunit_id == LIVEONE_ROKU and device.device_id == CONNECTED_TV) or
            (device.adunit_id == LIVETWO and device.device_id == CONNECTED_TV) or
            (device.adunit_id == LIVETWO_CHROMECAST and device.device_id == CONNECTED_TV) or
            (device.adunit_id == LIVETWO_AMAZONFIRE and device.device_id == CONNECTED_TV) or
            (device.adunit_id == LIVETWO_ANDROID and device.device_id == CONNECTED_TV) or
            (device.adunit_id == LIVETWO_APPLETV and device.device_id == CONNECTED_TV) or
            (device.adunit_id == LIVETWO_ROKU and device.device_id == CONNECTED_TV) or
            (device.adunit_id == LIVETHREE and device.device_id == CONNECTED_TV) or
            (device.adunit_id == LIVETHREE_CHROMECAST and device.device_id == CONNECTED_TV) or
            (device.adunit_id == LIVETHREE_AMAZONFIRE and device.device_id == CONNECTED_TV) or
            (device.adunit_id == LIVETHREE_ANDROID and device.device_id == CONNECTED_TV) or
            (device.adunit_id == LIVETHREE_APPLETV and device.device_id == CONNECTED_TV) or
            (device.adunit_id == LIVETHREE_ROKU and device.device_id == CONNECTED_TV)]),

        live_smartphone=reduce(get_adrequest, [device.total_impressions for device in session.query(DailyDashboard).all() 
            if (device.adunit_id == LIVEONE and device.device_id == SMARTPHONE) or 
            (device.adunit_id == LIVEONE_CHROMECAST and device.device_id == SMARTPHONE) or
            (device.adunit_id == LIVEONE_AMAZONFIRE and device.device_id == SMARTPHONE) or
            (device.adunit_id == LIVEONE_ANDROID and device.device_id == SMARTPHONE) or
            (device.adunit_id == LIVEONE_APPLETV and device.device_id == SMARTPHONE) or
            (device.adunit_id == LIVEONE_ROKU and device.device_id == SMARTPHONE) or
            (device.adunit_id == LIVETWO and device.device_id == SMARTPHONE) or
            (device.adunit_id == LIVETWO_CHROMECAST and device.device_id == SMARTPHONE) or
            (device.adunit_id == LIVETWO_AMAZONFIRE and device.device_id == SMARTPHONE) or
            (device.adunit_id == LIVETWO_ANDROID and device.device_id == SMARTPHONE) or
            (device.adunit_id == LIVETWO_APPLETV and device.device_id == SMARTPHONE) or
            (device.adunit_id == LIVETWO_ROKU and device.device_id == SMARTPHONE) or
            (device.adunit_id == LIVETHREE and device.device_id == SMARTPHONE) or
            (device.adunit_id == LIVETHREE_CHROMECAST and device.device_id == SMARTPHONE) or
            (device.adunit_id == LIVETHREE_AMAZONFIRE and device.device_id == SMARTPHONE) or
            (device.adunit_id == LIVETHREE_ANDROID and device.device_id == SMARTPHONE) or
            (device.adunit_id == LIVETHREE_APPLETV and device.device_id == SMARTPHONE) or
            (device.adunit_id == LIVETHREE_ROKU and device.device_id == SMARTPHONE)]),

        live_tablet=reduce(get_adrequest, [device.total_impressions for device in session.query(DailyDashboard).all() 
            if (device.adunit_id == LIVEONE and device.device_id == TABLET) or 
            (device.adunit_id == LIVEONE_CHROMECAST and device.device_id == TABLET) or
            (device.adunit_id == LIVEONE_AMAZONFIRE and device.device_id == TABLET) or
            (device.adunit_id == LIVEONE_ANDROID and device.device_id == TABLET) or
            (device.adunit_id == LIVEONE_APPLETV and device.device_id == TABLET) or
            (device.adunit_id == LIVEONE_ROKU and device.device_id == TABLET) or
            (device.adunit_id == LIVETWO and device.device_id == TABLET) or
            (device.adunit_id == LIVETWO_CHROMECAST and device.device_id == TABLET) or
            (device.adunit_id == LIVETWO_AMAZONFIRE and device.device_id == TABLET) or
            (device.adunit_id == LIVETWO_ANDROID and device.device_id == TABLET) or
            (device.adunit_id == LIVETWO_APPLETV and device.device_id == TABLET) or
            (device.adunit_id == LIVETWO_ROKU and device.device_id == TABLET) or
            (device.adunit_id == LIVETHREE and device.device_id == TABLET) or
            (device.adunit_id == LIVETHREE_CHROMECAST and device.device_id == TABLET) or
            (device.adunit_id == LIVETHREE_AMAZONFIRE and device.device_id == TABLET) or
            (device.adunit_id == LIVETHREE_ANDROID and device.device_id == TABLET) or
            (device.adunit_id == LIVETHREE_APPLETV and device.device_id == TABLET) or
            (device.adunit_id == LIVETHREE_ROKU and device.device_id == TABLET)]),

        live_desktop=reduce(get_adrequest, [device.total_impressions for device in session.query(DailyDashboard).all() 
            if (device.adunit_id == LIVEONE and device.device_id == DESKTOP) or 
            (device.adunit_id == LIVEONE_CHROMECAST and device.device_id == DESKTOP) or
            (device.adunit_id == LIVEONE_AMAZONFIRE and device.device_id == DESKTOP) or
            (device.adunit_id == LIVEONE_ANDROID and device.device_id == DESKTOP) or
            (device.adunit_id == LIVEONE_APPLETV and device.device_id == DESKTOP) or
            (device.adunit_id == LIVEONE_ROKU and device.device_id == DESKTOP) or
            (device.adunit_id == LIVETWO and device.device_id == DESKTOP) or
            (device.adunit_id == LIVETWO_CHROMECAST and device.device_id == DESKTOP) or
            (device.adunit_id == LIVETWO_AMAZONFIRE and device.device_id == DESKTOP) or
            (device.adunit_id == LIVETWO_ANDROID and device.device_id == DESKTOP) or
            (device.adunit_id == LIVETWO_APPLETV and device.device_id == DESKTOP) or
            (device.adunit_id == LIVETWO_ROKU and device.device_id == DESKTOP) or
            (device.adunit_id == LIVETHREE and device.device_id == DESKTOP) or
            (device.adunit_id == LIVETHREE_CHROMECAST and device.device_id == DESKTOP) or
            (device.adunit_id == LIVETHREE_AMAZONFIRE and device.device_id == DESKTOP) or
            (device.adunit_id == LIVETHREE_ANDROID and device.device_id == DESKTOP) or
            (device.adunit_id == LIVETHREE_APPLETV and device.device_id == DESKTOP) or
            (device.adunit_id == LIVETHREE_ROKU and device.device_id == DESKTOP)]),
    )

    session.add(impressions_data)
    session.commit()

def populate_sell_through_rate():
    sell_through_rate_data = SellThroughRate(
        cu_connected_tv=reduce(get_adrequest, [device.total_adrequests for device in session.query(DailyDashboard).all() 
            if (device.adunit_id == VM_CATCHUP and device.device_id == CONNECTED_TV) or 
            (device.adunit_id == VM_CATCHUP_CHROMECAST and device.device_id == CONNECTED_TV) or
            (device.adunit_id == VM_CATCHUP_AMAZONFIRE and device.device_id == CONNECTED_TV) or
            (device.adunit_id == VM_CATCHUP_ANDROIDTV and device.device_id == CONNECTED_TV) or
            (device.adunit_id == VM_CATCHUP_APPLETV and device.device_id == CONNECTED_TV) or
            (device.adunit_id == VM_CATCHUP_ROKU and device.device_id == CONNECTED_TV)])/
            reduce(get_adrequest, [device.total_response_served for device in session.query(DailyDashboard).all() 
            if (device.adunit_id == VM_CATCHUP and device.device_id == CONNECTED_TV) or 
            (device.adunit_id == VM_CATCHUP_CHROMECAST and device.device_id == CONNECTED_TV) or
            (device.adunit_id == VM_CATCHUP_AMAZONFIRE and device.device_id == CONNECTED_TV) or
            (device.adunit_id == VM_CATCHUP_ANDROIDTV and device.device_id == CONNECTED_TV) or
            (device.adunit_id == VM_CATCHUP_APPLETV and device.device_id == CONNECTED_TV) or
            (device.adunit_id == VM_CATCHUP_ROKU and device.device_id == CONNECTED_TV)])
    )
        
    session.add(sell_through_rate_data)
    session.commit()


def get_adrequest(x, y):
    return x + y


if __name__ == '__main__':
    Base.metadata.create_all(engine)
    start()