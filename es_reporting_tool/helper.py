from datetime import datetime, date, timedelta
from dateutil import tz
import os

def format_time(time):
    time = time.rstrip('Z')
    split_time = time.split('T')
    new_time = split_time[0] + " " + split_time[1]
    return get_ist(new_time).strftime("%Y-%m-%d %H:%M:%S")
    
def get_time():
    time_now = str(datetime.now()).split()
    return time_now[0] + ':' + time_now[1].split('.')[0]
    
def report_name():
    try:
        os.makedirs('reports')
    except OSError as e:
        pass
    report_name = os.getcwd() + '/reports/report_' + get_time() + '.pdf'
    return report_name
    
def y_date():
    yesterday = date.today() - timedelta(100)
    return yesterday


def get_ist(time):
    # METHOD 1: Hardcode zones:
    from_zone = tz.gettz('UTC')
    to_zone = tz.gettz('IST')

    # utc = datetime.utcnow()
    utc = datetime.strptime(time, '%Y-%m-%d %H:%M:%S.%f')

    # Tell the datetime object that it's in UTC time zone since
    # datetime objects are 'naive' by default
    utc = utc.replace(tzinfo=from_zone)

    # Convert time zone
    isttime = utc.astimezone(to_zone)

    return isttime
