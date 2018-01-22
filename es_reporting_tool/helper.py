from datetime import datetime
import os

def format_time(time):
    time = time.rstrip('Z')
    split_time = time.split('T')
    new_time = split_time[0] + " " + split_time[1]
    return new_time
    
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