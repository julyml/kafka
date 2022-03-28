import time
import datetime

def convert(str_date):
    return int(time.mktime(datetime.datetime.strptime(str_date,'%m/%d/%Y %H:%M:%S').timetuple()))

print(convert('04/14/2021 13:00:00'))