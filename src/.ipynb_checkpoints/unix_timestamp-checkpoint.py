import time
import datetime
def get_unix_time(s):
    s = str(s)
    return int(time.mktime(datetime.datetime.strptime(s, "%Y%m%d").timetuple()))

