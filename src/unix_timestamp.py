# Copyright (c) 2019 DEPEND Research Group at
# University of Illinois, Urbana Champaign (UIUC)
# This work is licensed under the terms of the UIUC/NCSA license.
# For a copy, see https://opensource.org/licenses/NCSA.

import time
import datetime
def get_unix_time(s):
    s = str(s)
    return int(time.mktime(datetime.datetime.strptime(s, "%Y%m%d").timetuple()))

