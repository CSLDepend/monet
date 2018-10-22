import time
import datetime
import os

from dateutil import tz
from struct import unpack

import pyspark.sql.functions as F

from pyspark.sql.window import Window

# Remove the 0 from '10' when doing later years data
fix_stats = ['nettopo_mesh_coord_Z',
             'nettopo_mesh_coord_Y',
             'nettopo_mesh_coord_X',
             'Z+_SAMPLE_GEMINI_LINK_CREDIT_STALL__%_x1e6_',
             'Y+_SAMPLE_GEMINI_LINK_CREDIT_STALL__%_x1e6_',
             'X+_SAMPLE_GEMINI_LINK_CREDIT_STALL__%_x1e6_',
             'Z+_SAMPLE_GEMINI_LINK_INQ_STALL__%_x1e6_',
             'Y+_SAMPLE_GEMINI_LINK_INQ_STALL__%_x1e6_',
             'X+_SAMPLE_GEMINI_LINK_INQ_STALL__%_x1e6_',
             'Z-_SAMPLE_GEMINI_LINK_CREDIT_STALL__%_x1e6_',
             'Y-_SAMPLE_GEMINI_LINK_CREDIT_STALL__%_x1e6_',
             'X-_SAMPLE_GEMINI_LINK_CREDIT_STALL__%_x1e6_',
             'Z-_SAMPLE_GEMINI_LINK_INQ_STALL__%_x1e6_',
             'Y-_SAMPLE_GEMINI_LINK_INQ_STALL__%_x1e6_',
             'X-_SAMPLE_GEMINI_LINK_INQ_STALL__%_x1e6_']

renamed = [  'Z',   'Y',   'X',
           'ZC+', 'YC+', 'XC+',
           'ZI+', 'YI+', 'XI+',
           'ZC-', 'YC-', 'XC-',
           'ZI-', 'YI-', 'XI-']
avged = [       'Z',        'Y',        'X',
         'avg(ZC+)', 'avg(YC+)', 'avg(XC+)',
         'avg(ZI+)', 'avg(YI+)', 'avg(XI+)',
         'avg(ZC-)', 'avg(YC-)', 'avg(XC-)',
         'avg(ZI-)', 'avg(YI-)', 'avg(XI-)']

#cwind = Window.partitionBy('bucket')

header_fmt = """\
VERSION .7
FIELDS x y z rgb
SIZE 4 4 4 4
TYPE F F F F
COUNT 1 1 1 1
WIDTH {}
HEIGHT 1
VIEWPOINT 0 0 0 1 0 0 0
POINTS {}
DATA ascii
"""

### Utility ###
def from_lts(lts):
    return datetime.datetime.fromtimestamp(lts, tz.gettz('America/Chicago'))

def to_lts(dt):
    import os
    os.environ['TZ'] = 'America/Chicago'
    time.tzset()
    return time.mktime(dt.timetuple())

def stall_to_float(credit, inq):
    # Credit -> R, Inq -> G
    # Scaled to 0-255, not rounding
    cr = int(2.55 * float(credit)/1000000)
    iq = int(2.55 * float(inq)/1000000)
    b = bytes([0, iq & 0xff, cr & 0xff, 0]) # little endian
    return unpack('<f', b)[0]

def get_bucketfile(d, b):
    return os.path.join(d, 'b_{:04}.pcd'.format(b))