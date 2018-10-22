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






# Things to do:
# 1) Logs -> Parquet
# 2) Parquet -> PCD
# 3) PCD -> Segmented Regions

import os
import sys
import subprocess
import itertools

import pyspark.sql.functions as F

from pyspark.sql.window import Window

from pyspark import *
from pyspark.sql.types import *

### Ovis -> Parquet ###
def parse_schema(schema_file):
    def tidy(s):
        st = s
        for c in " ,;{}()\n\t=#.":
            st = st.replace(c, '_')
        return st

    with open(schema_file, 'r') as f:
        fnames = [l.strip() for l in f.readline().strip().split(',')]
        fnames = map(tidy, fnames)
        fields = [StructField(fname, DecimalType(20, 0), True) for fname in fnames]
        fields[0] = StructField('#Time', DecimalType(38, 18), True)
        ovis_schema = StructType(fields)
    return ovis_schema

def ovis_to_parquet(spark, schema_file, ovis_file, parquet_out):
    ovis_schema = parse_schema(schema_file)
    
    df = spark.read.csv(ovis_file, schema=ovis_schema, ignoreLeadingWhiteSpace=True,
                        ignoreTrailingWhiteSpace=True, mode='DROPMALFORMED')
    df.write.parquet(parquet_out)

### Parquet -> PCD ###
def parquet_to_pcd(spark, day_parquet, day_store_dir, day_base_timestamp, min_bucket=1, max_bucket=1441):
    def max_rows(rs):
        maxes = []
        for feat in renamed[3:]:
            maxes.append(max([r[feat] for r in rs]))
        return maxes

    def create_rows(z, y, x,
                    zc, yc, xc,
                    zi, yi, xi,
                    zcw, ycw, xcw,
                    ziw, yiw, xiw,
                    zcl, ycl, xcl,
                    zil, yil, xil):
        # Choose correct maximum values, including when lead is null
        xcf = max(xc, xcw) if xcl is None else max(xc, xcl)
        ycf = max(yc, ycw) if ycl is None else max(yc, ycl)
        zcf = max(zc, zcw) if zcl is None else max(zc, zcl)
        xif = max(xi, xiw) if xil is None else max(xi, xil)
        yif = max(yi, yiw) if yil is None else max(yi, yil)
        zif = max(zi, ziw) if zil is None else max(zi, zil)

        # Create points
        fs = '{} {} {} {}\n{} {} {} {}\n{} {} {} {}\n'
        xf, yf, zf = float(x), float(y), float(z)
        xrgb = stall_to_float(xcf, xif)
        yrgb = stall_to_float(ycf, yif)
        zrgb = stall_to_float(zcf, zif)
        s = fs.format(xf + 0.5, yf, zf, xrgb,
                      xf, yf + 0.5, zf, yrgb,
                      xf, yf, zf + 0.5, zrgb)
        return s

    def with_cols(df, names, cols):
        for n, c in zip(names, cols):
            df = df.withColumn(n, c)
        return df

    def rename_cols(df, old, new):
        for o, n in zip(old, new):
            df = df.withColumnRenamed(o, n)
        return df

    def create_filestring_tup(r):
        l = r[1][1] * 3
        g = header_fmt.format(l, l)
        return (r[0], g + r[1][0])

    try: os.mkdir(day_store_dir)
    except OSError: pass
    
    # Add bucket
    df = spark.read.parquet(day_parquet)
    df = df.withColumn('bucket', F.ceil((F.col('#Time') - day_base_timestamp + 30)/60))
    
    # Filter
    df = df.where((F.col('bucket') >= min_bucket) &\
                  (F.col('bucket') < max_bucket))

    # Max between the 2 compids and any extra readings
    df = df.select('bucket', *fix_stats)
    df = df.na.fill({k: 0 for k in fix_stats[3:]}) # fill nulls with 0
    df = rename_cols(df, fix_stats, renamed)
    rdd = df.rdd
    rdd = rdd.map(lambda r: ((r['bucket'], r['Z'], r['Y'], r['X']), [r]))\
             .reduceByKey(lambda a, b: a + b)\
             .map(lambda kv: list(map(int, list(kv[0]) + max_rows(kv[1]))))
    df = spark.createDataFrame(rdd, ['bucket'] + renamed)

    # Add corresponding minus directions
    xw = Window.partitionBy('bucket', 'Z', 'Y').orderBy('X')
    yw = Window.partitionBy('bucket', 'Z', 'X').orderBy('Y')
    zw = Window.partitionBy('bucket', 'Y', 'X').orderBy('Z')

    wrap_names = ['ZC_wrap', 'YC_wrap', 'XC_wrap',
                  'ZI_wrap', 'YI_wrap', 'XI_wrap']
    wrap_cols = [F.first('ZC-').over(zw), F.first('YC-').over(yw), F.first('XC-').over(xw),
                 F.first('ZI-').over(zw), F.first('YI-').over(yw), F.first('XI-').over(xw)]

    lead_names = ['ZC_lead', 'YC_lead', 'XC_lead',
                  'ZI_lead', 'YI_lead', 'XI_lead']
    lead_cols = [F.lead('ZC-').over(zw), F.lead('YC-').over(yw), F.lead('XC-').over(xw),
                 F.lead('ZI-').over(zw), F.lead('YI-').over(yw), F.lead('XI-').over(xw)]

    df = with_cols(df, wrap_names, wrap_cols)
    df = with_cols(df, lead_names, lead_cols)
    df = df.drop(*renamed[9:])    

    # Calculate string
    str_args = renamed[:9] + wrap_names + lead_names
    udf_create_rows = F.udf(create_rows, StringType())
    df = df.withColumn('pt_string', udf_create_rows(*str_args))\
           .drop(*str_args)

    # Count and add headers
    rdd = df.rdd
    rdd = rdd.map(lambda r: (r['bucket'], [r['pt_string'], 1]))\
             .reduceByKey(lambda s1, s2: [s1[0] + s2[0], s1[1] + s2[1]])\
             .map(create_filestring_tup)
    file_contents = rdd.collect()

    for b, contents in file_contents:
        with open(get_bucketfile(day_store_dir, b), 'w+') as f:
            f.write(contents)
            
### PCD -> Segmented Regions ###
def pcd_to_regions(rgselect, pcds_dir, regions_out, rgtype=2, p=4, r=4, d=2, s=20, bstart=1, bend=1441):
    reg_str = ''
    for b in range(bstart, bend):
        pcd_file = get_bucketfile(pcds_dir, b)
        segments = subprocess.check_output([rgselect, pcd_file, rgtype, str(b),
                                            '-p', str(p), '-r', str(r),
                                            '-d', str(d), '-s', str(s)])
    reg_str += str(segments)

    with open(regions_out, 'w+') as f:
        f.write(reg_str)


import sys
#####
argp = True
PQ_DIR = 'data/processed'
PCD_DIR = 'data/pcd'
if(argp):
	PQ_DIR = sys.argv[3]
	PCD_DIR = sys.argv[4]
#####


import os

# spark
import pyspark
from pyspark.sql import SparkSession

# import python libraries 
import pandas as pd
import sys
from pprint import pprint
from os.path  import basename
from pprint import pprint
from operator import itemgetter
import time
import numpy as np
import pandas as pd
import time
from pprint import pprint
import datetime



import os

def process(file_name,timestamp):
	conf = pyspark.SparkConf()
	conf.setMaster("local[12]")
	# set other options as desired
	conf.set("spark.driver.memory", "60g")
	conf.set("spark.executor.memory", "200g")
	conf.set("spark.core.connection.ack.wait.timeout", "1200")
	conf.setAppName('API_test')
	conf.set("spark.rpc.netty.dispatcher.numThreads","2")
	# create the context
	sc = pyspark.SparkContext(conf=conf)
	#get sql session
	spark = SparkSession.builder.getOrCreate()

	MAY_12_TS = 1526101200
	MAY_13_TS = 1526187600
	MAY_20_TS = 1526792400
	MAY_21_TS = 1526878800	
	MAY_22_TS = 1526965200
	MAY_23_TS = 1527051600
	MAY_24_TS = 1527138000
	MAY_25_TS = 1527224400

	PQ_ARR = [file_name]
	TS_ARR = [timestamp]

	for PQ, TS in zip(PQ_ARR, TS_ARR):
		parquet_to_pcd(spark, os.path.join(PQ_DIR, PQ), os.path.join(PCD_DIR, PQ), TS, max_bucket=720)
		parquet_to_pcd(spark, os.path.join(PQ_DIR, PQ), os.path.join(PCD_DIR, PQ), TS, min_bucket=720)
		print('[+] Done ' + PQ)

process(sys.argv[1],int(sys.argv[2]))
