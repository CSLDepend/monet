# Copyright (c) 2019 DEPEND Research Group at
# University of Illinois, Urbana Champaign (UIUC)
# This work is licensed under the terms of the UIUC/NCSA license.
# For a copy, see https://opensource.org/licenses/NCSA.


#run as python3 date timestamp parquetDir pcdDir
#import python libraries
import time
import datetime
import os
from dateutil import tz
from struct import unpack
import sys
import subprocess
import itertools

import pyspark
from pyspark import *
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

import pandas as pd
import numpy as np
from pprint import pprint
from os.path  import basename
from pprint import pprint
from operator import itemgetter

#original column names
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


#concise column names
renamed = [  'Z',   'Y',   'X',
           'ZC+', 'YC+', 'XC+',
           'ZI+', 'YI+', 'XI+',
           'ZC-', 'YC-', 'XC-',
           'ZI-', 'YI-', 'XI-']

#averaged out column names
avged = [       'Z',        'Y',        'X',
         'avg(ZC+)', 'avg(YC+)', 'avg(XC+)',
         'avg(ZI+)', 'avg(YI+)', 'avg(XI+)',
         'avg(ZC-)', 'avg(YC-)', 'avg(XC-)',
         'avg(ZI-)', 'avg(YI-)', 'avg(XI-)']


#format of PCD header
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


#compress credit and inq stalls to RGB color decimal(pcd format)
def stall_to_float(credit, inq):
    # Credit -> R, Inq -> G
    # Scaled to 0-255, not rounding
    cr = int(2.55 * float(credit)/1000000)
    iq = int(2.55 * float(inq)/1000000)
    b = bytes([0, iq & 0xff, cr & 0xff, 0]) # little endian
    return unpack('<f', b)[0]

#get file path for each bucket
def get_bucketfile(d,b):
    return os.path.join(d, 'b_{:04}.pcd'.format(b))

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

PQ_DIR = sys.argv[3]
PCD_DIR = sys.argv[4]

def process(file_name,timestamp):
	# set spark config
	conf = pyspark.SparkConf()
	conf.setMaster("local[12]")
	conf.set("spark.driver.memory", "60g")
	conf.set("spark.executor.memory", "200g")
	conf.set("spark.core.connection.ack.wait.timeout", "1200")
	conf.setAppName('API_test')
	conf.set("spark.rpc.netty.dispatcher.numThreads","2")
	# create the context
	sc = pyspark.SparkContext(conf=conf)
	#get sql session
	spark = SparkSession.builder.getOrCreate()

	PQ_ARR = [file_name]
	TS_ARR = [timestamp]
    
    #parquet to pcd by dividing each day into 2 parts
	for PQ, TS in zip(PQ_ARR, TS_ARR):
		parquet_to_pcd(spark, os.path.join(PQ_DIR, PQ), os.path.join(PCD_DIR, PQ), TS, max_bucket=720)
		parquet_to_pcd(spark, os.path.join(PQ_DIR, PQ), os.path.join(PCD_DIR, PQ), TS, min_bucket=720)
		print('[+] Done ' + PQ)

process(sys.argv[1],int(sys.argv[2]))
