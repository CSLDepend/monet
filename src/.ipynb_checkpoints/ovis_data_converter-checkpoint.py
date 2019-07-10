### run like python3 rawDir parquetDir descriptorfile date

import sys
## enable arguments
argp = True
if(argp):
    rawDir = sys.argv[1]
    parquetDir = sys.argv[2]
    descriptorFile = sys.argv[3]
    date = int(sys.argv[4])

#configure spark settings
def get_spark_context():
    import pyspark
    from pyspark.sql import SparkSession
    conf = pyspark.SparkConf()
    conf.setMaster("local[40]")
    conf.set("spark.driver.memory", "10g")
    conf.set("spark.executor.memory", "40g")
    conf.set("spark.core.connection.ack.wait.timeout", "1200")
    conf.set("spark.rpc.netty.dispatcher.numThreads","4")
    conf.set("spark.driver.maxResultSize", "10g")
    conf.setAppName('Monet')
    sc = pyspark.SparkContext(conf=conf)
    spark = SparkSession.builder.getOrCreate()
    return spark

#import python libraries 
import os
import pandas as pd
import sys
from pprint import pprint
from os.path  import basename
from pprint import pprint
from operator import itemgetter
import time
from pathlib import Path
import os.path
import shutil

#import pyspark libraries
import pyspark
from pyspark.sql.window import Window
from pyspark.sql.functions import *
import pyspark.sql.functions as sf
from pyspark.sql.types import *
from pyspark import *
from pyspark.sql import *
from pyspark.sql import Row
from collections import OrderedDict
from pyspark.sql import SparkSession
from pprint import pprint


spark = get_spark_context()

#get list of ovis files to process
rawFiles = [rawDir+str(date)]
print(rawFiles)


#create consistent column names
def tidy(s):
    st = s
    for c in " ,;{}()\n\t=#.":
        st = st.replace(c, '_')
    return st

#create schema for pyspark dataframe
with open(descriptorFile, 'r') as f:
    fnames = [l.strip() for l in f.readline().strip().split(',')]
    fnames = map(tidy, fnames)
    fields = [StructField(fname, DecimalType(20, 0), True) for fname in fnames]
    fields[0] = StructField('#Time', DecimalType(38, 18), True)
    ovis_schema = StructType(fields)

#create parquet files from raw ovis csvs
for rF in rawFiles:
    df = spark.read.csv(rF, schema=ovis_schema, ignoreLeadingWhiteSpace=True,
                        ignoreTrailingWhiteSpace=True, mode='DROPMALFORMED')
    if not os.path.isfile(parquetDir+'{}/_SUCCESS'.format(basename(rF))):
        dfDir = parquetDir+'{}'.format(basename(rF))
        if os.path.isdir(dfDir):
            print("File not written correctly, rerunning for %s file" % basename(rF))
            shutil.rmtree(dfDir)
        df.repartition(50).write.parquet(parquetDir+'{}'.format(basename(rF)))
        print("processed file %s" % rF)