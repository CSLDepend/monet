### run like python3 rawDir parquetDir descriptorfile date

import sys
#########
argp = True
rawDir = "../data/ovis_greg/"
parquetDir = '../data/processed/'
descriptorFile = "../data/HEADER.20160115-"
date = 20181001

if(argp):
    rawDir = sys.argv[1]
    parquetDir = sys.argv[2]
    descriptorFile = sys.argv[3]
    date = int(sys.argv[4])
########


def get_spark_context():
    import pyspark
    from pyspark.sql import SparkSession
    conf = pyspark.SparkConf()
    conf.setMaster("local[40]")#"mesos://10.0.1.201:5050"
    # conf.set("spark.cores.max", "44")
    conf.set("spark.driver.memory", "100g")
    conf.set("spark.executor.memory", "300g")
    conf.set("spark.core.connection.ack.wait.timeout", "1200")
    conf.set("spark.rpc.netty.dispatcher.numThreads","4")
    conf.set("spark.driver.maxResultSize", "200g")
    conf.set("spark.python.worker.memory", "5g")
    conf.setAppName('Monet-hello')
    # create the context
    sc = pyspark.SparkContext(conf=conf)
    spark = SparkSession.builder.getOrCreate()
    return spark
import os
import pyspark

# import python libraries 
import pandas as pd
import sys
from pprint import pprint
from os.path  import basename
from pprint import pprint
from operator import itemgetter
import time

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
# rawFiles = [rawDir + f for f in os.listdir(rawDir)]
rawFiles = [rawDir+str(date)]
# rawFiles = ["../data/raw/201805{:02}".format(d) for d in range(2, 32)]
print(rawFiles)
def tidy(s):
    st = s
    for c in " ,;{}()\n\t=#.":
        st = st.replace(c, '_')
    return st
with open(descriptorFile, 'r') as f:
    fnames = [l.strip() for l in f.readline().strip().split(',')]
    fnames = map(tidy, fnames)
    fields = [StructField(fname, DecimalType(20, 0), True) for fname in fnames]
    fields[0] = StructField('#Time', DecimalType(38, 18), True)
    ovis_schema = StructType(fields)
   # pprint(ovis_schema)

from pathlib import Path
import os.path
import shutil

for rF in rawFiles:
    df = spark.read.csv(rF, schema=ovis_schema, ignoreLeadingWhiteSpace=True,
                        ignoreTrailingWhiteSpace=True, mode='DROPMALFORMED')
    if not os.path.isfile(parquetDir+'{}/_SUCCESS'.format(basename(rF))):
        dfDir = parquetDir+'{}'.format(basename(rF))
        if os.path.isdir(dfDir):
            print("blame gluster for not writing correctly, rerunning for %s file" % basename(rF))
            shutil.rmtree(dfDir)
        df.repartition(50).write.parquet(parquetDir+'{}'.format(basename(rF)))
        print("processed file %s" % rF)
