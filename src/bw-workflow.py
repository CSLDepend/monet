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


# viz
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from mpl_toolkits.mplot3d import Axes3D
import seaborn as sns
sns.set_style('whitegrid')
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="matplotlib")
import seaborn as sns
from __future__ import print_function
from ipywidgets import interact, interactive, fixed, interact_manual
import ipywidgets as widgets

import monet_bw
import monet_viz as mv

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
	PQ_DIR = '.data/processed'
	PCD_DIR = 'data/pcd'

	for PQ, TS in zip(PQ_ARR, TS_ARR):
    	monet_bw.parquet_to_pcd(spark, os.path.join(PQ_DIR, PQ), os.path.join(PCD_DIR, PQ), TS, max_bucket=720)
    	monet_bw.parquet_to_pcd(spark, os.path.join(PQ_DIR, PQ), os.path.join(PCD_DIR, PQ), TS, min_bucket=720)
    	print('[+] Done ' + PQ)
