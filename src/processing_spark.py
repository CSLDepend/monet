#run as python3 date
#import python libraries
import os
import sys
from unix_timestamp import *
import argparse

#initialize directory structure
rawDir = '/data/'
parquetDir = '/outputs/processed/'
pcdDir = '/outputs/pcd/'
segmentDir = '/outputs/segment/'
descriptorFile = '/monet/src/HEADER.20160115-'

# run the steps:
# 1. raw ovis -> parquet
# 2. parquet -> pcd

def main():
    date = sys.argv[1]
    timestamp = str(get_unix_time(date))
    
    #create directory structure
    os.system('mkdir -p '+parquetDir)
    os.system('mkdir -p '+pcdDir)
    os.system('mkdir -p '+segmentDir)
    os.system('mkdir '+pcdDir+date)
        
    #raw ovis to parquet convert
    os.system('python /monet/src/ovis_data_converter.py %s %s %s %s'%(rawDir,parquetDir,descriptorFile,date))
    
    #parquet to pcd
    os.system('python /monet/src/bw_workflow.py %s %s %s %s'%(date,timestamp,parquetDir[:-1],pcdDir[:-1]))

if __name__ == "__main__":
    main()
