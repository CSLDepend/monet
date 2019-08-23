# Copyright (c) 2019 DEPEND Research Group at
# University of Illinois, Urbana Champaign (UIUC)
# This work is licensed under the terms of the UIUC/NCSA license.
# For a copy, see https://opensource.org/licenses/NCSA.


#run as python3 date
#import python libraries
import os
import sys
import argparse

#initialize directory structure
rawDir = '/data/'
parquetDir = '/outputs/processed/'
pcdDir = '/outputs/pcd/'
segmentDir = '/outputs/segment/'
regionDir = '/outputs/region/'
descriptorFile = '/monet/src/HEADER'

# run the steps:
# 1. raw ovis -> parquet
# 2. parquet -> pcd

def main():
    date = sys.argv[1]
    timestamp = sys.argv[2]
    
    #create directory structure
    os.system('mkdir -p '+parquetDir)
    os.system('mkdir -p '+pcdDir)
    os.system('mkdir -p '+segmentDir)
    os.system('mkdir -p '+regionDir)
    os.system('mkdir -p '+pcdDir+date)
    os.system('mkdir -p '+segmentDir+date)
        
    #raw ovis to parquet convert
    os.system('python /monet/src/ovis_data_converter.py %s %s %s %s'%(rawDir,parquetDir,descriptorFile,date))
    
    #parquet to pcd
    os.system('python /monet/src/bw_workflow.py %s %s %s %s'%(date,timestamp,parquetDir[:-1],pcdDir[:-1]))

if __name__ == "__main__":
    main()
