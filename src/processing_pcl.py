#run as python3 date
#import python libraries
import os
import sys
from unix_timestamp import *
import argparse

#initialize directory structure
pcdDir = '/outputs/pcd/'
segmentDir = '/outputs/segment/'
curDir = '/monet/src/'


# run the steps:
# 1. pcd -> segments
# 2. segments -> regions

def main():
    date = sys.argv[1]
    timestamp = str(get_unix_time(date))
    
    #pcd to segment
    os.system('python3 /monet/src/rgrunner_unique.py single 20 %s 2 4 4 2 20 %s %s'%(pcdDir+date,segmentDir[:-1],curDir))
    
    #segment to regions
    

if __name__ == "__main__":
    main()
