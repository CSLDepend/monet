#run as python3 date
#import python libraries
import os
import sys
from unix_timestamp import *
import argparse

#initialize directory structure
pcdDir = '/outputs/pcd/'
segmentDir = '/outputs/segment/'
regionDir = '/outputs/region/'
curDir = '/monet/src/'


# run the steps:
# 1. pcd -> segments
# 2. segments -> regions

def main():
    date = sys.argv[1]
    timestamp = str(get_unix_time(date))
    
    #pcd to segment
    os.system('python3 /monet/src/segmentation.py %s %s'%(pcdDir+date+'/',segmentDir+date+'/'))
    os.system('cat %s/* >> %s'%(segmentDir+date,segmentDir+'regions_'+date))
    
    #segment to regions
    os.system('python3 /monet/src/segment_to_region.py %s %s'%(segmentDir+'regions_'+date,regionDir+date))

if __name__ == "__main__":
    main()
