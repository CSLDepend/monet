#run as python3 date
#import python libraries


# Copyright (c) 2019 DEPEND Research Group at
# University of Illinois, Urbana Champaign (UIUC)
# This work is licensed under the terms of the UIUC/NCSA license.
# For a copy, see https://opensource.org/licenses/NCSA.

import os
import sys
import argparse
import shutil

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
    
    #pcd to segment
    os.system('python3 /monet/src/segmentation.py %s %s'%(pcdDir+date+'/',segmentDir+date+'/'))

    #concatenate all files into one
    with open(segmentDir+'regions_'+date,'wb') as wfd:
        for f in [segmentDir+date+'/'+'{:04}'.format(i) for i in range(1,1441)]:
            with open(f,'rb') as fd:
                shutil.copyfileobj(fd, wfd)
    
    #segment to regions
    os.system('python3 /monet/src/segment_to_region.py %s %s'%(segmentDir+'regions_'+date,regionDir+date))

if __name__ == "__main__":
    main()
