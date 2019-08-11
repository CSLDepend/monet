# Copyright (c) 2019 DEPEND Research Group at
# University of Illinois, Urbana Champaign (UIUC)
# This work is licensed under the terms of the UIUC/NCSA license.
# For a copy, see https://opensource.org/licenses/NCSA.

import os
import sys
from multiprocessing import Pool
def create_segments(args):
    file_path = args[0]
    output_path = args[1]
    bucket = args[2]
    print('PCL Segmentation running for bucket {}'.format(bucket))
    file_name = file_path+'b_{:04}.pcd'.format(bucket)
    output_name = output_path+'{:04}'.format(bucket)
    os.system('/monet/src/rgselect_unique {} 2 {} -p 4 -r 4 -d 2 -s 20 > {}'.format(file_name,bucket,output_name))

input_path = sys.argv[1]
output_path = sys.argv[2]
p = Pool(30)
args = [[input_path,output_path,bucket] for bucket in range(1,1441)]
p.map(create_segments,args)
