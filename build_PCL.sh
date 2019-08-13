# Copyright (c) 2019 DEPEND Research Group at
# University of Illinois, Urbana Champaign (UIUC)
# This work is licensed under the terms of the UIUC/NCSA license.
# For a copy, see https://opensource.org/licenses/NCSA.

docker run --rm  -v `pwd`:/monet/ -ti monet_pcl:latest /bin/bash -c "cd /monet/src/pcl; mkdir build; cd build; cmake ..; make; cp rgselect_unique ../../rgselect_unique"
