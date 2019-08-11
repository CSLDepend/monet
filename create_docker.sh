#!/bin/bash

# Copyright (c) 2019 DEPEND Research Group at
# University of Illinois, Urbana Champaign (UIUC)
# This work is licensed under the terms of the UIUC/NCSA license.
# For a copy, see https://opensource.org/licenses/NCSA.

docker build -t monet_pcl:latest -f ./Dockerfile_pcl.base .
docker build -t monet_spark:latest -f ./Dockerfile_spark.base .
