#!/bin/bash
docker build -t monet_pcl:latest -f ./Dockerfile_pcl.base .
docker build -t monet_spark:latest -f ./Dockerfile_spark.base .