#!/bin/bash
args=( $@ )
arglen=${#args[@]}
if [ $arglen -ne 2 ] 
then
	echo "Ensure arguments to be: <data-dir> <output-dir>"
	exit 1 
fi
DATA=`realpath $1`
OUTPUTS=`realpath $2`

docker run -v $DATA/:/data -v /dev/shm:/dev/shm -v $OUTPUTS:/outputs -v `pwd`/src:/home/jovyan/work -it --rm -p 9095:8888 jupyter/all-spark-notebook:latest start.sh jupyter lab --NotebookApp.password='sha1:8ae766c31cc6:ad930abd81ceed1523b69e2a17f7cf8b806d3dc2'

