#!/bin/bash
args=( $@ )
arglen=${#args[@]}
if [ $arglen -ne 3 ]
then
	echo "Ensure arguments to be: <data-dir> <output-dir> <date>"
	exit 1
fi

DATA=`realpath $1`
OUTPUTS=`realpath $2`
PYTHON_ARGS=${args[@]:2:$arglen}

docker run --rm -v $DATA:/data -v $OUTPUTS:/outputs -v `pwd`:/monet/ -ti  monet_spark:latest /monet/bin/run_spark.sh ${PYTHON_ARGS}
docker run --rm -v $DATA:/data -v $OUTPUTS:/outputs -v `pwd`:/monet/ -ti  monet_pcl:latest /monet/bin/run_pcl.sh ${PYTHON_ARGS}