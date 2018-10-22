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

docker run --rm -v $DATA:/data -v $OUTPUTS:/outputs -v `pwd`:/monet/ -ti  monet:latest /monet/bin/run.sh ${PYTHON_ARGS}
