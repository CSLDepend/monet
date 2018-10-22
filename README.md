# README MONET Blue Waters

## Creating docker 
execute
`
./create_docker.sh
`

## Launch Monet Batch Processing 
runs clustering algorithm on raw dataset and produces segments in /outputs (docker directory)
execute
        `
        ./launch_batch_processing.sh <host-data-directory> <host-output-directory> <date>
        `
Assumptions, arguments
* <host-data-directory> : directory containing OVIS files in YYMMDD format (no extensions)
* <host-output-directory>: directory where user wants to dump output files
* <date> : date that needs to be proccessed (YYMMDD format)


## Lauch Vizualization Webpage
runs visulization engine on a webpage using jupyter-notebook based docker
execute 
    `
    ./launch_viz.sh <host-data-directory> <host-output-directory>
    `
Assumptions, arguments
* <host-data-directory> : directory containing workload information in 'YYMMDD_app.csv' format 
* <host-output-directory>: output directory specified during the above launch

**NOTE** 
'YYMMDD_app.csv must be dumped from isc database, e.g.,
`
mysql -h isc -u internalro isc -e "select * from jobs where start >= 1537765000 and start < 1537765000 + 24*3600" >> ~/20180924_app.csv 
`


