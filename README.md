# README MONET Blue Waters

## Creating docker 
Build dockers for using PCL (Point Cloud Library) and Spark using:    
`
./create_docker.sh
`

## Launch Monet Batch Processing 
Runs raw OVIS files through the data processing pipeling and produces congestion regions.  
Execute the following command:        
        `
        ./launch_batch_processing.sh [host-data-directory] [host-output-directory] [date]
        `    
        
Arguments:    
* *host-data-directory* : directory containing OVIS files in YYYYMMDD format
* *host-output-directory* :  directory to output region files
* *date* : date that needs to be proccessed (YYYYMMDD format)

## Output Format
`*host-output-directory*\region` contains output of the region growth segmentation and region duration algorithms.

There are two CSVs in the output dir:
* `YYYYMMDD` : Contains information about the congestion region size, duration and average stall values.
* `YYYYMMDD_edges` : Contains information about transition between regions.


