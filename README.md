# MONET Blue Waters

## Tool Description
Monet is a congestion characterization toolkit for toroidal networks. We have *only* released the following:
        * Network hotspot extraction and characterization tool, which extracts CRs at runtime; it does so by using an unsupervised region-growth clustering algorithm: The clustering method requires specification of congestion metrics (e.g., percent time stalled (PTS) or stall-to-flit ratios) and a network topology graph to extract regions of congestion that can be used for runtime or long-term network congestion characterization. 
        * Congestion vizualization toolkit: This allows to visualize the congestion clouds in the network along with applications. Please launch the jupyter notebook located [here](). See below references.  

Monet was used to characterize the Blue Waters congestion dataset obtained using [LDMS](https://github.com/ovis-hpc/ovis). The results and methodology are explained in detail in our NSDI 2020 and HOTI paper.

## Citation
 * [S. Jha, A. Patke, B. Lim, J. Brandt, A. Gentile, G. Bauer, M. Showerman, L. Kaplan, Z. Kalbarczyk, W. T. Kramer, R. Iyer (2020). Measuring Congestion in High-Performance Datacenter Interconnects. 17th USENIX Symposium on Networked Systems Design and Implementation (NSDI).]()
 
 * [S. Jha, A. Patke, J. Brandt, A. Gentile, M. Showerman, E. Roman, Z. Kalbarczyk, W. T. Kramer, R. Iyer (2019). A Study of Network Congestion in Two Supercomputing High-Speed Interconnects. 2019 IEEE 26th Annual Symposium on High-Performance Interconnects (HOTI).](https://arxiv.org/abs/1907.05312)

## License 
Refer to license.md for license details.

## Steps for using the tool
### Create Docker 
Build dockers for PCL (Point Cloud Library) and Spark using:    
`
./create_docker.sh
`

### Build PCL Segmentation
Build executable for custom PCL region growth segmentation:   
`
./build_PCL.sh
`

### Launch Monet Batch Processing 
Runs raw OVIS files through the data processing pipeling and produces congestion regions.  
Execute the following command:        
        `
        ./launch_batch_processing.sh [host-data-directory] [host-output-directory] [date] [timestamp]
        `    
        
Arguments:    
* *host-data-directory* : directory containing OVIS files in YYYYMMDD format
* *host-output-directory* :  directory to output region files
* *date* : date that needs to be proccessed (YYYYMMDD format)
* *timestamp* : starting timestamp

### Output Format
`[host-output-directory]/region` contains output of the region growth segmentation and region duration algorithms.

There are two CSVs in the output dir:
* `YYYYMMDD` : Contains information about the congestion region size, duration and average stall values.
* `YYYYMMDD_edges` : Contains information about transitions between regions.

