/* Copyright (c) 2019 DEPEND Research Group at
* University of Illinois, Urbana Champaign (UIUC)
* This work is licensed under the terms of the UIUC/NCSA license.
* For a copy, see https://opensource.org/licenses/NCSA.
*/

#include <iostream>
#include <string>
#include <sstream>
#include <vector>
#include <stdlib.h>
#include <math.h>
#include <pcl/point_types.h>
#include <pcl/search/pcl_search.h>
#include <pcl/search/flann_search.h>
#include <pcl/search/impl/flann_search.hpp>
#include <pcl/io/pcd_io.h>
#include <boost/thread/thread.hpp>
//#include <pcl/visualization/pcl_visualizer.h>
#include <pcl/filters/passthrough.h>
#include "region_growing_rgb_bw.h"
#include <pcl/console/parse.h>

#define XMIN    0
#define XMAX    23
#define YMIN    0
#define YMAX    23
#define ZMIN    0
#define ZMAX    23

/*
 * We will use only L1 distance, see: 
 * http://www.pcl-users.org/Exact-nearest-neighbor-not-working-td4035576.html
 * If wraparound, is no longer a valid distance metric
 * (triangle inequality fails)
 */

typedef pcl::PointXYZRGB PtTy;
typedef pcl::PointCloud<PtTy> CloudTy;
typedef CloudTy::Ptr CloudPtrTy;
typedef pcl::search::FlannSearch<PtTy, flann::L1<float> > KdTreeTy;
typedef KdTreeTy::Ptr KdTreePtrTy;

std::string make_region_string(CloudPtrTy cloud,
                               int bucket_number,
                               float point_color_threshold_value,
                               float region_color_threshold_value,
                               float distance_threshold_value,
                               int min_cluster_sz_value,int type)
{
    KdTreePtrTy kdtree(new KdTreeTy);
    kdtree->setInputCloud(cloud);
    pcl::RegionGrowingRGBBW<PtTy> reg;
    reg.setInputCloud(cloud);
    reg.setSearchMethod(kdtree);
    reg.setPointColorThreshold(point_color_threshold_value);
    reg.setRegionColorThreshold(region_color_threshold_value);
    reg.setDistanceThreshold(distance_threshold_value);
    reg.setMinClusterSize(min_cluster_sz_value);

    std::vector<pcl::PointIndices> clusters;
    reg.extract(clusters);

    std::ostringstream s;
    for (auto &cluster : clusters) {

        int sz = cluster.indices.size();
        s << bucket_number << " " << sz << " "<<type<<" ";
        for(auto &ind : cluster.indices)
        {
            pcl::PointXYZRGB &pt = (*cloud)[ind];
            s<<(float)pt.x<<" "<<(float)pt.y<<" "<<(float)pt.z<<" "<<(float)pt.r<<" "<<(float)pt.g<<" ";
        }
        s << std::endl;
    }
    return s.str();
}

int main(int argc, char **argv)
{
    if (argc < 3)
    {
        pcl::console::print_error ("Syntax is: %s <pcd-file> \n <type> \n <bucket number> \n"
                                    "-p <point threshold>\n-r <region threshold>\n"
                                    "-d <distance threshold>\n-s <cluster size>\n"
                                    , argv[0]);
        return (1);
    }
    CloudPtrTy cloud (new CloudTy);
    if (pcl::io::loadPCDFile<PtTy>(argv[1], *cloud) == -1)
    {
        //std::cout << "Cloud reading failed." << std::endl;
        return -1;
    }

    int type_number = atoi(argv[2]);

    int bucket_number = atoi(argv[3]);

    // parse switch
    //
    bool point_color_threshold = pcl::console::find_switch (argc, argv, "-p");
    float point_color_threshold_value = 1.0;
    if (point_color_threshold){
        pcl::console::parse(argc, argv, "-p", point_color_threshold_value);
        //std::cout << "Setting point color threshold to: " << point_color_threshold_value << std::endl;
    }
    bool region_color_threshold = pcl::console::find_switch (argc, argv, "-r");
    float region_color_threshold_value = 1.0;
    if (region_color_threshold){
        pcl::console::parse(argc, argv, "-r", region_color_threshold_value);
        //std::cout << "Setting region color threshold value to: " << region_color_threshold_value << std::endl;
    }
    bool distance_threshold = pcl::console::find_switch (argc, argv, "-d");
    float distance_threshold_value = 1.0;
    if (distance_threshold){
        pcl::console::parse(argc, argv, "-d", distance_threshold_value);
        //std::cout << "Setting distance theshold value to: " << distance_threshold_value << std::endl;
    }
    bool min_cluster_sz = pcl::console::find_switch (argc, argv, "-s");
    int min_cluster_sz_value = 1;
    if (min_cluster_sz){
        pcl::console::parse(argc, argv, "-s", min_cluster_sz_value);
        //std::cout << "Setting min cluster size value to: " << min_cluster_sz_value << std::endl;
    }

    /* Find regions */
    /* 0 - comb */
    /* 1 - max */
    /* 2 - sep */

    CloudPtrTy rcloud;

    if (type_number & 4) {
        type_number ^= 4;
        std::vector<int> pvec;
        for (int i = 0; i < cloud->points.size(); i++) {
            auto &pt = cloud->points[i];
            if (pt.r > 2.55 * 5.0 || pt.g > 2.55 * 5.0) {
                pvec.push_back(i);
            }
        }
        rcloud = CloudPtrTy(new CloudTy(*cloud, pvec));
    } else {
        rcloud = cloud;
    }


    if (type_number == 2) {
        CloudPtrTy ccloud(new CloudTy(*rcloud));
        for (auto &pt : ccloud->points)
            pt.g = 0;
        CloudPtrTy icloud(new CloudTy(*rcloud));
        for (auto &pt : icloud->points)
            pt.r = 0;
    
        std::cout << make_region_string(ccloud, bucket_number, point_color_threshold_value,
                                        region_color_threshold_value, distance_threshold_value,
                                        min_cluster_sz_value,0);
        std::cout << make_region_string(icloud, bucket_number, point_color_threshold_value,
                                        region_color_threshold_value, distance_threshold_value,
                                        min_cluster_sz_value,1);
    }
    return 0;
}
