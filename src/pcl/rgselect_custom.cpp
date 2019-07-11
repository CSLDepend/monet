#include <iostream>
#include <string>
#include <bits/stdc++.h>
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
#include <pcl/visualization/pcl_visualizer.h>
#include <pcl/filters/passthrough.h>
#include "region_growing_rgb_bw.h"
#include <pcl/console/parse.h>

#define XMIN    0
#define XMAX    23
#define YMIN    0
#define YMAX    23
#define ZMIN    0
#define ZMAX    23
#define pb push_back
#define ms(s, n) memset(s, n, sizeof(s))

/*
 * We will use only L1 distance, see: 
 * http://www.pcl-users.org/Exact-nearest-neighbor-not-working-td4035576.html
 * If wraparound, is no longer a valid distance metric
 * (triangle inequality fails)
 */

using namespace std;
typedef pcl::PointXYZRGB PtTy;
typedef pcl::PointCloud<PtTy> CloudTy;
typedef CloudTy::Ptr CloudPtrTy;
typedef pcl::search::FlannSearch<PtTy, flann::L1<float> > KdTreeTy;
typedef KdTreeTy::Ptr KdTreePtrTy;


const int d = 48;
float cong[d*d*d];
bool visited[d*d*d];
int region[d*d*d];

struct Point
{
    int x,y,z;
    Point(int x1,int y1,int z1)
    {
        x = x1;
        y = y1;
        z = z1;
    }
};

int ptoi(Point &pt)
{
    return pt.x*d*d + pt.y*d + pt.z;
}

Point itop(int ind)
{
    return Point(ind/(d*d),(ind/(d*d))%d,ind%d);
}

vector<Point> getNeighbours(Point pt)
{
    vector<Point> nei;
    nei.pb(Point(pt.x+1,pt.y,pt.z));
    nei.pb(Point(pt.x-1,pt.y,pt.z));
    nei.pb(Point(pt.x,pt.y+1,pt.z));
    nei.pb(Point(pt.x,pt.y-1,pt.z));
    nei.pb(Point(pt.x,pt.y,pt.z+1));
    nei.pb(Point(pt.x,pt.y,pt.z-1));
    for(auto &pt : nei)
    {
        pt.x = pt.x>=d?pt.x-d:pt.x;
        pt.y = pt.y>=d?pt.y-d:pt.y;
        pt.z = pt.z>=d?pt.z-d:pt.z;
        pt.x = pt.x<0?pt.x+d:pt.x;
        pt.y = pt.y<0?pt.y+d:pt.y;
        pt.z = pt.z<0?pt.z+d:pt.z;
    }
    return nei;
}
void createRegion(int ind,int region_number,float point_color_threshold_value)
{
    int ign_cnt = 0;
    int thresh = point_color_threshold_value;
    queue<int> q;
    q.push(ind);
    region[ind] = region_number;
    visited[ind] = region_number;
    while(q.size()>0)
    {
        int curr = q.front();
        q.pop();
        vector<int> eli;
        vector<Point> nei = getNeighbours(itop(curr));
        for(Point pt : nei)
        {
            eli.pb(ptoi(pt));
        }
        for(Point pt1 : getNeighbours(itop(curr)))
        {
            for(Point pt2 : getNeighbours(pt1))
            {
                eli.pb(ptoi(pt2));
            }
        }
        for(int next : eli)
        {
            if(!visited[next] && abs(cong[curr]-cong[next])<=2.55*point_color_threshold_value)
            {
                q.push(next);
                visited[next] = 1;
                ign_cnt++;
                region[next] = region_number;
            }
        }
    }
    cout<<ign_cnt<<" ** "<<endl;
}

void makeRegions(float point_color_threshold_value)
{
    int region_number = 0;
    for(int i = 0; i<d*d*d ; i++)
    {
        if(!visited[i] && cong[i]>0)
        {
            region_number++;
            createRegion(i,region_number,point_color_threshold_value);
        }
    }
    cout<<region_number<<endl;
    int cnt = 0;
    for(int i = 0; i<d*d*d ; i++)
    {
        if(visited[i])
        {
            cnt +=1;
        }
    }
    cout<<cnt<<endl;
}
void clear()
{
    ms(cong,0);
    ms(visited,0);
    ms(region,0);
}
std::string make_region_string(CloudPtrTy cloud,
                               int bucket_number,
                               float point_color_threshold_value,
                               float region_color_threshold_value,
                               float distance_threshold_value,
                               int min_cluster_sz_value)
{
    
    std::ostringstream s;
    clear();
    for(auto &pt : cloud->points)
    {
        int x = (int)pt.x*2;
        int y = (int)pt.y*2;
        int z = (int)pt.z*2;
        Point p(x,y,z);
        cong[ptoi(p)] = max(pt.r,pt.g);
    }
    makeRegions(point_color_threshold_value);
    s<<"A lot of points !!"<<endl;
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
    /* 2 - max */

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
        CloudPtrTy mcloud(new CloudTy(*rcloud));
        /*for (auto &pt : mcloud->points) {
            pt.r = pt.r >= pt.g ? pt.r : pt.g;
            pt.g = 0;
        }*/

        std::cout << make_region_string(mcloud, bucket_number, point_color_threshold_value,
                                        region_color_threshold_value, distance_threshold_value,
                                        min_cluster_sz_value);
    }
    return 0;
}