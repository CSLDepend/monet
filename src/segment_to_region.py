#!/usr/bin/env pypy3

# Copyright (c) 2019 DEPEND Research Group at
# University of Illinois, Urbana Champaign (UIUC)
# This work is licensed under the terms of the UIUC/NCSA license.
# For a copy, see https://opensource.org/licenses/NCSA.


#Recommended to run with pypy3 for faster performance
from UF import *
import os
from multiprocessing import Pool
import argparse
import statistics
import sys
from copy import deepcopy
import csv

region_to_stall_finalg = {} #map from region number to stall values
region_to_time_finalg = {} #map from region number to region duration
region_to_size_finalg = {} #map from region number to region size
graph = [] #adjacency list representation of graph showing transition between regions
maxr  = 40000 #max possible regions in a single day
uf = UF(maxr) #union-find structure to merge 2 regions

#utility function to compute mean
def meanx(lis):
    n = len(lis)
    sum = 0
    for a in lis:
        sum += a
    return float(sum) / float(n)

#utility function to compute standard deviation
def stdx(lis):
    return statistics.stdev(lis)

#uniting regions due to folding in torus
def unite(bucket,regions,region_to_type,region_to_points,region_to_stall,point_type_to_stall):
    d = 24
    point_type_to_region = {}
    for region in regions:
        for point in region_to_points[region]:
            point_type_to_region[(point, region_to_type[region])] = region

    for region in regions:
        for point in region_to_points[region]:
            (x, y, z) = point
            nei = [(x + 1, y, z), (x, y + 1, z), (x, y, z + 1)]
            for (xt, yt, zt) in nei:
                if (xt >= d or yt >= d or zt >= d):
                    if (xt >= d):
                        xt -= d
                    if (yt >= d):
                        yt -= d
                    if (zt >= d):
                        zt -= d
                    if ((xt, yt, zt), region_to_type[region]) in point_type_to_region:
                        region_corr = point_type_to_region[((xt, yt, zt), region_to_type[region])]
                        if (region in region_to_stall and region_corr in region_to_stall):
                            if (abs(region_to_stall[region] - region_to_stall[region_corr]) < 4):
                                uf.union(region, region_corr)

    valid = set()
    for region in region_to_points:
        valid.add(uf.find(region))

    region_to_points_final = {}
    region_point_to_stall = {}
    region_to_stall_final = {}
    region_to_type_final = region_to_type

    for region in regions:
        if (uf.find(region) not in region_to_points_final):
            region_to_points_final[uf.find(region)] = set()

        for point in region_to_points[region]:
            (x, y, z) = point
            region_to_points_final[uf.find(region)].add((x, y, z))
            region_point_to_stall[(uf.find(region), (x, y, z))] = point_type_to_stall[
                (point, region_to_type[region])]

    for region in region_to_points_final:
        credit_stalls = []
        inq_stalls = []
        for point in region_to_points_final[region]:
            credit_stalls.append(region_point_to_stall[(region, point)][0])
            inq_stalls.append(region_point_to_stall[(region, point)][1])

        avg_credit = meanx(credit_stalls) / 2.55
        avg_inq = meanx(inq_stalls) / 2.55
        region_to_stall_final[region] = [avg_credit, avg_inq]

    region_to_stall_finalg.update(region_to_stall_final)
    for region in region_to_points_final:
        region_to_size_finalg[region] = len(region_to_points_final[region])

    return [region_to_type_final, region_to_points_final, region_to_stall_final]

#finding transition between regions across consecutive buckets
def intersect_region(bucket_to_maps1, bucket_to_maps2):
    region_to_type1 = bucket_to_maps1[0]
    region_to_type2 = bucket_to_maps2[0]
    region_to_points1 = bucket_to_maps1[1]
    region_to_points2 = bucket_to_maps2[1]
    region_to_stall1 = bucket_to_maps1[2]
    region_to_stall2 = bucket_to_maps2[2]

    point_type_to_region2 = {}
    for region in region_to_points2:
        for point in region_to_points2[region]:
            point_type_to_region2[(point, region_to_type2[region])] = region

    comm_points = {}
    for region in region_to_points1:
        for point in region_to_points1[region]:
            if (point, region_to_type1[region]) in point_type_to_region2:
                region_corr = point_type_to_region2[(point, region_to_type1[region])]
                if (region_to_type1[region] == region_to_type2[region_corr]):
                    typ = region_to_type1[region]
                    if (region, region_corr) in comm_points:
                        comm_points[(region, region_corr)] += 1
                    else:
                        comm_points[(region, region_corr)] = 1
    covered = set()
    for (region, region_o) in comm_points:
        if region in covered:
            continue
        iscore = float(comm_points[(region, region_o)]) / min(len(region_to_points1[region]),
                                                              len(region_to_points2[region_o]))
        if (iscore > 0.5):
            graph.append((region,region_o))
            covered.add(region)

#recursive function to calculate duration for each region
def dp(g,u):
    if(len(g[u])==0):
        region_to_time_finalg[u] = 1
        return region_to_time_finalg[u]

    if u in region_to_time_finalg:
        return region_to_time_finalg[u]

    ret = 0
    for v in g[u]:
        ret = max(ret,dp(g,v)+1)

    region_to_time_finalg[u] = ret
    return ret

#creating and initializing transition graph
def find_times():
    vertices = set()
    for (u,v) in graph:
        vertices.add(u)
        vertices.add(v)
    maxv = max(vertices)
    g = [[] for i in range(0,maxv+1)]

    for (u,v) in graph:
        g[u].append(v)
        
    for u in vertices:
        dp(g,u)

#function to compute congestion regions from segments formed by PCL segmentation
def create_region_from_segment(args):
    file_path = args[0]
    f = open(file_path)
    bucket_to_maps = [[{},{},{}],[{},{},{}]]
    regionsg = []  # 0
    region_to_typeg = {}  # 1
    region_to_pointsg = {}  # 2
    region_to_stallg = {}  # 3
    point_type_to_stallg = {}  # 4
    prev_bucket = 0
    region = 0
    for line in f:
        region += 1
        cols = line.split()
        try:
            bucketg = int(cols[0])
            if (bucketg > prev_bucket):
                if (bucketg % 20 == 0):
                    print('Reading bucket ', bucketg)
                bucket_to_maps[0] = deepcopy(bucket_to_maps[1])
                bucket_to_maps[1] = deepcopy(unite(bucketg,regionsg,region_to_typeg,region_to_pointsg,region_to_stallg,point_type_to_stallg))
                intersect_region(bucket_to_maps[0],bucket_to_maps[1])
                regionsg.clear()
                region_to_typeg.clear()
                region_to_pointsg.clear()
                region_to_stallg.clear()
                point_type_to_stallg.clear()
                prev_bucket = bucketg

            sz = int(cols[1])
            typ = int(cols[2])
            regionsg.append(region)
            region_to_typeg[region] = typ
            region_to_pointsg[region] = []
            avg_credit = 0
            avg_inq = 0
            for j in range(3, len(cols), 5):
                x = float(cols[j])
                y = float(cols[j + 1])
                z = float(cols[j + 2])
                credit = float(cols[j + 3])
                inq = float(cols[j + 4])
                avg_credit += credit
                avg_inq += inq
                region_to_pointsg[region].append((x, y, z))
                point_type_to_stallg[((x, y, z), typ)] = (credit, inq)
            avg_credit = float(avg_credit) / ((len(cols) - 3) / 5)
            avg_inq = float(avg_inq) / ((len(cols) - 3) / 5)
            if (avg_inq == 0):
                region_to_stallg[region] = avg_credit
            else:
                region_to_stallg[region] = avg_inq
        except:
             print('Error on line %d in file %s' % (region, file_path))


    find_times()
    csvData = [['Region','Size', 'Avg_Credit', 'Avg_Inq', 'Time']]

    for region in region_to_time_finalg:
        csvData.append([region,region_to_size_finalg[region], region_to_stall_finalg[region][0], region_to_stall_finalg[region][1],
                        region_to_time_finalg[region]])

    with open(args[1], 'w') as csvFile:
        writer = csv.writer(csvFile)
        writer.writerows(csvData)
    
    csvData = [['Region1','Region2']]
    with open(args[1]+'_edges','w') as csvFile:
        writer = csv.writer(csvFile)
        writer.writerows(csvData+([[region,region_o] for (region,region_o) in graph]))
    print('Done with file %s' % (file_path))

def main():
    sys.setrecursionlimit(100000) #setting higher limit for longer duration regions
    create_region_from_segment([sys.argv[1],sys.argv[2]])


if __name__ == "__main__":
    main()
