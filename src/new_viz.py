import numpy as np
import matplotlib.pyplot as plt
plt.switch_backend('agg')
import sys
def visualise_final(file_name,buckets):
    def get_visualisations(bucket,points,strengths):
        from mpl_toolkits.mplot3d import Axes3D
        X = [point[0] for point in points]
        Y = [point[1] for point in points]
        Z = [point[2] for point in points]
        colors = [point[3] for point in points]

        Xt,Yt,Zt,strengthst = [],[],[],[]
        for x,y,z,strength in zip(X,Y,Z,strengths):
            if(x>=0 and x<=23.5 and y>=0 and y<=23.5 and z>=0 and z<=23.5):
                Xt.append(x)
                Yt.append(y)
                Zt.append(z)
                strengthst.append(strength)
        X = Xt
        Y = Yt
        Z = Zt
        # matplotlib.use('agg')
        ax = plt.axes(projection='3d')
        ax.scatter3D(X, Y, Z,c=colors)
        # plt.plot(ax)
        plt.savefig('data/images/'+file_name+'/'+str(bucket))


    def fill_points(file_path):
        f = open(file_path)
        region = 0 
        points = []
        strengths = []
        prev_bucket = 0
        for line in f:
            region +=100
            cols = line.split()
            bucket = int(cols[0])
            sz = int(cols[1])

            if(bucket>prev_bucket and bucket%5==0):
                print('Processing bucket %d' %(bucket))

            if(bucket>prev_bucket and bucket-1 in buckets):
                print(len(points))
                get_visualisations(bucket-1,points,strengths)

            if(bucket>prev_bucket):
                prev_bucket = bucket
                points.clear()
                strengths.clear()

            typ = int(cols[2])
            mnx = 24
            mny = 24
            mnz = 24
            mxx = 0
            mxy = 0
            mxz = 0
            # print(sz)
            credits = []
            inqs = []
            pointst = []
            strengthst = []
            for j in range(3,len(cols),5):
                x = float(cols[j])
                y = float(cols[j+1])
                z = float(cols[j+2])
                credit = float(cols[j+3])
                inq = float(cols[j+4])
                if(inq==0):
                    continue
                pointst.append((x,y,z,region))
                strengthst.append(credit+inq)
                mxx = max(x,mxx)
                mxy = max(y,mxy)
                mxz = max(z,mxz)
                mnx = min(x,mnx)
                mny = min(y,mny)
                mnz = min(z,mnz)
            if(len(strengthst)==0):
                continue
            avg_st = float(sum(strengthst))/len(strengthst)
            # print(avg_st)
            if(avg_st>10):
                points += pointst
                strengths += strengthst

    fill_points('data/segment/regions_unique_2_4-4-2-20_'+file_name)

file_name = sys.argv[1]

buckets = set()
if(sys.argv[2]=='all'):
    buckets = set([i for i in range(1,1441)])
else:
    for i in range(3,len(sys.argv)):
        buckets.add(int(sys.argv[i]))

visualise_final(file_name,buckets)