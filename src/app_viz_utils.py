from copy import deepcopy
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
from ipywidgets import HBox,VBox
from ipywidgets import widgets
import math
def draw(fig,ax,ax2,tab,file_path_segment,bucket,bucket_to_apps):
    d = 24
    apps = bucket_to_apps[bucket]
    app_to_info = {}
    app_num = 0
    X,Y,Z,color = [],[],[],[]
    for app in apps:
        app_num +=1
        (jobid,jobname, node_count,shape_x,shape_y,shape_z,origin_x,origin_y,origin_z) = app
        app_to_info[app_num] = (jobid,jobname, node_count)
        for x in range(origin_x,origin_x+shape_x):
            for y in range(origin_y,origin_y+shape_y):
                for z in range(origin_z,origin_z+shape_z):
                    X.append((x+d)%d)
                    Y.append((y+d)%d)
                    Z.append((z+d)%d)
                    color.append(app_num)
    f = open(file_path_segment)
    target_bucket = bucket
    region = 0 
    points = []
    strengths = []
    point_to_region = {}
    region_to_total_stall = {}
    region_to_avg_stall = {}
    point_to_stall = {}
    region_to_app = {}
    for line in f:
        region +=1
        cols = line.split()
        bucket = int(cols[0])
        sz = int(cols[1])
        if(bucket<target_bucket):
            continue
        if(bucket>target_bucket):
            break
        typ = int(cols[2])
        mnx = 24
        mny = 24
        mnz = 24
        mxx = 0
        mxy = 0
        mxz = 0
        credits = []
        inqs = []
        pointst = []
        strengthst = []
        for j in range(3,len(cols),5):
            x = float(cols[j])
            y = float(cols[j+1])
            z = float(cols[j+2])
            if(x<0 or x>=24 or y<0 or y>=24 or z<0 or z>=24):
                continue
            credit = float(cols[j+3])
            inq = float(cols[j+4])
            if(inq==0):
                continue
            pointst.append((x,y,z,region))
            point_to_region[(int(x),int(y),int(z))] = region
            region_to_app[region] = set()
            point_to_stall[(x,y,z)] = credit+inq
            strengthst.append(credit+inq)
            mxx = max(x,mxx)
            mxy = max(y,mxy)
            mxz = max(z,mxz)
            mnx = min(x,mnx)
            mny = min(y,mny)
            mnz = min(z,mnz)
        if(len(strengthst)==0):
            continue
        region_to_total_stall[region] = sum(strengthst)
        avg_st = float(sum(strengthst))/len(strengthst)
        region_to_avg_stall[region] = avg_st
        if(avg_st>15):
            points += pointst
            strengths += strengthst

    for point in points:
        region_to_app[point[3]] = set()
        
    pointsc = set([(int(point[0]),int(point[1]),int(point[2])) for point in points])
    el_colors = set()
    
    apppts = set()
    for x,y,z,c in zip(X,Y,Z,color):
        apppts.add((x,y,z))
        if((x,y,z) in pointsc):
            el_colors.add(c)
            region_to_app[point_to_region[(x,y,z)]].add(c)
    Xcon = [point[0] for point in points]
    Ycon = [point[1] for point in points]
    Zcon = [point[2] for point in points]
    region_con = [point[3] for point in points]
    
    from ipywidgets import widgets
    from ipywidgets import Checkbox
    def draw2(fig,ax2,states2):
        ax2.clear()
        Xt,Yt,Zt,regiont = [],[],[],[]
        for x,y,z,r in zip(Xcon,Ycon,Zcon,region_con):
            if(r in states2):
                Xt.append(x)
                Yt.append(y)
                Zt.append(z)
                stall = point_to_stall[(x,y,z)]
                if(stall>45):
                    regiont.append('#42005c')
                elif(stall>30):
                    regiont.append('#af006a')
                else:
                    regiont.append('#ea3575')
        ax2.set_xlim([0,24])
        ax2.set_ylim([0,24])
        ax2.set_zlim([0,24])
        ax2.set_xlabel('X')
        ax2.set_ylabel('Y')
        ax2.set_zlabel('Z')
        ax2.set_title('Congestion Regions')
        ax2.scatter3D(Xt,Yt,Zt,c =regiont)
        fig.canvas.draw()
    def draw(fig,ax,states):
        app_num = 0
        Xf,Yf,Zf,colorf = [],[],[],[]
        for app in apps:
            app_num +=1
            if(app_num not in el_colors or app_num not in states):
                continue
            (jobid,jobname, node_count,shape_x,shape_y,shape_z,origin_x,origin_y,origin_z) = app
            app_to_info[app_num] = (jobid,jobname, node_count)
            for x in range(origin_x,origin_x+shape_x):
                for y in range(origin_y,origin_y+shape_y):
                    for z in range(origin_z,origin_z+shape_z):
                        Xf.append((x+d)%d)
                        Yf.append((y+d)%d)
                        Zf.append((z+d)%d)
                        colorf.append(app_num)
        ax.clear()
        ax.set_xlim([0,24])
        ax.set_ylim([0,24])
        ax.set_zlim([0,24])
        ax.set_xlabel('X')
        ax.set_ylabel('Y')
        ax.set_zlabel('Z')
        ax.set_title('Application Workflow')
        ax.scatter3D(Xf, Yf, Zf,c = colorf)
        fig.canvas.draw()
    states = set()
    states2 = set([0])
    def changed(b):
        col = int(b.owner.description.split(':')[0])
        if(col in states):
            states.remove(col)
        else:
            states.add(col)
        draw(fig,ax,states)
    
    def changed2(b):
        region = int(b.owner.description)
        if(region in states2):
            states2.remove(region)
        else:
            states2.add(region)
        draw2(fig,ax2,states2)
        
    region_to_avg_stall[0] = 0
    region_to_total_stall[0] = 0
    region_to_app[0] = []
    
    region_stall_temp = []
    region_stall_temp.append((0,0))
    for region in set(region_con):
        region_stall_temp.append((region_to_total_stall[region],region))
    
    region_stall_temp.sort(reverse=True)
    
    VBoxes = []
    regions = []
    for (stall,region) in region_stall_temp:
        regions.append(region)
        boxes = []
        for col in region_to_app[region]:
            box = Checkbox(False,description = str(col)+':'+str(app_to_info[col]))
            box.observe(changed)
            boxes.append(box)
        VBoxes.append(VBox(boxes))
        
    def changed_tab(b):
        try:
            if(type(b['old'])!=int):
                if('selected_index' in b['old']):
                    ind = b['old']['selected_index']
                    states2 = set([regions[ind]])
                    draw2(fig,ax2,states2)
        except:
            pass
    children = VBoxes
    tab.children = children
    for i in range(len(children)):
        stl = int(region_to_avg_stall[regions[i]])
        tab.set_title(i, str(regions[i])+str(' (')+str(int(stl/2.55))+str(')'))
    tab.observe(changed_tab)

def get_important_regions(file_path):
    f  = open(file_path)
    stall_region = []
    region = 0
    region_to_total_stall = {}
    region_to_avg_stall = {}
    prev_bucket = 0
    for line in f:
            region +=1
            cols = line.split()
            bucket = int(cols[0])
            if(bucket>prev_bucket):
                if(bucket%100==0):
                    print('Done with processing ',bucket)
                prev_bucket = bucket

            sz = int(cols[1])
            typ = int(cols[2])
            mnx = 24
            mny = 24
            mnz = 24
            mxx = 0
            mxy = 0
            mxz = 0
            credits = []
            inqs = []
            pointst = []
            strengthst = []
            for j in range(3,len(cols),5):
                x = float(cols[j])
                y = float(cols[j+1])
                z = float(cols[j+2])
                if(x<0 or x>=24 or y<0 or y>=24 or z<0 or z>=24):
                    continue
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
            region_to_total_stall[region] = sum(strengthst)
            avg_st = float(sum(strengthst))/len(strengthst)
            region_to_avg_stall[region] = avg_st
            if(avg_st>15):
                stall_region.append((region_to_total_stall[region],(bucket,region)))

    stall_region.sort(reverse=True)
    for i in range(0,10):
        print('Region Description : Region Number %d Bucket Number %d Total inq stall %d' %(stall_region[i][1][1],stall_region[i][1][0],stall_region[i][0]))


def get_bucket_to_apps(appDF,timestamp):
    bucket_to_apps = {}
    cnt = 0
    bad_lines = 0
    for index,row in appDF.iterrows():
        try:
            u_start = float(row['start'])
            u_end = float(row['end'])
            if(math.isnan(u_start) or math.isnan(u_end)):
                continue
            cnt +=1
            if(cnt%10000==0):
                print('Done with computing %d rows' %(cnt))
            bucket_start = math.ceil(float((u_start-timestamp+30))/60)
            bucket_end = math.ceil(float((u_end-timestamp+30))/60)
            jobid,jobname, node_count,shape_x,shape_y,shape_z,origin_x,origin_y,origin_z = int(row['jobid']),row['jobname'],int(row['node_count']),int(row['shape_x']),int(row['shape_y']),int(row['shape_z']),int(row['origin_x']),int(row['origin_y']),int(row['origin_z'])
            app = (jobid,jobname, node_count,shape_x,shape_y,shape_z,origin_x,origin_y,origin_z)
            for bucket in range(max(1,bucket_start),min(1440,bucket_end)+1):
                if bucket not in bucket_to_apps:
                    bucket_to_apps[bucket] = [app]
                else:
                    bucket_to_apps[bucket].append(app)
        except:
            bad_lines +=1
    print(bad_lines)
    return bucket_to_apps