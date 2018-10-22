import pandas as pd
import numpy as np
from datetime import datetime

from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType
from pyspark.sql.types import StringType
from pyspark.sql.types import LongType  
from pyspark.sql.types import Row
import os

from intervaltree import Interval, IntervalTree

def populate_app_dict(appTSV):
    app_range_trees = IntervalTree()
    header = True
    with open(appTSV, "r") as ifh:
        for line in ifh:
            if header:
                header = False
                continue
            cols = line.split("\t")
            app_st = int((int(cols[9])/60.0)*60)
            app_end = int((int(cols[10])/60.0)*60)
            nodes = cols[16].strip("[]").split(",")
            app_range_trees[app_st:app_end] = [cols[2], cols[3], nodes]
    return app_range_trees


def get_exitcode_df(appExitCodeTSV):
    exitCodeDF = pd.read_csv(appExitCodeTSV, sep="\t")
    exitCodeDF["exit_code"] = exitCodeDF["exit_code"].apply( lambda x: int(x))
    return exitCodeDF


def hms_to_seconds(t):
    h, m, s = [int(i) for i in t.split(':')]
    return 3600*h + 60*m + s

def label_exit(row):
    if row["category"] != "Success":
        return 1
    else:
        return 0
    
def get_linux_style_exit_code(exitcode):
    if exitcode < 128:
        return exitcode
    else:
        return (exitcode-128) % 128
    

def get_app_df(appParsedTSV, appExitCodeTSV, st, et):
    appDF = pd.read_csv(appParsedTSV, sep="\t")
    countsBefore = len(appDF)
    print("total entries %d"% countsBefore)
    appDF = appDF[(appDF.u_start >= st) & (appDF.u_start <= et)]
    
    # remove these bad entries
    appDF = appDF[np.isnan(appDF["exit_code"]) == False]
    # remove debug queue
    appDF = appDF[appDF.queue !="debug"]
    countsDebug = len(appDF)
    print("Entries in debug %d" % (countsBefore-countsDebug))
    appDF["u_end"] = appDF["u_end"].apply(lambda x: np.long(x))
    appDF["duration"] = appDF["u_end"] - appDF["u_start"]
    appDF["node_seconds"] = appDF["num_nodes"] * appDF["duration"]
    appDF["exit_code"] = appDF["exit_code"].apply(lambda x : get_linux_style_exit_code(int(x)))
    exitCodeDF = get_exitcode_df(appExitCodeTSV)
    appDF = pd.merge(appDF, exitCodeDF, on = "exit_code")
    # appDF = appDF[(appDF["category"]=="system") | (appDF["category"]=="user/system") | (appDF["category"]=="Success") ]
    appDF["is_failed"] = appDF.apply(lambda row: label_exit(row), axis=1)
    appDF = appDF[appDF.duration > 0.0]
    countsAfter = len(appDF)
    print("percentage bad entries %f" % (1.0 - (countsBefore-countsAfter)/countsBefore))
    return appDF
    # appDF["is_failed"] = appDF["exit_code"].apply(lambda x: 1 if x !="0"  else 0 )
    #appDF[["apid", "queue", "num_nodes", "u_start", "u_end", "RUwalltime", "moab_run_time", "RLwalltime", "duration"]].head(n=200)
    

def get_appscale_failure_ratio_df(localDF):
    totalJobs = len(localDF)
    localDF["bin_id"] = localDF["num_nodes"].apply(lambda row : (np.log2(row).astype("int")))
    localDF = localDF[localDF.bin_id >= 0 ]
    groupedValues = localDF.groupby("bin_id").agg(len).reset_index()
    jobSzCount = groupedValues["apid"]
    jobSzCount["Ratio"] = jobSzCount.apply(lambda row: row/float(totalJobs))
    jobSzCount["numNodes"] = groupedValues["bin_id"]
    return jobSzCount


def get_nodestate_df(nodeStateTSV, st, et):
    nodeState = pd.read_csv(nodeStateTSV, sep="\t")
    nodeState = nodeState[(nodeState.change_time >= st) & (nodeState.change_time <= et)]
    print(nodeState.status_from.unique())
    # nodeState = nodeState[(nodeState.status_to!="admindown") & (nodeState.status_from!="admindown")]
    nodeState = nodeState[~((nodeState.status_to=="up") & (nodeState.status_from=="up"))]
    nodeState = nodeState[~((nodeState.status_to=="down") & (nodeState.status_from=="down"))]
    nodeState = nodeState.sort(["change_time"], ascending=[True])
    nodeState = nodeState[["nid", "change_time", "status_from", "status_to"]]
    nodeDown = nodeState.groupby(["nid"]).agg(lambda x: tuple(x))
    return nodeDown
    
def get_node_availability(nodeState):
    nodeDown = nodeState
    tbe = {"down":[], "admindown":[], "suspect":[], "xxx":[]}
    nidDown = []
    def calc(nid, times, nFrom, nTo):
        hours = {"down":0, "admindown":0, "suspect":0, "xxx":0}
        state = list(zip(times, nFrom, nTo))
        for idx in range(1, len(times)):
            if nTo[idx-1] == "suspect":
                continue
            elif nTo[idx-1] == "up":
                continue
            elif nTo[idx-1] == "xxx":
                hours["down"] += (times[idx] - times[idx-1])/3600.0
            else:    
                hours[nTo[idx-1]] += (times[idx] - times[idx-1])/3600.0
            tbe[nTo[idx-1]].append([nid, str(datetime.utcfromtimestamp(times[idx-1]).strftime('%Y-%m-%d')), (times[idx] - times[idx-1])/3600.0])
        return hours

    for idx in range(len(nodeDown)):
        change_time = nodeDown.iloc[idx].change_time
        status_from = nodeDown.iloc[idx].status_from
        status_to = nodeDown.iloc[idx].status_to
        nidHours = calc(idx, change_time, status_from, status_to)
        nidHours["nid"] = nodeDown.iloc[idx].name
        nidDown.append(nidHours)

    df = pd.DataFrame(nidDown)
    df = df.set_index("nid")
    return df, tbe



def get_spark_context():
    import pyspark
    from pyspark.sql import SparkSession
    conf = pyspark.SparkConf()
    conf.setMaster("local[60]")#"mesos://10.0.1.201:5050"
    # conf.set("spark.cores.max", "44")
    conf.set("spark.driver.memory", "100g")
    conf.set("spark.executor.memory", "600g")
    conf.set("spark.core.connection.ack.wait.timeout", "1200")
    conf.set("spark.rpc.netty.dispatcher.numThreads","4")
    conf.set("spark.driver.maxResultSize", "200g")
    conf.set("spark.python.worker.memory", "5g")
    conf.setAppName('rurApp')
    # create the context
    sc = pyspark.SparkContext(conf=conf)
    spark = SparkSession.builder.getOrCreate()
    return spark


def load_apps(spark, appDF):
    appDF = appDF[[
        "batch_id", 
        "apid", 
        "num_nodes",
        "u_start", 
        "u_end", 
        "exit_code", 
        "exit_code_array", 
        "exit_signal_array", 
        "cmd_line", "nodeType", "is_failed", "nodes", "duration"
    ]]
    
    appDF = spark.createDataFrame(appDF)
    nodecreator = F.udf(lambda col : col.strip("[]").split(","), ArrayType(StringType(), True))
    nodecreator = F.udf(lambda col : col.strip("[]").split(","), ArrayType(StringType(), True))
    appDF = appDF.withColumn("nodeList", nodecreator("nodes"))
    return appDF


def merge_with_apps(row, app_range_trees):
    t_epoch = row.Time
    nid = str(row.CompId)
    app_intervals = app_range_trees.value[t_epoch]
    #yield app_intervals
    d = row.asDict()
    for app in app_intervals:
        if nid in app[2][2]:
            d['apid'] = app[2][0]
            d["jobid"] = app[2][1]
            new_row = Row(**d) 
            yield (new_row)
            break

def map_apps_to_nodes(spark, app_jobs_tsv, rcOvis):
    app_range_trees = populate_app_dict(app_jobs_tsv)
    app_range_trees_ = spark.sparkContext.broadcast(app_range_trees)
    rcRDD = rcOvis.rdd.flatMap(lambda row: merge_with_apps(row, app_range_trees_))
    return spark.createDataFrame(rcRDD)



