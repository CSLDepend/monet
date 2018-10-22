import os
import sys
from unix_timestamp import *
import argparse
#argp = True
rawDir = "/data/"
parquetDir = '/outputs/processed/'
pcdDir = '/outputs/pcd/'
segmentDir = '/outputs/segment/'
descriptorFile = '/monet/src/HEADER.20160115-'
#date = '20181001'
#timestamp = '1538370000'
curDir = '/monet/src/'

#if(argp):
#    rawDir = sys.argv[1]
#    outputDir = sys.argv[2]
#    parquetDir  = outputDir+'processed/'
#    pcdDir = outputDir+'pcd/'
#    segmentDir  = outputDir+'segment/'
#    date = sys.argv[3]
#    timestamp = str(get_unix_time(date))

def main():
#    parser = argparse.ArgumentParser(description='Monet production code for bluewaters')
#    parser.add_argument("--date", help="assumes filename in the form of YYMMDD with no extension in /data/", required=True)
#    args = parser.parse_args()
    date = sys.argv[1]
    timestamp = str(get_unix_time(date))
    os.system('mkdir -p '+parquetDir)
    os.system('mkdir -p '+pcdDir)
    os.system('mkdir -p '+segmentDir)
    os.system('python3 /monet/src/ovis_data_converter.py %s %s %s %s'%(rawDir,parquetDir,descriptorFile,date))
    os.system('mkdir '+pcdDir+date)
    os.system('python3 /monet/src/bw_workflow.py %s %s %s %s'%(date,timestamp,parquetDir[:-1],pcdDir[:-1]))
    os.system('python3 /monet/src/rgrunner_unique.py single 20 %s 2 4 4 2 20 %s %s'%(pcdDir+date,segmentDir[:-1],curDir))

if __name__ == "__main__":
    main()
#python3 ovis_data_converter.py /data/ /outputs/processed/ /monet/HEADER.20160115- 20181001
#python3 bw_workflow.py 20181001 1538370000 /outputs/processed /outputs/pcd
#python3 rgrunner_unique.py single 20 /outputs/pcd/20181001 2 4 4 2 20 /outputs/segment /monet/
