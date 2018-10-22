import util

def parse_df(appDF,timestamp):
	appDF = appDF[['apid', 'jobid', 'u_start', 'u_end', 'cmd_line', 'num_nodes','shape_x','shape_y','shape_z','origin_x','origin_y','origin_z']]
	print(appDF.describe())
	appDF[((appDF.u_start>=timestamp-60) & (appDF.u_start<timestamp+86400+60)) | ((appDF.u_end>=timestamp-60) & (appDF.u_end<timestamp+86400+60))]
	print(len(appDF))
	

timestamp = 1494565200
appParsedTSV = "data/app/processedWorkload.tsv"
appExitCodeTSV = "data/app/exitCodesLessThan128.txt"
st = 1485302400
et = 1485302400 + 5 *30 * 24 * 3600
print('Started creating DF for appload')
appDF = util.get_app_df(appParsedTSV, appExitCodeTSV, st, et)
print('Finished creating DF for appload')
appDF = parse_df(appDF,timestamp)