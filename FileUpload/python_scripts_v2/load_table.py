  
from env_global_variables import *
from gcp_bq_utils import load_table_from_file
from gcp_bq_utils import load_table_from_query
from gcp_bq_utils import delete_table
from gcp_bq_utils import create_table
from gcp_bq_utils import create_view
from gcp_bq_utils import get_table_schema
from gcp_bq_utils import check_job_status
from gcp_bq_utils import check_job_error
from gcp_bq_utils import archive_bucket_object
from gcp_bq_utils import create_bq_service
from gcp_gcs_utils import create_gcs_service
#from gcp_log_utils import get_logger

from apiclient.discovery import build 
from oauth2client.client import flow_from_clientsecrets 
from oauth2client.file import Storage 
from oauth2client import tools 
from apiclient.errors import HttpError 
from oauth2client.client import GoogleCredentials 

import json
import os 
import sys 
import httplib2 
import argparse
import datetime
import time
import inspect



def get_attribute_list(fileSchema, prefix, delimeter):

	attributeList = ''
	lastAttribute = fileSchema[-1]['name']
	
	for attribute in fileSchema:
		if prefix == '':
			attributeList = attributeList + attribute['name']
		else:
			attributeList = attributeList + prefix + attribute['name'] + ' ' + attribute['name']
		if lastAttribute <> attribute['name']:
			attributeList = attributeList + delimeter
	
	return attributeList


#python load_table.py tccc-dev-volume-south-latin/ARCHIVE/Andina  2016071215145_Andina_20160333_0453_chn.csv chn.json chn_key.json "," tccc-dev-volume-south-latin source t_chn WA CIN 0
		
def main():

	# tccc-dev-volume-south-latin/ARCHIVE/Andina  2016071215145_Andina_20160333_0453_chn.csv chn.json chn_key.json "," tccc-dev-volume-south-latin source t_chn WA CIN 0

	src_bucket = sys.argv[1]
	src_file = sys.argv[2]
	srcSchemaFile = sys.argv[3] 
	srcKeyFile = sys.argv[4] 
	src_file_delimiter = sys.argv[5] 
	tgt_project_id = sys.argv[6]
	tgt_dataset_id = sys.argv[7]
	tgt_table_id =sys.argv[8]
	writeDisposition = sys.argv[9]
	createDisposition = sys.argv[10]
	skipLeadingRows = sys.argv[11]
		
	scopes = [glb_bq_scope]
	bq_service = create_bq_service(glb_client_secret, scopes)	
	
	scopes = [glb_gcs_rw_scope]
	gs_service = create_gcs_service(glb_client_secret, scopes)	
	
	job_list = []
	
	src_file = "gs://" + src_bucket + "/" + src_file
	
	with open(srcSchemaFile, 'r') as schema_file:
		src_schema = json.load(schema_file)

	with open(srcKeyFile, 'r') as key_file:
		src_key = json.load(key_file)		

	tstamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y%m%d%H%M%S') + str(datetime.datetime.now()).split('.')[1]
	stageTableID = tgt_table_id + '_' + tstamp
	deltaTableID = tgt_table_id + '_delta'
	indexTableID = tgt_table_id + '_index'
	
	#Load Temporary Stage Table
	
	load_resp = load_table_from_file(bq_service, src_schema, src_file, tgt_project_id, tgt_dataset_id, stageTableID, src_file_delimiter, writeDisposition, createDisposition, skipLeadingRows, 3)
	job_list.append(load_resp['jobReference'])
		
	job_running_flag = True
	while job_running_flag:
		job_running_flag = check_job_status(bq_service, job_list)
		if job_running_flag:
			time.sleep(glb_wait_secs)
		
	error = check_job_error(bq_service, job_list)

	#Load Delta Table
	
	selectList = get_attribute_list(src_schema, '', ', ')
	keyList_temp = get_attribute_list(src_key, '', ' + ')
	keyList_split = keyList_temp.split("+")
	keyList=""
	for key in keyList_split:
		keyList=keyList+"STRING("+key+")+"
	keyList=keyList[:-1]	
	sqlStmt = 'select ' + selectList + ', sha1(' + keyList + ') hash_key, current_timestamp() load_dttm from ' + tgt_dataset_id + '.' + stageTableID
	
	load_resp = load_table_from_query(bq_service, sqlStmt, '', tgt_project_id, tgt_project_id, tgt_dataset_id, deltaTableID, 'WA', 5)
	job_list.append(load_resp['jobReference'])

	job_running_flag = True
	while job_running_flag:
		job_running_flag = check_job_status(bq_service, job_list)
		if job_running_flag:
			time.sleep(glb_wait_secs)
	
	error = check_job_error(bq_service, job_list)
	
	#Load Index Table

	sqlStmt = 'select hash_key, max(load_dttm) load_dttm from ' + tgt_dataset_id + '.' + deltaTableID + ' group by hash_key' 	
	
	load_resp = load_table_from_query(bq_service, sqlStmt, '', tgt_project_id, tgt_project_id, tgt_dataset_id, indexTableID, 'WT', 5)
	job_list.append(load_resp['jobReference'])

	job_running_flag = True
	while job_running_flag:
		job_running_flag = check_job_status(bq_service, job_list)
		if job_running_flag:
			time.sleep(glb_wait_secs)
	
	error = check_job_error(bq_service, job_list)
	
	tblSchema = get_table_schema(bq_service, tgt_project_id, tgt_dataset_id, deltaTableID)
	resp = create_table(bq_service, tgt_project_id, tgt_dataset_id, tgt_table_id, tblSchema, expiration_time=None)
	
	if error:
		raise RuntimeError('\n'.join(e['message'] for e in error))
		print(error)
		return -1
	
	selectList = get_attribute_list(src_schema, '#table#.', ', ')
	
	sqlStmt = 'select * from'
	sqlStmt = sqlStmt + ' (select ' + selectList.replace('#table#.', 'base.') + ', base.hash_key hash_key, base.load_dttm load_dttm from ' + tgt_dataset_id + '.' + tgt_table_id + ' base'
	sqlStmt = sqlStmt + ' where hash_key not in (select hash_key from ' + tgt_dataset_id + '.' + indexTableID + ')),'
	sqlStmt = sqlStmt + ' (select ' + selectList.replace('#table#.', 'delta.') + ', delta.hash_key hash_key, delta.load_dttm load_dttm from ' + tgt_dataset_id + '.' + deltaTableID + ' delta'
	sqlStmt = sqlStmt + ' join ' + tgt_dataset_id + '.' + indexTableID + ' index on delta.hash_key = index.hash_key and delta.load_dttm = index.load_dttm)'
	
	viewName = tgt_table_id + '_view'
	resp = create_view(bq_service, tgt_project_id, tgt_dataset_id, viewName, sqlStmt)
	
	# Delete Stage Table
	resp = delete_table(bq_service, tgt_project_id, tgt_dataset_id, stageTableID)

	# logger.info(glb_END + params)		
		
if __name__ == '__main__': 
	main() 
