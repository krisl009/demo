  
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



#python reduce_table.py tccc-dev-volume-south-latin source t_chn
		
def main():

	projectID = sys.argv[1]
	datasetID = sys.argv[2]
	tableID = sys.argv[3]

		
	scopes = [glb_bq_scope]
	bq_service = create_bq_service(glb_client_secret, scopes)	
	
	scopes = [glb_gcs_rw_scope]
	gs_service = create_gcs_service(glb_client_secret, scopes)	
	
	job_list = []

	deltaTableID = tableID + '_delta'
	indexTableID = tableID + '_index'
	viewName = tableID + '_view'
	
	#Load Base Table

	sqlStmt = 'select * from '+ datasetID + '.' + viewName
	
	load_resp = load_table_from_query(bq_service, sqlStmt, '', projectID, projectID, datasetID, tableID, 'WT', 5)
	job_list.append(load_resp['jobReference'])

	job_running_flag = True
	while job_running_flag:
		job_running_flag = check_job_status(bq_service, job_list)
		if job_running_flag:
			time.sleep(glb_wait_secs)
	
	error = check_job_error(bq_service, job_list)
	
	#Truncate Index Table

	sqlStmt = 'select * from '+ datasetID + '.' + indexTableID + ' where 1 = 0'
	
	load_resp = load_table_from_query(bq_service, sqlStmt, '', projectID, projectID, datasetID, indexTableID, 'WT', 5)
	job_list.append(load_resp['jobReference'])

	job_running_flag = True
	while job_running_flag:
		job_running_flag = check_job_status(bq_service, job_list)
		if job_running_flag:
			time.sleep(glb_wait_secs)
	
	error = check_job_error(bq_service, job_list)

	#Truncate Delta Table

	sqlStmt = 'select * from '+ datasetID + '.' + deltaTableID + ' where 1 = 0'
	
	load_resp = load_table_from_query(bq_service, sqlStmt, '', projectID, projectID, datasetID, deltaTableID, 'WT', 5)
	job_list.append(load_resp['jobReference'])

	job_running_flag = True
	while job_running_flag:
		job_running_flag = check_job_status(bq_service, job_list)
		if job_running_flag:
			time.sleep(glb_wait_secs)
	
	error = check_job_error(bq_service, job_list)	

	# logger.info(glb_END + params)		
		
if __name__ == '__main__': 
	main() 