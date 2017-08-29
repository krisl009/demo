#gcp_bq_utils.py


import json
import uuid
import datetime
import time

from gcp_gcs_utils import move_object
#from gcp_log_utils import get_logger
from env_global_variables import *

from apiclient.errors import HttpError 
from googleapiclient import discovery
from oauth2client.service_account import ServiceAccountCredentials

def create_bq_service(clientSecretFile, scopes):

    credentials = ServiceAccountCredentials.from_json_keyfile_name(clientSecretFile, scopes)

    # You can browse other available api services and versions here:
    #     http://g.co/dev/api-client-library/python/apis/
    return discovery.build('bigquery', 'v2', credentials=credentials)


def transform_row(row, schema):
        """Apply the given schema to the given BigQuery data row.

        Args:
            row: A single BigQuery row to transform.
            schema: The BigQuery table schema to apply to the row, specifically
                    the list of field dicts.

        Returns:
            Dict containing keys that match the schema and values that match
            the row.
        """

        log = {}

        # Match each schema column with its associated row value
        for index, col_dict in enumerate(schema):
            col_name = col_dict['name']
            row_value = row['f'][index]['v']

            if row_value is None:
                log[col_name] = None
                continue

            # Recurse on nested records
            if col_dict['type'] == 'RECORD':
                row_value = self.recurse_on_row(col_dict, row_value)

            # Otherwise just cast the value
            elif col_dict['type'] == 'INTEGER':
                row_value = int(row_value)

            elif col_dict['type'] == 'FLOAT':
                row_value = float(row_value)

            elif col_dict['type'] == 'BOOLEAN':
                row_value = row_value in ('True', 'true', 'TRUE')

            elif col_dict['type'] == 'TIMESTAMP':
                row_value = float(row_value)

            log[col_name] = row_value

        return log

def recurse_on_row(col_dict, nested_value):
        """Apply the schema specified by the given dict to the nested value by
        recursing on it.

        Args:
            col_dict: A dict containing the schema to apply to the nested
                      value.
            nested_value: A value nested in a BigQuery row.
        Returns:
            Dict or list of dicts from applied schema.
        """

        row_value = None

        # Multiple nested records
        if col_dict['mode'] == 'REPEATED' and isinstance(nested_value, list):
            row_value = [transform_row(record['v'], col_dict['fields'])
                         for record in nested_value]

        # A single nested record
        else:
            row_value = transform_row(nested_value, col_dict['fields'])

        return row_value


# [START check_job_error]
def check_job_error(bq_service, job_list):
	
	for item in job_list:
		status_request = bq_service.jobs().get(projectId = item['projectId'], jobId = item['jobId'])
		status = status_request.execute(num_retries=2)
		if status['status'].get('errors'):
			return status['status']['errors']
	
	return 0 
# [END check_job_error]


# [BEGIN delete_table]
def delete_table(gqService, projectId, datasetId, tableId):

	try:
		resp = gqService.tables().delete(projectId=projectId, datasetId=datasetId, tableId=tableId).execute()
	except HttpError as err: 
		print('Error: {}'.format(err.content)) 
		raise err
	return 0
# [END delete_table]

# [START check_job_error]
def check_job_status(bq_service, job_list):

	job_running_flag = False
	
	for item in job_list:
		status_request = bq_service.jobs().get(projectId = item['projectId'], jobId = item['jobId'])
		status = status_request.execute(num_retries=2)
		if status['status']['state'] != "DONE":
			job_running_flag = True
			return job_running_flag 
	
	return job_running_flag 
# [END check_job_status]

# [START load_table_from_query]
def load_table_from_query(service, query, cycle, srcProjectId, tgtProjectId, datasetId, tableId, write_disposition="WT", num_retries=5):
    # Generate a unique job_id so retries
    # don't accidentally duplicate query
		
	tstamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y%m%d%H%M%S') + str(datetime.datetime.now()).split('.')[1]
	
	if write_disposition == "WT":
		write_disposition = "WRITE_TRUNCATE"
	
	if write_disposition == "WA":
		write_disposition = "WRITE_APPEND"
	
	if write_disposition == "WE":
		write_disposition = "WRITE_EMPTY"
	
	job_data = {
            'jobReference': {
                    'projectId': [srcProjectId],
					'jobId': "job_load_" + cycle + "_" + datasetId + "_" + tableId + "_" + tstamp
                    },
            'configuration': {
					'query': {
							 'query': [query],	
							 'allowLargeResults': [True],
							 'writeDisposition': [write_disposition],
							 'destinationTable': {
                                    'projectId': tgtProjectId,
                                    'datasetId': datasetId,
                                    'tableId': tableId
                                    },
                            }
                    }
            }
	
	try:
		return service.jobs().insert(
			projectId=srcProjectId,
			body=job_data).execute(num_retries=num_retries)
	except HttpError as err: 
		print('Error: {}'.format(err.content)) 
		raise err 	

# [END load_table_from_query]


# [START load_table_from_file]
def load_table_from_file(service, source_schema, source_file, projectId, datasetId, tableId, fieldDelimiter, 
			   write_disposition="WT", create_disposition="CIN", skipLeadingRows=0, num_retries=5):
    # Generate a unique job_id so retries
    # don't accidentally duplicate query	
    tstamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y%m%d%H%M%S') + str(datetime.datetime.now()).split('.')[1]
	
   
	
    if write_disposition == "WT":
		write_disposition = "WRITE_TRUNCATE"
    if write_disposition == "WA":
		write_disposition = "WRITE_APPEND"
    if write_disposition == "WE":
		write_disposition = "WRITE_EMPTY"	
    if create_disposition == "CIN":
		create_disposition = "CREATE_IF_NEEDED"	
    if create_disposition == "CN":
		create_disposition = "CREATE_NEVER"	
		
	
    job_data = {
            'jobReference': {
                    'projectId': projectId,
                    'jobId': "job_load_" + datasetId + "_" + tableId + "_" + tstamp #"job_load_" + cycle + "_" + datasetId + "_" + tableId + "_" + tstamp
                    },
            'configuration': {
					#'query': {
					#		 'allowLargeResults': [True]
					#		 },
                    'load': {
							'writeDisposition': [write_disposition],
							'createDisposition': [create_disposition],
							'fieldDelimiter': [fieldDelimiter],
                            'sourceUris': [source_file],
							'skipLeadingRows': [skipLeadingRows],
							'allowQuotedNewlines': [True],
                            'schema': {
                                    'fields': source_schema
                                    },
                            'destinationTable': {
                                    'projectId': projectId,
                                    'datasetId': datasetId,
                                    'tableId': tableId
                                    },
                            }
                    }
            }
    return service.jobs().insert(
        projectId=projectId,
        body=job_data).execute(num_retries=num_retries)
# [END load_table_from_file]


# [BEGIN get_exec_slot]	
def get_exec_slot(projectId, jobs, jobQuota):
	
	job_count = 0
	job_running = 0
	page_token = None
	while True:
		try:
			jobs_list = jobs.list(projectId=projectId, projection='minimal', 
				allUsers=True,
				maxResults=jobQuota + jobQuota, 
				#stateFilter="done", 
				pageToken=None).execute()
		except HttpError as err:
			pass
	
		if jobs_list['jobs'] is not None:
			for job in jobs_list['jobs']:
				job_count = job_count + 1
				if job['state'] == "RUNNING":
					job_running = job_running + 1
					
		if jobs_list.get('nextPageToken') and job_count < jobQuota + jobQuota:
			page_token = jobs_list['nextPageToken']
		else:
			break
	
	#print "running: " + str(job_running) + " quota: " + str(jobQuota)
	return job_running < jobQuota
# [END get_exec_slot]

# [BEGIN running_job_count]
def running_job_count(projectId, jobs, jobQuota):
	
	job_count = 0
	job_running = 0
	page_token = None
	
	while True:
		try:
			jobs_list = jobs.list(projectId=projectId, projection='minimal', 
				allUsers=True,
				maxResults=jobQuota + jobQuota, 
				#stateFilter="done", 
				pageToken=page_token).execute()
		except HttpError as err:
			pass
	
		if jobs_list['jobs'] is not None:
			for job in jobs_list['jobs']:
				job_count = job_count + 1
				if job['state'] == "RUNNING":
					job_running = job_running + 1
					
		if jobs_list.get('nextPageToken') and job_count < jobQuota + jobQuota:
			page_token = jobs_list['nextPageToken']
		else:
			break
	return job_running
# [END running_job_count]


# [START archive_bucket_object]

def archive_bucket_object(bq_service, gs_service, job_list, src_object_prefix, tgt_object_prefix):

	for item in job_list:
		status_request = bq_service.jobs().get(projectId = item['projectId'], jobId = item['jobId'])
		status = status_request.execute(num_retries=2)
		if ((status['status']['state'] == "DONE") and not(status['status'].get('errors'))):
			src_object = status['configuration']['load']['sourceUris'][0]
			src_object = src_object.replace("gs://", "")
			split_char = src_object.find(src_object_prefix)
			src_bucket = src_object[0:split_char - 1]
			src_object = src_object[split_char:len(src_object)]
			tgt_object = src_object.replace(src_object_prefix, tgt_object_prefix, 1)
			temp = move_object(gs_service, src_bucket, src_object, src_bucket, tgt_object)

	return 0		
# [END archive_bucket_object]

def load_pivot_tables(projectId, datasetId, bqService, yearId, qryFile445, qryFileGreg, qryFileListMonths, jobQuota):
    # [START build_service] 
    # Grab the application's default credentials from the environment. 
	    
    query_file_nbr_years = "list-sls_act_years.sql"
	
    cycle = "full"
    write_disposition = "WT"
    tmpJob = bqService.jobs()
    jobList = []
    query = open(qryFileListMonths, "rU").read()
    query = query.replace("?YYYY?", yearId)
    query_data = {
            'query': query,
            'timeoutMs': 10000,
            'maxResults': 100
		}
	
    try:
		query_resp = bqService.jobs().query(projectId=projectId,body=query_data).execute()
		schema = query_resp.get('schema', {'fields': None})['fields']
		rows = query_resp.get('rows', [])
		query_result = sorted([transform_row(row, schema) for row in rows])
		
		for item in query_result:
			
			query = open(qryFile445, "rU").read()
			query = query.replace("?YYYYMM-1?", item['month_id'])
			query = query.replace("?YYYYMM-2?", str(int(item['month_id']) - 100))
			query = query.replace("?YYYYMM-3?", str(int(item['month_id']) - 200))

			while not(get_exec_slot(projectId, tmpJob, jobQuota)):
				print "  running jobs: " + str(running_job_count(projectId, tmpJob, jobQuota))
				print "  I cannot run - waiting 30 secs"
				time.sleep(30)
				
			resp = load_table_from_query(bqService, query, cycle, projectId, projectId, datasetId, 
				"tmp_sls_act_445_" + item['month_id'] + "01", write_disposition, 5)			
			jobList.append(resp['jobReference'])
			print "starting... " + resp['jobReference']['jobId'], qryFile445

			query = open(qryFileGreg, "rU").read()
			query = query.replace("?YYYYMM-1?", item['month_id'])
			query = query.replace("?YYYYMM-2?", str(int(item['month_id']) - 100))
			query = query.replace("?YYYYMM-3?", str(int(item['month_id']) - 200))			

			while not(get_exec_slot(projectId, tmpJob, jobQuota)):
				print "  running jobs: " + str(running_job_count(projectId, tmpJob, jobQuota))
				print "  I cannot run - waiting 30 secs"
				time.sleep(30)			
			
			resp = load_table_from_query(bqService, query, cycle, projectId, projectId, datasetId, 
				"tmp_sls_act_greg_" + item['month_id'] + "01", write_disposition, 5)			
			jobList.append(resp['jobReference'])	
			print "starting... " + resp['jobReference']['jobId'], qryFileGreg
			
		return jobList
    except HttpError as err: 
		print('Error: {}'.format(err.content)) 
		raise err 
		return []


def create_table(bqService, projectId, datasetId, tableId, tblSchema, expiration_time=None):
	"""Create a new table in the dataset.

	Args:
		dataset: the dataset to create the table in.
		table: the name of table to create.
		schema: table schema dict.
		expiration_time: the expiry time in milliseconds since the epoch.

	Returns:
		bool indicating if the table was successfully created or not,
		or response from BigQuery if swallow_results is set for False.
	"""
	
	body = {
		'schema': {'fields': tblSchema},
		'tableReference': {
			'tableId': tableId,
			'projectId': projectId,
			'datasetId': datasetId
		}
	}

	if expiration_time is not None:
		body['expirationTime'] = expiration_time

	try:
		table = bqService.tables().insert(
			projectId=projectId,
			datasetId=datasetId,
			body=body
		).execute()

	except HttpError as err:
		return err
	
	return None	

def get_table_schema(bqService, projectId, datasetId, tableId):
	"""Return the table schema.

	Args:
		dataset: the dataset containing the table.
		table: the table to get the schema for.

	Returns:
		A list of dicts that represent the table schema. If the table
		doesn't exist, None is returned.
	"""

	try:
		result = bqService.tables().get(
			projectId=projectId,
			tableId=tableId,
			datasetId=datasetId).execute()
	except HttpError as err:
		raise err

	return result['schema']['fields']

def create_view(bqService, projectId, datasetId, viewName, query):
	"""Create a new view in the dataset.

	Args:
		dataset: the dataset to create the view in.
		view: the name of view to create.
		query: a query that BigQuery executes when the view is referenced.

	Returns:
		bool indicating if the view was successfully created or not,
		or response from BigQuery if swallow_results is set for False.
	"""

	body = {
		'tableReference': {
			'tableId': viewName,
			'projectId': projectId,
			'datasetId': datasetId
		},
		'view': {
			'query': query
		}
	}

	try:
		view = bqService.tables().insert(
			projectId=projectId,
			datasetId=datasetId,
			body=body
		).execute()
	except HttpError as err:
		return err
	
	return None	

def patch_view(bqService, projectId, datasetId, viewName, query):
	"""Create a new view in the dataset.

	Args:
		dataset: the dataset to create the view in.
		view: the name of view to create.
		query: a query that BigQuery executes when the view is referenced.

	Returns:
		bool indicating if the view was successfully created or not,
		or response from BigQuery if swallow_results is set for False.
	"""

	body = {
		'tableReference': {
			'tableId': viewName,
			'projectId': projectId,
			'datasetId': datasetId
		},
		'view': {
			'query': query
		}
	}

	try:
		view = bqService.tables().patch(
			tableId=viewName, 
			projectId=projectId,
			datasetId=datasetId,
			body=body
		).execute()
	except HttpError as err:
		return err
	
	return None	

def get_all_tables(bqService, projectId, datasetId):
	"""Retrieve a list of all tables for the dataset.

	Args:
		dataset_id: the dataset to retrieve table names for.
		cache: To use cached value or not. Timeout value
			   equals CACHE_TIMEOUT.
	Returns:
		a dictionary of app ids mapped to their table names.
	"""

	result = bqService.tables().list(
		projectId=projectId,
		datasetId=datasetId).execute()

	page_token = result.get('nextPageToken')
	while page_token:
		res = bqService.tables().list(
			projectId=projectId,
			datasetId=datasetId,
			pageToken=page_token
		).execute()
		page_token = res.get('nextPageToken')
		result['tables'] += res.get('tables', [])

	return result['tables']	
	
def delete_table(bqService, projectId, datasetId, tableId):
	"""Delete a table from the dataset.

	Args:
		dataset: the dataset to delete the table from.
		table: the name of the table to delete.

	Returns:
		bool indicating if the table was successfully deleted or not,
		or response from BigQuery if swallow_results is set for False.
	"""

	try:
		response = bqService.tables().delete(
			projectId=projectId,
			datasetId=datasetId,
			tableId=tableId
		).execute()
	except HttpError as err:
		return err
	
	return response	