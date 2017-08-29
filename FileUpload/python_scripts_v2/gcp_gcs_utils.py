#!/usr/bin/env python

# Copyright (C) 2016 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#            http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Application for uploading an object using the Cloud Storage API.

This sample is used on this page:

    https://cloud.google.com/storage/docs/json_api/v1/json-api-python-samples

For more information, see the README.md under /storage.
"""

from googleapiclient import discovery
from googleapiclient import http
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from oauth2client.client import GoogleCredentials
from oauth2client.service_account import ServiceAccountCredentials
#from gcp_log_utils import get_logger
from env_global_variables import *

import inspect


def create_gcs_service(clientSecretFile, scopes):

    credentials = ServiceAccountCredentials.from_json_keyfile_name(clientSecretFile, scopes)

    # You can browse other available api services and versions here:
    #     http://g.co/dev/api-client-library/python/apis/
    return discovery.build('storage', 'v1', credentials=credentials)


def upload_object(service, bucket, folder, object, readers, owners):

    # This is the request body as specified:
    # http://g.co/cloud/storage/docs/json_api/v1/objects/insert#request

	
    body = {
        'name': folder + object[object.rfind("\\") + 1:],
    }

    # If specified, create the access control objects and add them to the
    # request body
    if readers or owners:
        body['acl'] = []

    for r in readers:
        body['acl'].append({
            'entity': 'user-%s' % r,
            'role': 'READER',
            'email': r
        })
    for o in owners:
        body['acl'].append({
            'entity': 'user-%s' % o,
            'role': 'OWNER',
            'email': o
        })

    # Now insert them into the specified bucket as a media insertion.
    # http://g.co/dev/resources/api-libraries/documentation/storage/v1/python/latest/storage_v1.objects.html#insert
    with open(object, 'rb') as f:
        req = service.objects().insert(
            bucket=bucket, body=body,
            media_body= http.MediaIoBaseUpload(f, 'application/octet-stream'))
        resp = req.execute()
    
    return resp

def download_object(service, bucket, object, localDir):

    # Use get_media instead of get to get the actual contents of the object.
    # http://g.co/dev/resources/api-libraries/documentation/storage/v1/python/latest/storage_v1.objects.html#get_media
	
    req = service.objects().get_media(bucket=bucket, object=object)
    if localDir is None:
		local_object = os.getcwd() + "\\" + object
    else:
		local_object = localDir + "\\" + object
    
    out_file = file(local_object, 'w')
	
    downloader = MediaIoBaseDownload(out_file, req)

    done = False
    while done is False:
        status, done = downloader.next_chunk()

    return out_file

def delete_object(service, bucket, object):

    req = service.objects().delete(bucket=bucket, object=object)
    resp = req.execute()

    return resp
	
# [START copy_bucket_object]
def copy_object(gs_service, src_bucket, src_object, tgt_bucket, tgt_object):

    destination_object_resource = {}
    gs_req = gs_service.objects().copy(
		sourceBucket = src_bucket,
		sourceObject = src_object,
		destinationBucket = tgt_bucket,
		destinationObject = tgt_object,
		body=destination_object_resource)
    gs_resp = gs_req.execute()
    return gs_resp

# [END copy_bucket_object]
	
	
# [START move_bucket_object]
def move_object(gs_service, src_bucket, src_object, tgt_bucket, tgt_object):

	temp = copy_object(gs_service, src_bucket, src_object, tgt_bucket, tgt_object)
	temp = delete_object(gs_service, src_bucket, src_object)

# [END move_bucket_object]

	
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('filename', help='The name of the file to upload')
    parser.add_argument('bucket', help='Your Cloud Storage bucket.')
    parser.add_argument('--reader', action='append', default=[],
                        help='Your Cloud Storage bucket.')
    parser.add_argument('--owner', action='append', default=[],
                        help='Your Cloud Storage bucket.')

    args = parser.parse_args()

    main(args.bucket, args.filename, args.reader, args.owner)