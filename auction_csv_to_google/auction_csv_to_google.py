#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  auction_csv_to_google.py
#
#  Copyleft 2017 Maarten De Schrijver
#  <maarten de schrijver in the gmail domain dot com>
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program. If not, see <http://www.gnu.org/licenses/>.
#
#
#######################################################################
#
#  auction_csv_to_google.py
#
#  AWS λ-function to:
#
#   - get's two files from S3 (triggered by SNS-topic):
#       - s3://bdm-auction-exports/clean_csv/latest.csv
#       - s3://bdm-auction-exports/clean_csv/diff.csv
#   - uploads them to Google Drive as spreadsheets
#
#   That's all...
#
#######################################################################

from __future__ import print_function
PY_3 = False
import httplib2
try:
    from urllib.parse import unquote_plus   # Python 3
    PY_3 = True
except ImportError:    
    from urllib import unquote_plus         # Python 2
import os
import logging
import json
from pprint import pprint
# Third party
import pytz
import unicodecsv as csv
from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.http import MediaFileUpload
# pip install google-cloud for storage
#~ from google.cloud import storage
# Boto3
import boto3
from botocore.client import Config

try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None
    
# Get logger
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(lineno)d - %(levelname)s - %(message)s')
# add formatter to ch
ch.setFormatter(formatter)
# add ch to logger
log.addHandler(ch)

# Initialize boto3-clients per container
region_name = 'eu-central-1'
s3_client   = boto3.client('s3', config=Config(signature_version='s3v4'))

GOOGLE_PROJECT_ID = os.environ['GOOGLE_PROJECT_ID']
GOOGLE_LOGIN_EMAIL = os.environ['GOOGLE_LOGIN_EMAIL']
FOLDER_ID = os.environ['FOLDER_ID']

descriptions_by_filename = {
    'latest.csv': 'Laatste CSV-export',
    'diff.csv'  : 'Delta (verschil) tussen laatste CSV-export en CSV van gisteren.'
}
header_list_clean = (
    'ogm',
    'pa_title',
    'auc_title',
    'auc_id',
    'auc_link',
    'high_bid',
    'admin_cost',
    'garant_price',
    'date_high_bid',
    'pay_date',
    'annul_ins',
    'full_option',
    'annul_date',
    'collect_date',
    'extra_info',
    'clang_id',
    'cust_fname',
    'cust_lname',
    'cust_email',
    'cust_street',
    'cust_housenr',
    'cust_hnr_suff',
    'cust_post_code',
    'cust_town',
    'cust_phone',
    'bid_is_suspicious',
)
#~ event_path = lambda: 
evt_src_switcher = {
    # TODO: the following code is Py2 only.
    # Py3 would be: unquote_plus(quoted_string, encoding='utf-8')
    # S3-event is normal
    'aws:s3'    : lambda rcd: unquote_plus(
        rcd['s3']['object']['key'].encode('utf8')),
    # SNS-event has the S3-event in it's Message-attribute
    'aws:sns'   : lambda rcd: unquote_plus(json.loads(
        rcd['Sns']['Message'])['Records'][0]['s3']['object']['key'].encode('utf8')),
}

def get_s3_record(event):
    if event['Records'][0]['EventSource'] == 'aws:s3':
        return event['Records'][0]['s3']
    elif event['Records'][0]['EventSource'] == 'aws:sns':
        return json.loads(event['Records'][0]['Sns']['Message'])['Records'][0]['s3']

def get_key_from_event(event):
    quoted_key = get_s3_record(event)['object']['key']
    if PY_3:
        return unquote_plus(quoted_key, encoding='utf8')
    else:
        return unquote_plus(quoted_key.encode('utf8'))
        #~ return evt_src_switcher[event['Records'][0]['EventSource']](event['Records'][0])

def get_bucket_from_event(event):
    quoted_bucket = get_s3_record(event)['bucket']['name']
    if PY_3:
        return unquote_plus(quoted_bucket, encoding='utf8')
    else:
        return unquote_plus(quoted_bucket.encode('utf8'))

def get_delegated_credentials(email):
    log.debug('Authenticating with delegated user creds...')
    json_file = os.environ['JSON_FILE']
    scopes = ['https://www.googleapis.com/auth/drive', ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(json_file, scopes)
    return creds.create_delegated(email)

def csv_to_list_of_tuples(csvfile_path):
    """Read in a csv file (from path) and return a list of tuples.
       Header line is skipped."""
    with open(csvfile_path, 'rb') as f:
        rdr = csv.reader(f, delimiter=',', quotechar='"')
        # Skip header line
        #~ header = rdr.next()
        return list(map(tuple, rdr))

def lot_to_csv_file(lot, file_path, quoting=csv.QUOTE_ALL):
    with open(file_path, 'wb') as f:
        wrt = csv.writer(f, delimiter=',', quotechar='"', quoting=quoting)
        # Insert header row again
        #~ wrt.writerow(header_list_clean)
        for line in lot:
            l = ["'"+field for field in line]
            wrt.writerow(tuple(l))

def prepare_csv_for_google(csvfile_path):
    # Read to tuples
    lot = csv_to_list_of_tuples(csvfile_path)
    # Change date timezone?
    # not for now: keep UTC
    # Save back to file_path
    lot_to_csv_file(lot, csvfile_path)

def delete_previous_sheets(service, folder_id, list_of_names=[]):
    query = "'" + folder_id + "'" + " in parents"
    f = service.files().list(q=query).execute()
    ids_to_delete = [r['id'] for r in f['files'] if r['name'] in list_of_names]
    for sh_id in ids_to_delete:
        log.debug('Deleting: %s', sh_id)
        response = service.files().delete(fileId=sh_id).execute()


def lambda_handler(event, context):
    if context:
        bucket   = get_bucket_from_event(event)
        key      = get_key_from_event(event)
        filename = key.split('/')[-1:][0]
        log.info("Handling key %s in bucket %s.", key, bucket)
        log.debug("Filename is: %s.", filename)
        # Get object from S3
        with open('/tmp/tmp.csv', 'wb') as f:
            s3_client.download_fileobj(bucket, key, f)
    # Read in csv and prepare for Google Sheets
    prepare_csv_for_google('/tmp/tmp.csv')
    # Google Auth
    credentials = get_delegated_credentials(google_login_email)
    http = credentials.authorize(httplib2.Http())
    service = discovery.build('drive', 'v3', http=http)
    # Find and delete previous sheets in folder
    delete_previous_sheets(service, FOLDER_ID, [filename, ])
    file_metadata = {
      'name' : filename,
      'description': descriptions_by_filename.get(filename),
      'parents': [ FOLDER_ID ],
      'mimeType' : 'application/vnd.google-apps.spreadsheet',
    }
    media = MediaFileUpload('/tmp/tmp.csv', mimetype='text/csv', resumable=True)
    f = service.files().create(body=file_metadata, media_body=media,
                               fields='id').execute()
    log.info('File ID: %s', f.get('id'))
    # Upload to Google Cloud Storage
    #~ storage_service = storage.Client(project=GOOGLE_PROJECT_ID)

def main():
    from mock_event import event
    lambda_handler(event, True)
    return 0
    
if __name__ == '__main__':
    main()


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
