#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  auction_csv_to_s3.py
#
#  Copyleft 2017 Mali Media Group
#  <http://malimedia.be>
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
###############################################################################
#
#  auction_csv_to_s3.py
#
#  AWS λ-function to:
#
#   - download the csv-auction export from:
#       <auction_export_url> (environment variable)
#   - preform some basic checks: catch potential errors early...
#   - ship it to s3://bdm-auction-exports/raw_csv/yyyy/mm/...
#
#   This function is time-triggered via CloudWatch rules
#
#   That's all...
#
###############################################################################


import logging
from datetime import datetime
from os.path import getsize
# 3th party
import requests
import boto3
from botocore.client import Config

# Logging
log = logging.getLogger('auction_csv_to_s3')
log.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(message)s')
ch.setFormatter(formatter)
log.addHandler(ch)

# Some constants
# auction_export_url:
# u.format(date=today) today as 'yyyy-mm-dd' -> date.today().strftime('%Y-%m-%d')
auction_export_url = os.environ['AUCTION_EXPORT_URL']
# Default error subject
DEFAULT_ERR_SUBJ = 'Error in: auction_csv_to_s3.py'
DEFAULT_ERR_MSG  = 'Error in: auction_csv_to_s3.py'

# Download CSV as file to /tmp: easier for the csv-module
# instead of: csv_string = response['Body'].read()
tmp_file = '/tmp/tmp.csv'

# Bucket & path to ship to
bucket      = 'bdm-auction-exports'
s3_key_fmt  = 'raw_csv/{y}/{m}/auctions-{y}-{m}-{d}.csv'
s3_path_fmt = 's3://bdm-auction-exports/raw_csv/{y}/{m}/auctions-{y}-{m}-{d}.csv'

# Initialize boto3-clients per container
region_name = 'eu-central-1'
s3_client   = boto3.client('s3', config=Config(signature_version='s3v4'))


def send_out_warning(subject=DEFAULT_ERR_SUBJ,
                     msg=DEFAULT_ERR_MSG,
                     short_msg=''):
    """Send out a warning through AWS SNS."""
    client = boto3.client('sns', region_name='eu-west-1')
    response = client.publish(
        TopicArn            = os.environ['TOPIC_ARN'],
        #~ MessageStructure    = 'json',
        Subject             = subject,
        Message             = msg,
    )

def check_response(response):
    if not response.status_code in (200, 201):
        raise Exception

def check_csv(csv_file_path):
    size = getsize(csv_file_path)
    if size < 100*1000:
        raise Exception('File size unusually small: %s bytes' % size)

def add_object_to_S3(file_path, key, tag_dict=None):
    if tag_dict:
        func_params['Tagging'] = urllib.urlencode(tag_dict)
    with open(tmp_file, 'rb') as f:
        func_params = {
            'Key' : key,
            'Bucket': bucket,
            'ContentType' : 'text/csv',
            'ContentEncoding' : 'utf-8',
            'Body' : f,
        }
        res = s3_client.put_object(**func_params)
    return res

def lambda_handler(event, context):
    log.debug(event)
    log.debug(context)
    if not context:
        today_str = event.today
    else:
        today_str = datetime.today().strftime('%Y-%m-%d')
    y = today_str.split('-')[0:1][0]
    m = today_str.split('-')[1:2][0]
    d = today_str.split('-')[2:3][0]
    log.debug('%s, %s, %s' % (y, m, d))
    url = auction_export_url.format(date=today_str)
    log.debug('URL is: %s.', url)
    s3_path = s3_path_fmt.format(y=y, m=m, d=d)
    s3_key  = s3_key_fmt.format(y=y, m=m, d=d)
    log.debug('S3 path is: %s.', s3_path)
    try:
        response = requests.get(url)
    except requests.exceptions.ConnectionError as e:
        log.error(e)
        send_out_warning(msg='Request failed: %s' % e)
    else:
        try:
            check_response(response)
        except Exception as e:
            log.error(e)
            send_out_warning(msg='Error in response: %s' % response.text)
        else:
            with open(tmp_file, 'wb') as f:
                s = f.write(response.content)
            try:
                check_csv(tmp_file)
            except Exception as e:
                log.error(e)
                send_out_warning(msg='CSV not ok.\n%s\nExport URL is: "%s"' % (e, url))
                # Raise the exception again to Lambda
                raise
            else:
                log.info("All good...")
                # Upload to S3
                r = add_object_to_S3(tmp_file, s3_key)
                log.debug(r)

def main():
    class event(object):
        today = '2017-08-21'
    lambda_handler(event, None)
    return 0

if __name__ == '__main__':
    main()


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
