#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  auction_csv_to_raw_mysql.py
#
#  Copyleft 2017 Maarten De Schrijver
#  <http://maartendeschrijver.me>
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
#########################################################################
#
# This AWS λ-function:
#
#   - downloads /clean_csv/diff.csv from s3://bdm-auction-export
#   - adds (via REPLACE) these records to `AuctionsRaw`-table on
#     MySQL RDS
#   - is triggered by the arrival of the diff.csv file on S3
#     (via SNS fan-out)
# 
#   ENV VARS:
#       - MYSQL_DB_PASSWORD
# 
##########################################################################

# System imports
from __future__ import print_function
import os
import logging
import json
PY_3 = False
try:                    
    from urllib.parse import unquote_plus   # Python 3
    PY_3 = True
except ImportError:    
    from urllib import unquote_plus         # Python 2
import pymysql
from pymysql.err import IntegrityError
from base64 import b64decode
# 3th party
import unicodecsv as csv
import boto3
from botocore.client import Config


# Logging
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(message)s')
ch.setFormatter(formatter)
log.addHandler(ch)

# Decrypt function to retrieve sensitive key-value pairs from AWS KMS
def decrypt(env_var):
    ENCRYPTED = os.environ[env_var]
    return boto3.client('kms').decrypt(CiphertextBlob=b64decode(ENCRYPTED))['Plaintext']

def make_fields_list(header_list):
    return ', '.join(['`{}`'.format(f[1]) for f in header_list])

def make_create_fields_list(header_list):
    return ', '.join(['`{}` {}'.format(f[1], f[2]) for f in header_list])

# Some constants
MYSQL = {
    'type'        : 'mysql',
    'host'        : os.environ['MYSQL_HOST'],
    'port'        : 3306,
    'db_name'     : os.environ['MYSQL_DB_NAME'],
    'db_username' : os.environ['MYSQL_DB_USERNAME'],
    'db_password' : decrypt('MYSQL_DB_PASSWORD'),
    'table_name'  : os.environ['MYSQL_TABLE_NAME'],
}

HEADER_LIST = [
    ('OGM_code', 'ogm', 'VARCHAR(12) UNIQUE NOT NULL'),
    ('Partner_Titel', 'pa_title', 'VARCHAR(255)'),
    ('Veiling_Titel', 'auc_title', 'VARCHAR(255)'),
    ('Veiling_ID', 'auc_id', 'INTEGER UNIQUE NOT NULL'),
    ('Veiling_link', 'auc_link', 'VARCHAR(255)'),
    ('Hoogste_bod', 'high_bid', 'DECIMAL(5,2)'),
    ('Administratiekost', 'admin_cost', 'DECIMAL(5,2)'),
    ('Garante_prijs', 'garant_price', 'DECIMAL(5,2)'),
    ('Datum_Hoogste_bod', 'date_high_bid', 'DATETIME'),
    ('Betaal_datum', 'pay_date', 'DATETIME'),
    ('Annuleringsverzekering', 'annul_ins', 'DECIMAL(5,2)'),
    ('Full_option', 'full_option', 'DECIMAL(5,2)'),
    ('Annulatie_datum', 'annul_date', 'DATETIME'),
    ('Inningsdatum', 'collect_date', 'DATETIME'),
    ('Extra_informatie', 'extra_info', 'TEXT'),
    ('Clang_ID', 'clang_id', 'INTEGER'),
    ('Klant_Voornaam', 'cust_fname', 'VARCHAR(255)'),
    ('Klant_Achternaam', 'cust_lname', 'VARCHAR(255)'),
    ('Klant_Email', 'cust_email', 'VARCHAR(255)'),
    ('Klant_Straat', 'cust_street', 'VARCHAR(255)'),
    ('Klant_Nummer', 'cust_housenr', 'VARCHAR(255)'),
    ('Klant_Toevoeging', 'cust_hnr_suff', 'VARCHAR(255)'),
    ('Klant_Postcode', 'cust_post_code', 'VARCHAR(255)'),
    ('Klant_Gemeente', 'cust_town', 'VARCHAR(255)'),
    ('Klant_Telefoon', 'cust_phone', 'VARCHAR(255)'),
    ('bid_is_suspicious', 'bid_is_suspicious', 'BOOLEAN'),
]

IS_SUSP_FIELD = ()

CREATE_SQL = """CREATE TABLE `{table_name}` (
    {field_list}, PRIMARY KEY ( auc_id )
) DEFAULT CHARSET=utf8;""".format(table_name=MYSQL['table_name'],
             field_list=make_create_fields_list(HEADER_LIST))

INSERT_SQL = """REPLACE INTO `{table_name}` ({field_list}) VALUES (
    {plcs}
);""".format(table_name=MYSQL['table_name'],
            field_list=make_fields_list(HEADER_LIST),
            plcs=','.join(['%s' for i in range(len(HEADER_LIST))]))

tmp_file = '/tmp/diff.csv'

# Initialize boto3-clients per container
region_name = 'eu-central-1'
s3_client   = boto3.client('s3', config=Config(signature_version='s3v4'))

# Init MySQL conn
db_conn = pymysql.connect(MYSQL['host'],
    port=MYSQL['port'],
    user=MYSQL['db_username'],
    passwd=MYSQL['db_password'],
    db=MYSQL['db_name'],
    charset='utf8',
    connect_timeout=5)

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

def csv_to_list_of_tuples(csvfile_path):
    """Read in a csv file and return a list of tuples."""
    with open(csvfile_path, 'rb') as f:
        rdr = csv.reader(f, delimiter=',', quotechar='"')
        # Skip header line
        header = rdr.next()
        return list(map(tuple, rdr))

def lambda_handler(event, context):
    bucket   = get_bucket_from_event(event)
    key      = get_key_from_event(event)
    log.debug("Bucket is: %s", bucket)
    log.debug("Key is: %s", key)
    if key != 'clean_csv/diff.csv':
        log.info("Exiting: not diff.csv")
    else:
        # Get today's diff file
        log.info('Get %s from %s.', key, bucket)
        with open(tmp_file, 'wb') as f:
            s3_client.download_fileobj(bucket, key, f)
        diff_lot = csv_to_list_of_tuples(tmp_file)
        log.debug(diff_lot[0])
        log.debug(INSERT_SQL)
        with db_conn.cursor() as cursor:
            for row in diff_lot:
                cursor.execute(INSERT_SQL, row)
        db_conn.commit()

def main():
    from mock_event import event
    lambda_handler(event, None)
    return 0

if __name__ == '__main__':
    main()

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
