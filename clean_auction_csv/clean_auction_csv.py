#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  clean_auction_csv.py
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
#
###############################################################################
#
#  clean_auction_csv.py
#
#  AWS λ-function to:
#
#   - get/read today's raw csv from S3
#   - clean it:
#       - format fields (datetime to UTC, strip away chars in OGM, ...)
#       - find and remove bad lines (send out warning)
#       - mark suspicious bids
#   - filter it:
#       - remove rows from own domains and emails
#   - save to S3 on /clean_csv (which will trigger the diff-fn):
#       - rename clean_csv/latest.csv -> clean_csv/yesterday.csv
#       - upload new to clean_csv/latest.csv
#
#   That's all...
#
###############################################################################


import logging
import urllib
import gzip
import decimal
from datetime import datetime, timedelta
from collections import OrderedDict
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO
# 3th party
import unicodecsv as csv
import pytz
import requests
import boto3
from botocore.client import Config
import botocore.exceptions as boto_exceptions

decimal.getcontext().prec = 2

# Logging
log = logging.getLogger('clean_auction_csv')
log.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(message)s')
ch.setFormatter(formatter)
log.addHandler(ch)

# Download CSV as file to /tmp: easier for the csv-module
# instead of: csv_string = response['Body'].read()
tmp_file = '/tmp/tmp.csv'

tmp_names = {
    'today': {
        'tmp': '/tmp/current_csv.csv',
        'tmp_clean': '/tmp/current_csv_clean.csv',
        's3_key' : 'clean_csv/latest.csv',
    },
    'yesterday': {
        'tmp': '/tmp/yesterday_csv.csv',
        'tmp_clean': '/tmp/yesterday_csv_clean.csv',
        's3_key' : 'clean_csv/yesterday.csv',
    },
    'diff': {
        'tmp': '/tmp/diff_csv.csv',
        'tmp_clean': '/tmp/diff_csv_clean.csv',
        's3_key' : 'clean_csv/diff.csv',
    },
    'all': {
        'tmp': '/tmp/all_csv.csv',
        'tmp_clean': '/tmp/all_csv_clean.csv',
        's3_key' : 'clean_csv/all.csv',
    },
}


# Bucket 
bucket      = os.environ['BUCKET']

# Initialize boto3-clients per container
region_name = 'eu-central-1'
s3_client   = boto3.client('s3', config=Config(signature_version='s3v4'))

# Validation/clean function
def format_quoted_field(val):
    return val.strip('"=')

def trim(val):
    return val.strip()

def trim_lower(val):
    return val.strip().lower()

def trim_title(val):
    return val.strip().title()

def field_to_decimal(val):
    """Convert a numeric field to a proper value
       for 'Decimal'-type. (Replaces ',' by '.')
       Returns 0 if empty.
    """
    return decimal.Decimal(val.replace(',', '.')) if val else decimal.Decimal(0.0)

def datetime_to_utc(val):
    """Convert a datetime à la "2017-01-12 17:23:29" in Europe/Brussels to
       "2017-01-12T16:23:29Z" in UTC.
    """
    if val:
        ts = datetime.strptime(val, "%Y-%m-%d %H:%M:%S")
        dt = pytz.timezone('Europe/Brussels').localize(ts)
        return dt.astimezone(pytz.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    return None

# TODO: add these to DDB-table for instance
DOMAIN_FILTER = [
    'example.com',
]
EMAIL_FILTER = [
    'alice@somedomain.com',
]
HEADER_LIST = [
    # Index, Original name,       Clean name, format fn, is_rel_field
]
#
CLEAN_DICT = OrderedDict([(x[0], x[3]) for x in HEADER_LIST])

# auc_id, pay_date, annul_date, collect_date
RELFIELDS = [x[0] for x in HEADER_LIST if x[4]]

def is_not_known(email):
    try:
        dom = email.split('@')[1]
    except IndexError as e:
        return False
    else:
        if dom.lower() in DOMAIN_FILTER or email.lower() in EMAIL_FILTER:
            return False
        else:
            return True

def bid_is_suspicious(row):
    bid     = row[5]
    cost    = row[7]
    max_bid = 800
    payed_or_cancelled = row[9] or row[12]
    if not payed_or_cancelled:
        if cost:
            # TODO: find better formula
            #~ if (bid/cost) > 0.3*((int(bid)-int(cost))/(bid/cost)):
            if (bid/cost) > 5 or bid > max_bid:
                return True
            return False
        else:
            if bid > max_bid:
                return True
            return False
    return False

def lot_to_set_of_reltuples(lot, relfields=[]):
    """Generate a set of relevant tuples from a list of tuples.
       `relfields` is a list of fields (indices) to preserve.
    """
    return set([tuple([row[n] for n in relfields]) for row in lot])

def add_in_element(lst, index):
    lst.insert(index, u'')
    return lst

def csv_to_list_of_tuples(csvfile_path):
    """Read in a csv file (from path) and return a list of tuples.
       Header line is skipped."""
    with open(csvfile_path, 'rb') as f:
        rdr = csv.reader(f, delimiter=';', quotechar='"')
        # Skip header line
        header = rdr.next()
        if 'Klant Toevoeging' in header:
            return list(map(tuple, rdr))
        else:
            return list(map(tuple, [add_in_element(r, 21) for r in rdr]))

def find_bad_lines(lot):
    """Finds the "bad" lines in a csv line.
       "Bad" only means: a line/row of different length then the first line.
       (We assume the first line is correct).
       Returns a dict with the lines indices as keys."""
    i = 0
    bad_lines = dict()
    # Assume first line has the correct length (# of fields).
    csv_row_length = len(lot[0])
    for t in lot:
        if len(t) != csv_row_length:
            bad_lines[i] = t
        i += 1
    if bad_lines:
        log.warn('%s bad_lines lines found.' % len(bad_lines))
    return bad_lines

def remove_bad_lines(lot, bad_lines):
    """Remove bad lines from list of tuples based on bad lines' indices"""
    correct_list = [i for j, i in enumerate(lot) if j not in bad_lines.keys()]
    return correct_list

def clean_list_of_tuples(lot):
    """Apply function to every element and drop last column (clang_error)."""
    r = [tuple([func(row[n]) for n, func in CLEAN_DICT.iteritems()][:-1]) for row in lot]
    return r

def filter_list_of_tuples(lot):
    r = [row for row in lot if is_not_known(row[18])]
    return r

def get_yesterday(filename):
    # Return yesterday's date (as object) based on today's date
    today_str = '-'.join(filename.split('.')[:-1][0].split('-')[1:4])
    return datetime.strptime(today_str, '%Y-%m-%d') - timedelta(days=1)

def key_from_date(date_obj):
    """Generate S3-key for raw_csv"""
    y = date_obj.strftime('%Y')
    m = date_obj.strftime('%m')
    d = date_obj.strftime('%d')
    s3_key_fmt  = 'raw_csv/{y}/{m}/auctions-{y}-{m}-{d}.csv'
    return s3_key_fmt.format(y=y, m=m, d=d)

def get_diff_lot(set_diff, lot):
    """Return those tuples from `lot` (list of tuples) who's ID's are
       in `set_diff`.
       Return list of tuples as well."""
    auc_ids = [x[0] for x in set_diff]
    return [tup for tup in lot if tup[3] in auc_ids]

def lot_to_csv_file(lot, file_path, quoting=csv.QUOTE_MINIMAL):
    with open(file_path, 'wb') as f:
        wrt = csv.writer(f, delimiter=',', quotechar='"', quoting=quoting)
        # Write header
        header_list_clean = [x[2] for x in HEADER_LIST][:-1]
        header_list_clean.append('bid_is_suspicious')
        wrt.writerow(header_list_clean)
        for line in lot:
            l = list(line)
            l.append(bid_is_suspicious(line))
            wrt.writerow(tuple(l))

def send_bad_lines_warning(filename, bad_lines):
    """Send out a warning about this csv-file with a summary of the bad
       lines found."""
    msg = '\n'.join([':'.join([str(k), ','.join([f for f in v])]) for k, v in bad_lines.items()])
    client = boto3.client('sns')
    response = client.publish(
        TopicArn    = 'arn:aws:sns:eu-central-1:625469223576:Corrupt_Auction_CSV',
        Subject     = 'OPGELET: corrupte csv: %s corrupte lijnen in %s.' % (len(bad_lines), filename),
        Message     = msg,
    )
    log.warn('%s bad lines found in csv "%s".' % (len(bad_lines), filename))
    log.info('Warning sent out via email.')
    
def add_object_to_S3(file_path, key, bucket, tag_dict={}, compressed=False):
    func_params = {
        'Key'   : key,
        'Bucket': bucket,
        'ACL'   : 'private',
        'ContentType' : 'text/csv',
        'ContentEncoding' : 'utf-8',
        'Tagging' : urllib.urlencode(tag_dict),
    }
    with open(file_path, "rb") as f:
        data = f.read()
    if compressed:
        func_params['Key'] = '.'.join([key, 'gz'])
        func_params['ContentEncoding'] = 'gzip'
        out = StringIO()
        with gzip.GzipFile(fileobj=out, mode="w") as f:
            f.write(data)
        data = out.getvalue()
    func_params['Body'] = data
    return s3_client.put_object(**func_params)

def change_object_key(from_key, to_key, bucket):
    func_params = {
        'Bucket'        : bucket,
        'CopySource'    : '/'.join([bucket, from_key]),
        'Key'           : to_key,
    }
    # Copy old object to new key
    res = s3_client.copy_object(**func_params)
    # Delete old object
    func_params = {
        'Bucket' : bucket,
        'Key'    : from_key,
    }
    return s3_client.delete_object(**func_params)
    
    
def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))
    log.info('Handling key "%s" in bucket "%s".', key, bucket)
    filename = key.split('/')[-1:][0]
    # Get today's file (raw)
    if context:
        log.info('Get %s from %s.', key, bucket)
        with open(tmp_names['today']['tmp'], 'wb') as f:
            s3_client.download_fileobj(bucket, key, f)
    else:
        log.debug('No context: local test, no file downloaded.')
    lot = csv_to_list_of_tuples(tmp_names['today']['tmp'])
    bad_lines = find_bad_lines(lot)
    if bad_lines:
        send_bad_lines_warning(filename, bad_lines)
        lot = remove_bad_lines(lot, bad_lines)
    clean_lot = clean_list_of_tuples(lot)
    log.debug('Cleaned list: %s elements', len(clean_lot))
    filter_lot = filter_list_of_tuples(clean_lot)
    log.debug('Filtered list: %s elements (diff=%s)', len(filter_lot), len(clean_lot)-len(filter_lot))
    # Change latest.csv on S3 to yesterday.csv
    # TODO: we could also solve this through object-versioning
    log.info('Changing "%s" to "%s" on S3-bucket "%s".',
                tmp_names['today']['s3_key'],
                tmp_names['yesterday']['s3_key'],
                bucket)
    res = change_object_key(from_key=tmp_names['today']['s3_key'],
                            to_key=tmp_names['yesterday']['s3_key'],
                            bucket=bucket)
    # Current CSV to S3
    log.info('Save to tmp file: %s.', tmp_names['today']['tmp_clean'])
    lot_to_csv_file(filter_lot, tmp_names['today']['tmp_clean'])
    log.info('Save %s to S3.', tmp_names['today']['s3_key'])
    if context:
        res = add_object_to_S3(tmp_names['today']['tmp_clean'],
                               tmp_names['today']['s3_key'],
                               bucket,
                               tag_dict={'raw_object': filename})
        log.debug(res)
    else:
        log.debug('No context: local test, no file uploaded.')

def main():
    from mock_event import event
    lambda_handler(event, True)
    return 0

if __name__ == '__main__':
    main()


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
