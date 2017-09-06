#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  bdm_event_lead_trigger.py
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
#########################################################################
#
# This AWS λ-function:
#
#   - is triggered by the arrival of a JSON lead on S3
#       (s3://bdm-events/leads/)
#       (which in itself was triggered by the campaign_entries λ-function)
#   - reads in information from the JSON file and (based on config from
#       DDB):
#       - adds to RDS (MySQL: Contacts, ContactsCampaigns)
#       - adds to Mailjet, if needed
#       - can add campaign to Campaigns (MySQL) if it doesn't exist
# 
##########################################################################

# System imports
from __future__ import print_function
import os
import logging
import json
import urllib
import uuid
import random
import pymysql
from datetime import datetime
from pymysql.err import IntegrityError
from base64 import b64decode

import boto3
from botocore.client import Config
from mailjet_rest import Client


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

# Some constants
MYSQL = {
    'type'        : 'mysql',
    'host'        : os.environ['MYSQL_HOST'],
    'port'        : 3306,
    'db_name'     : 'mmgmysqldb',
    'db_username' : 'mmgmysqluser',
    'db_password' : decrypt('MYSQL_DB_PASSWORD'),
    'table_name'  : 'Contacts',
}

# Initialize boto3-clients per container
region_name = 'eu-central-1'
s3_client   = boto3.client('s3',
                config=Config(signature_version='s3v4'))
ddb_client  = boto3.client('dynamodb',
                region_name=region_name, 
                endpoint_url="https://dynamodb.eu-central-1.amazonaws.com")

# Init MySQL conn
db_conn = pymysql.connect(MYSQL['host'],
    port=MYSQL['port'],
    user=MYSQL['db_username'],
    passwd=MYSQL['db_password'],
    db=MYSQL['db_name'],
    connect_timeout=5)

# Get and decrypt environment variables (store globally)
# Mailjet ADD
MJ_ADD_APIKEY_PUBLIC    = decrypt('MJ_ADD_APIKEY_PUBLIC')
MJ_ADD_APIKEY_PRIVATE   = decrypt('MJ_ADD_APIKEY_PRIVATE')
# Mailjet TRANSACTIONAL (welcome mail)
MJ_TRANS_APIKEY_PUBLIC    = decrypt('MJ_TRANS_APIKEY_PUBLIC')
MJ_TRANS_APIKEY_PRIVATE   = decrypt('MJ_TRANS_APIKEY_PRIVATE')

# Init MJ API for Main Account
Mailjet_Main = Client(auth=(MJ_ADD_APIKEY_PUBLIC, MJ_ADD_APIKEY_PRIVATE))


def send_out_warning(subject, msg, short_msg=''):
    """Send out a warning through AWS SNS."""
    client = boto3.client('sns', region_name='eu-west-1')
    response = client.publish(
        TopicArn            = os.environ['TOPIC_ARN'],
        #~ MessageStructure    = 'json',
        Subject             = subject,
        Message             = msg,
    )

def split_email(email):
    email_cleaned   = None
    email_local     = None
    email_domain    = None
    email_tld       = None
    err_msg         = None
    email_cleaned   = email.strip().lower()
    # First, check for multiple '@' and issue warning
    parts = email_cleaned.split('@')
    if len(parts) == 2:
        email_local = parts[0]
        email_domain = parts[1]
        email_domains = email_domain.split('.')
        if len(email_domains) > 1:
            email_tld = email_domains[-1:][0]
        else:
            err_msg = 'No domain'
    elif len(parts) == 1:
        err_msg = 'No @-character'
        email_local = None
        email_domain = None
    elif len(parts) > 2:
        err_msg = 'More then one @-character'
        email_local = None
        email_domain = None
    return email_cleaned, email_local, email_domain, email_tld, err_msg

def mailjet_get(contact_id_or_email, with_data=True,
                with_subscriptions=True, with_msg_stats=True):
    """Get the contact data for this ID or email.
    Defaults to getting ALL the data + subscriptions."""
    result = Mailjet_Main.contact.get(id=contact_id_or_email)
    if result.status_code == 200:
        con = result.json()['Data'][0]
        if with_data:
            res = Mailjet_Main.contactdata.get(id=contact_id_or_email)
            con['ContactData'] = res.json()['Data'][0]['Data']
        if with_subscriptions:
            filters={'Contact': con['ID']}
            res = Mailjet_Main.listrecipient.get(filters=filters)
            con['Subscriptions'] = list()
            con['Subscriptions'].extend(res.json()['Data'])
        if with_msg_stats:
            filters = {'ContactEmail': con['Email']}
            res = Mailjet_Main.messagestatistics.get(filters=filters)
            con['MessageStatistics'] = res.json()['Data'][0]
        return con
    # Contact not found
    elif result.status_code == 404:
        return False
    # Error during GET: bad emailaddress?
    elif result.status_code == 400:
        print('Status: %s - Reason: %s', result.status_code, result.reason)
        return False

def get_uuid_and_long(email):
    sql = 'SELECT * FROM `Contacts` WHERE email_cleaned = %s'
    with db_conn.cursor() as cursor:
        cursor.execute(sql, (email, ))
        res = cursor.fetchone()
    if res:
        return res[0], uuid.UUID(res[0]).int & (1<<32)-1
    return False, False

def gen_uuid4_and_long():
    u = uuid.uuid4()
    return str(u), u.int & (1<<32)-1
    
def make_mpt_dict(email, uuid, seg_num, event_body, campaign, mj_contact):
    """Take all top-level (ie, string or unicode) attributes from the
       event_body (ie., the S3-object) and put them together with some
       other properties into a flat dict.
       This dict will populate the contactdata to add in Mailjet."""
    mpt_dict = dict()
    if mj_contact:
        c_at = mj_contact['CreatedAt']
    else:
        c_at = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    for k, v in event_body['data'].items():
        if isinstance(v, str) or isinstance(v, unicode):
            mpt_dict[k] = v
    mpt_dict['CampaignDecimal'] = float(campaign['CampaignDecimal']['N'])
    mpt_dict['email']           = email
    mpt_dict['created_at']      = c_at
    mpt_dict['uuid']            = uuid
    mpt_dict['seg_num']         = seg_num
    mpt_dict['block']           = random.randint(1,6)
    return mpt_dict

def mailjet_main_add(email, uuid, seg_num, event_body, campaign, mj_contact):
    list_id = int(campaign['SubscribesTo']['L'][0]['M']['ListID']['N'])
    log.debug('MJ list ID: %s.', list_id)
    # "Action": "addforce" is in the mapping_template
    mapping_template = campaign['MJMappingTemplate']['S']
    # Flatten dicts to use for string formatting
    mpt_dict = make_mpt_dict(email, uuid, seg_num, event_body, campaign, mj_contact)
    data_string = mapping_template % mpt_dict
    data = json.loads(data_string)
    response = Mailjet_Main.contactslist_managecontact.create(id=list_id, data=data)
    s = int(response.status_code) 
    if s not in (200, 201):
        log.error(response)
        log.error(response.status_code)
        log.error(response.text)
        log.error(response.headers)
        send_out_warning('Mailjet adding failed: %s' % response.status_code,
                         'Email: %s. \n\nText: %s. \n\nHeaders: %s.' % (email, response.text, response.headers))
        return False
    else:
        log.debug(response.text)
        return response.json()['Data'][0]

def send_welcome_mail(mailjet, campaign, email):
    t_id  = int(campaign['WelcomeMail']['M']['TemplateID']['N'])
    data = {
        'FromEmail': 'veilingen@biedmee.be',
        'FromName': 'Biedmee.be',
        'Subject': 'Welkom bij Biedmee.be!',
        'MJ-TemplateID': t_id,
        'MJ-TemplateLanguage': 'false',
        'Recipients': [{ "Email": email }]
    }
    result = mailjet.send.create(data=data)
    return result.json()

def ddb_contacts_add(uuid, event_body, campaign):
    pass

def rds_contact_add(uuid, email_tuple, event_body, campaign, location):
    con_tuple = (
        uuid,
        email_tuple[0],
        email_tuple[0],
        email_tuple[1],
        email_tuple[2],
        email_tuple[3],
        email_tuple[4],
    )
    ins_sql = """INSERT INTO `Contacts` (
        `uuid`,
        `email`,
        `email_cleaned`,
        `email_local`,
        `email_domain`,
        `email_tld`,
        `err_msg`)
    VALUES ({plcs});""".format(plcs=','.join(['%s' for i in range(len(con_tuple)) ]))
    with db_conn.cursor() as cursor:
        cursor.execute(ins_sql, con_tuple)
    db_conn.commit()

def rds_campaign_add(campaign):
    # 4th field `created_at` DEFAULT CURRENT_TIMESTAMP
    cam_tuple   = (
        campaign['UUID']['S'], campaign['CampaignShortName']['S'],
        campaign['CampaignDecimal']['N']
    )
    ins_sql = """INSERT INTO `Campaigns` (uuid, short_name, campaign_decimal)
    VALUES ({plcs});""".format(plcs=','.join(['%s' for i in range(len(cam_tuple)) ]))
    with db_conn.cursor() as cursor:
        cursor.execute(ins_sql, cam_tuple)
    db_conn.commit()

def rds_contactcampaign_add(contact_uuid, email_tuple, event_body, campaign, location):
    try:
        time_stamp = event_body['data']['timestamp']
    except KeyError as e:
        time_stamp = event_body['meta']['time_stamp']
    created_at_day = time_stamp.split('T')[0]
    concam_tuple  = (
        contact_uuid, campaign['UUID']['S'], time_stamp, created_at_day,
        event_body['data']['source_ip'], location
    )
    log.debug(concam_tuple)
    concam_sql = """INSERT INTO `ContactsCampaigns`
    VALUES ({plcs});""".format(plcs=','.join(['%s' for i in range(len(concam_tuple)) ]))
    with db_conn.cursor() as cursor:
        # Add to `ContactsCampaigns`
        try:
            cursor.execute(concam_sql, concam_tuple)
        except IntegrityError as e:
            log.warn(e)
            # Duplicate entry? (1062)
            # Contact already came in through this campaign on this day
            if e.args[0] == 1062:
                pass
            # FK-constraint? (1452)
            # Should be missing Campaign
            elif e.args[0] == 1452:
                rds_campaign_add(campaign)
                log.info('New campaign "%s" added to RDS.',
                         campaign['CampaignShortName']['S'])
                # Finally, still add the ContactsCampaigns-record
                cursor.execute(concam_sql, concam_tuple)
            else:
                raise e
                # TODO: send out warning (SNS?)
                # Will now trigger a DLQ
    db_conn.commit()
    
def get_s3_location(bucket, key):
    """Provide absolute S3-location based on bucket and key."""
    return 's3://%s/%s' % (bucket, key)

def campaign_get(campaign_token):
    response = ddb_client.get_item(
        TableName='EntryCampaigns',
        Key={'CampaignToken': {'S': campaign_token}}
    )
    return response['Item']

def lambda_handler(event, context):
    log.debug(json.dumps(event))
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))
    log.debug("Bucket is: %s", bucket)
    log.debug("Key is: %s", key)
    location = get_s3_location(bucket, key)
    # Get object from S3
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
    except Exception as e:
        log.error(e)
        log.error('Error getting object {} from bucket {}. \
                   Make sure they exist and your bucket is in the same \
                   region as this function.'.format(key, bucket))
        raise e
    else:
        event_body = json.loads(response['Body'].read())
        log.info(event_body)
        stage    = event_body['meta']['context']['stage']
        # Get campaign data and configuration via api-key ("CampaignToken")
        campaign = campaign_get(event_body['meta']['context']['api-key'])
        log.info('Contact came via "%s".', campaign['CampaignShortName']['S'])
        # Clean email and return tuple: (email_cleaned, email_local, email_domain, email_tld, err_msg)
        email_tuple = split_email(event_body['data']['email'])
        # if err_msg
        if email_tuple[4]:
            msg = 'Not a valid emailaddrss "%s": %s. Location is: %s.' % \
                (event_body['data']['email'], email_tuple[4], location)
            log.warn(msg)
            send_out_warning('Invalid email in bdm_event_lead_trigger', msg)
        else:
            uuid, seg_num = get_uuid_and_long(email_tuple[0])
            if not uuid:
                uuid, seg_num = gen_uuid4_and_long()
                log.info('Contact not found in RDS. Will add with uuid=%s and seg_num=%s', uuid, seg_num)
                rds_contact_add(uuid, email_tuple, event_body, campaign, location)
                rds_contactcampaign_add(uuid, email_tuple, event_body, campaign, location)
            log.debug('uuid=%s,  seg_num=%s', uuid, seg_num)
            rds_contactcampaign_add(uuid, email_tuple, event_body, campaign, location)
            # Get contact and some data
            mj_contact = mailjet_get(email_tuple[0])
            #~ ddb_contacts_add(uuid, event_body, campaign)
            if campaign.get('SubscribesTo'):
                # Add to Mailjet Main Account
                res = mailjet_main_add(email_tuple[0], uuid, seg_num,
                        event_body, campaign, mj_contact)
            # Send out welcome mail if new contact or contact without msg's
            if (not mj_contact or not mj_contact['MessageStatistics']['DeliveredCount']) \
                and campaign['WelcomeMail']['M']['SendWelcomeMail']['BOOL']:
                # Initiate Mailjet Transactional Account
                Mailjet_Trans = Client(auth=(MJ_TRANS_APIKEY_PUBLIC, MJ_TRANS_APIKEY_PRIVATE))
                res = send_welcome_mail(Mailjet_Trans, campaign, email_tuple[0])
                log.info('Welcome mail %s sent to %s.',
                         campaign['WelcomeMail']['M']['TemplateID']['N'],
                         email_tuple[0])
            else:
                log.info('NO Welcome mail sent to %s.', email_tuple[0])

def main():
    from mock_event import event
    lambda_handler(event, None)
    return 0

if __name__ == '__main__':
    main()


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
