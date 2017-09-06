# AWS λ-functions

Collection of AWS λ-functions

## Functions

### auction_csv_to_s3

AWS λ-function to:

 - download the csv-auction export from:
     <auction_export_url> (environment variable)
 - perform some basic checks: catch potential errors early...
 - ship it to s3://bdm-auction-exports/raw_csv/yyyy/mm/...

This function is cron-triggered via CloudWatch rules

$ source env.secrets

before starting.

That's all...

### bdm_event_lead_trigger

- is triggered by the arrival of a JSON lead on S3
    (s3://bdm-events/leads/)
    (which in itself was triggered by the campaign_entries λ-function)
- reads in information from the JSON file and (based on config from
    DDB):
    - adds to RDS (MySQL: Contacts, ContactsCampaigns)
    - adds to Mailjet, if needed
    - can add campaign to Campaigns (MySQL) if it doesn't exist

That's all...

## Flowcharts

TODO
