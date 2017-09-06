# This AWS λ-function:

- is triggered by the arrival of a JSON lead on S3
    (s3://bdm-events/leads/)
    (which in itself was triggered by the campaign_entries λ-function)
- reads in information from the JSON file and (based on config from
    DDB):
    - adds to RDS (MySQL: Contacts, ContactsCampaigns)
    - adds to Mailjet, if needed
    - can add campaign to Campaigns (MySQL) if it doesn't exist
