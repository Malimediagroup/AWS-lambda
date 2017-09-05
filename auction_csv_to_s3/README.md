# auction_csv_to_s3

AWS λ-function to:

 - download the csv-auction export from:
     <auction_export_url> (environment variable)
 - perform some basic checks: catch potential errors early...
 - ship it to s3://bdm-auction-exports/raw_csv/yyyy/mm/...

This function is cron-triggered via CloudWatch rules

$ source env.secrets

before starting.

That's all...
