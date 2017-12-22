### auction_csv_to_google

AWS λ-function to:

 - get's two files from S3 (triggered by SNS-topic):
     - s3://bdm-auction-exports/clean_csv/latest.csv
     - s3://bdm-auction-exports/clean_csv/diff.csv
 - uploads them to Google Drive as spreadsheets

That's all
