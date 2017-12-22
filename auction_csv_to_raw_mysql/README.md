### auction_csv_to_raw_mysql

This AWS λ-function:

  - downloads /clean_csv/diff.csv from s3://bdm-auction-export
  - adds (via REPLACE) these records to `AuctionsRaw`-table on
    MySQL RDS
  - is triggered by the arrival of the diff.csv file on S3
    (via SNS fan-out)

 That's all...
