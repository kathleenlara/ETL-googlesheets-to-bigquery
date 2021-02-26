# ETL data from Google Sheets to BigQuery Cloud Datawarehouse

## Business Problem
The reporting team would like to have the goals and benchmarks data they have on google sheet connected to the Big Query Cloud Data Warehouse
where the performance data is located. This will allow them to produce a dashboard with both goals  & benchmarks | actual performance data.

## Data Set
The data from Google Sheets are managed by the reporting team with over 100+ goals and benchmarks per campaign. The performance data in 
Big Query Data Warehouse contains 20MIL+ rows of app event data.

## Solving the Problem
- Created data tables with similar schema from Google Sheets in BQ
- Extracted the Data from Google Sheets via the Cloud API tool
- Transformed the data from Google Sheets to make sure we're collecting clean data
- Scheduled the query to run every hour (so the collection is continuous)

## Author
https://www.kathleenlara.com/






