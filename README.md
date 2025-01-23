# props-by-ai
React web app publishing AI predictions for daily player props

## Folders
#### app/* - 
# AWS ReactJS web application front end hosted on static S3 bucket, using API gateway and Lambda functions to pull from DynamoDB\n
#### backend/* - 
# Airflow scheduler running PySpark ETL pipelines from APIs to/within PostgreSQL DB (running locally)\n
#### training_ml_model/* - 
# PySpark code base to load data from nba_api, train ML models, and predict NBA player stats on a daily cadence\n

## TODO:
1. Configure frontend to handle players with no/partial prop lines for the day
2. Moto testing with mock AWS resources for automatic testing in CI/CD pipeline
3. Deploy AWS services using CloudFormation or Terraform