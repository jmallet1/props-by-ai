from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
import boto3
from pyspark.sql.types import IntegerType, StringType
from training_ml_model.nba_model.utils.etl import extract


# Initialize Spark session
spark = SparkSession.builder \
    .appName("Load game log data to PostgreSQL") \
    .getOrCreate()

input_table = "predicting.predictions_raw"
output_table = "nba_lines"

# Function to write rows in batches
def batch_write_to_dynamodb(df, table):
    with table.batch_writer() as batch:
        for row in df.collect():  # Collect DataFrame to iterate through rows
            item = row.asDict()  # Convert each row to a dictionary
            batch.put_item(Item=item)

# Need to delete all data in DDB table
# Quickest way is to drop and recreate table, also supports schema evolution
def recreate_ddb_table():

    dynamodb = boto3.client('dynamodb')

    # Delete the table
    dynamodb.delete_table(TableName=output_table)
    print(f"Deleting table {output_table}...")

    # Wait for the table to be deleted
    waiter = dynamodb.get_waiter('table_not_exists')
    waiter.wait(TableName=output_table)
    print(f"Table {output_table} deleted.")

    # Recreate the table (replace with your table schema)
    dynamodb.create_table(
        TableName=output_table,
        KeySchema=[
            {'AttributeName': 'player_id', 'KeyType': 'HASH'},  # Partition key
            {'AttributeName': 'prop_type', 'KeyType': 'RANGE'}       # Sort key
        ],
        AttributeDefinitions=[
            {'AttributeName': 'player_id', 'AttributeType': 'S'},  # String type for player_id
            {'AttributeName': 'prop_type', 'AttributeType': 'S'}        # Number type for date
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
    print(f"Table {output_table} recreated.")

    # Wait for the table to be active
    waiter = dynamodb.get_waiter('table_exists')
    waiter.wait(TableName=output_table)
    print(f"Table {output_table} is now active.")  

def transform(df: DataFrame):

    # Change data types of 2 columns
    df = df.withColumn("player_id", df["player_id"].cast(StringType()))
    df = df.withColumn("date", df["date"].cast(IntegerType()))

    return df

# Load to DDB
def load(df: DataFrame):

    # Write data to DynamoDB in batches
    dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
    table = dynamodb.Table(output_table)

    # Apply the batch write function
    batch_write_to_dynamodb(df, table)

def handler():
    
    query = f"""
    SELECT
        player_id,
        player_name,
        team,
        prop_type,
        TO_CHAR(date, 'YYYYMMDD') as date,
        matchup,
        ROUND(prediction::NUMERIC, 1)::DECIMAL as prediction,
        high_line::DECIMAL,
        high_odds::DECIMAL,
        high_sportsbook,
        low_line::DECIMAL,
        low_odds::DECIMAL,
        low_sportsbook
    FROM {input_table}
    """

    recreate_ddb_table()
    data = extract(query=query, spark=spark)
    transformed_data = transform(data)
    load(transformed_data)

    # Stop Spark session
    spark.stop()