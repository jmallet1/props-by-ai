from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import udf
import boto3
from pyspark.sql.types import StringType
from training_ml_model.nba_model.utils.etl import extract


# Initialize Spark session
spark = SparkSession.builder \
    .appName("Load game log data to PostgreSQL") \
    .getOrCreate()

input_table = "predicting.team_defense_raw"
output_table = "nba_matchup_difficulty"

# Function to write rows in batches
def batch_write_to_dynamodb(df, table):
    with table.batch_writer() as batch:
        for row in df.collect():  # Collect DataFrame to iterate through rows
            item = row.asDict()  # Convert each row to a dictionary
            batch.put_item(Item=item)

# Need to delete all data in DDB table
# Quickest way is to drop and recreate table
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
            {'AttributeName': 'team', 'KeyType': 'HASH'}  # Partition key
        ],
        AttributeDefinitions=[
            {'AttributeName': 'team', 'AttributeType': 'S'}  # String type for player_id
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
    df = df.withColumn("team", df["team"].cast(StringType()))

    # Register the UDF
    add_suffix_udf = udf(add_suffix, StringType())

    # Apply the UDF to add the suffix
    df = df.withColumn("pts_rank", add_suffix_udf(df["pts_rank"])) \
           .withColumn("reb_rank", add_suffix_udf(df["reb_rank"])) \
           .withColumn("ast_rank", add_suffix_udf(df["ast_rank"])) \
           .withColumn("blk_rank", add_suffix_udf(df["blk_rank"])) \
           .withColumn("stl_rank", add_suffix_udf(df["stl_rank"])) \
           .withColumn("tov_rank", add_suffix_udf(df["tov_rank"]))

    return df

def add_suffix(num):
    # Handle special cases for 11, 12, 13
    if 10 <= num % 100 <= 13:
        suffix = "th"
    else:
        # Determine suffix based on last digit
        last_digit = num % 10
        if last_digit == 1:
            suffix = "st"
        elif last_digit == 2:
            suffix = "nd"
        elif last_digit == 3:
            suffix = "rd"
        else:
            suffix = "th"
    
    return f"{num}{suffix}"


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
        abbreviation as team,
        pts_rnk as pts_rank,
        reb_rnk as reb_rank,
        ast_rnk as ast_rank,
        blk_rnk as blk_rank,
        stl_rnk as stl_rank,
        tov_rnk as tov_rank
    FROM {input_table}
    """

    recreate_ddb_table()
    data = extract(query=query, spark=spark)
    transformed_data = transform(data)
    load(transformed_data)

    # Stop Spark session
    spark.stop()